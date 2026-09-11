// Ingestor aggregates telemetry into Redis (the primary history store) and
// archives closed hours to PostgreSQL (durable, cold-boot backfill source).
//
// Key layout (per metric, sorted sets / hashes / sets / marker):
//
//	pi-mix:raw:sensor_<metric>   raw samples, member "<ts>:<value>", score=ts
//	pi-mix:15m:sensor_<metric>   agg JSON member per 15m bucket, score=bucket_ts
//	pi-mix:1h:sensor_<metric>    agg JSON member per hour, score=hour_ts
//	pi-mix:dirty:sensor_<metric> hash hour_ts -> sample count (roll-up needs)
//	pi-mix:seen:YYYYMMDD         set of ingested event ids (at-least-once dedup)
//	pi-mix:initialized           marker; absent => backfill Redis from PG
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// bucketSecs is the live aggregation resolution (15m). The raw tail keeps
// individual samples; every sample also merges into its 15m bucket.
const bucketSecs = 900

var (
	metrics = []string{"temp_c", "humidity_pct", "analog", "weight_kg", "bits"}

	metricSensor = map[string]string{
		"temp_c": "dht11", "humidity_pct": "dht11",
		"analog": "ldr", "weight_kg": "scale", "bits": "ir",
	}
	plainKeys = []string{"raw", "15m", "1h"}
)

// luaMerge dedups by event id, keeps the per-sample raw tail and merges the
// sample into its 15m Bucket and marks the owning hour dirty. Atomic so a
// redelivered QoS1 message can never be applied twice.
//
//	KEYS[1]  seen set (pi-mix:seen:YYYYMMDD)
//	KEYS[1+3i]   raw, KEYS[2+3i] b15, KEYS[3+3i] dirty  (i >= 1)
//	ARGV[1]  event id
//	ARGV[2]  sample ts (unix seconds)
//	ARGV[2+i] sample value (one per metric, aligned with KEYS)
var luaMerge = `
local used = redis.call('SADD', KEYS[1], ARGV[1])
if used == 0 then return 0 end
redis.call('EXPIRE', KEYS[1], 259200)
local n = (#KEYS - 1) / 3
local ts = tonumber(ARGV[2])
local bucket = math.floor(ts / ` + strconv.Itoa(bucketSecs) + `) * ` + strconv.Itoa(bucketSecs) + `
local hour = math.floor(bucket / 3600) * 3600
for i = 1, n do
  local val = tonumber(ARGV[2 + i])
  local raw = KEYS[1 + 3 * (i - 1) + 1]
  local b15 = KEYS[1 + 3 * (i - 1) + 2]
  local dirty = KEYS[1 + 3 * (i - 1) + 3]
  redis.call('ZADD', raw, ts, tostring(ts) .. ':' .. tostring(val))
  local cur = redis.call('ZRANGEBYSCORE', b15, tostring(bucket), tostring(bucket))
  local agg
  if cur[1] then agg = cjson.decode(cur[1]) else agg = {c=0,s=0,mn=val,mx=val,l=val,lt=ts} end
  agg.c = agg.c + 1
  agg.s = agg.s + val
  if val < agg.mn then agg.mn = val end
  if val > agg.mx then agg.mx = val end
  if ts >= agg.lt then agg.l = val; agg.lt = ts end
  redis.call('ZADD', b15, bucket, cjson.encode(agg))
  redis.call('HINCRBY', dirty, tostring(hour), 1)
end
return 1
`

// agg is the member stored in the 15m/1h ZSETs; avg is derived as sum/count.
type agg struct {
	C  int64   `json:"c"`
	S  float64 `json:"s"`
	Mn float64 `json:"mn"`
	Mx float64 `json:"mx"`
	L  float64 `json:"l"`
	Lt int64   `json:"lt"`
}

// pgRow is one row of pi_sensor_data produced by a complete-hour roll-up.
type pgRow struct {
	sensor, metric, interval string
	ts, count                int64
	avg, min, max, last      float64
	sum                      float64
	lastTS                   int64
}

func (a agg) avg() float64 {
	if a.C == 0 {
		return 0
	}
	return a.S / float64(a.C)
}

// addAgg folds b into a. Intended for merging the four 15m buckets of a closed
// hour into its 1h aggregate.
func addAgg(a *agg, b agg) {
	a.C += b.C
	a.S += b.S
	if b.Mn < a.Mn {
		a.Mn = b.Mn
	}
	if b.Mx > a.Mx {
		a.Mx = b.Mx
	}
	if b.Lt >= a.Lt {
		a.L = b.L
		a.Lt = b.Lt
	}
}

func seriesKey(series, metric string) string {
	return fmt.Sprintf("pi-mix:%s:sensor_%s", series, metric)
}
func dirtyKey(metric string) string { return "pi-mix:dirty:sensor_" + metric }
func seenKey(now time.Time) string  { return "pi-mix:seen:" + now.Format("20060102") }

const markerKey = "pi-mix:initialized"

type config struct {
	rawHours float64
	d15mDays float64
	d1hDays  float64
}

func defaultConfig() config {
	return config{rawHours: envFloat("REDIS_RAW_HOURS", 6), d15mDays: envFloat("REDIS_15M_DAYS", 30), d1hDays: envFloat("REDIS_1H_DAYS", 90)}
}

type event struct {
	ID     string             `json:"event_id"`
	Sensor string             `json:"sensor"`
	TS     int64              `json:"ts"`
	Values map[string]float64 `json:"values"`
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envFloat(k string, d float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			return f
		}
	}
	return d
}

func newID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func parseEvent(raw []byte, now time.Time) (event, error) {
	if len(raw) > 4096 {
		return event{}, fmt.Errorf("payload too large")
	}
	var p map[string]any
	if err := json.Unmarshal(raw, &p); err != nil {
		return event{}, err
	}
	sensor, _ := p["sensor"].(string)
	allowed := map[string][]string{"dht11": {"temp_c", "humidity_pct"}, "ldr": {"analog"}, "scale": {"weight_kg"}, "ir": {"bits"}}
	keys, ok := allowed[sensor]
	if !ok {
		return event{}, fmt.Errorf("unknown sensor %q", sensor)
	}
	ts, ok := p["ts"].(float64)
	if !ok {
		ts = float64(now.Unix())
	}
	if ts < float64(now.Add(-7*24*time.Hour).Unix()) || ts > float64(now.Add(5*time.Minute).Unix()) {
		return event{}, fmt.Errorf("timestamp out of range")
	}
	id, _ := p["event_id"].(string)
	if id == "" {
		id = newID()
	}
	e := event{ID: id, Sensor: sensor, TS: int64(ts), Values: map[string]float64{}}
	for _, key := range keys {
		if v, ok := p[key].(float64); ok && !math.IsInf(v, 0) && !math.IsNaN(v) {
			e.Values[key] = v
		}
	}
	if len(e.Values) == 0 {
		return event{}, fmt.Errorf("no sensor values")
	}
	return e, nil
}

var redisErrMu sync.Mutex
var redisErrTimes = map[string]time.Time{}

// logRedisError rate-limits Redis failure logs. Redis is the primary history
// store: losing it must be visible (AUD-07) without spamming rotated logs.
func logRedisError(msg string, err error) {
	if err == nil {
		return
	}
	now := time.Now()
	redisErrMu.Lock()
	defer redisErrMu.Unlock()
	if last, ok := redisErrTimes[msg]; ok && now.Sub(last) < 30*time.Second {
		return
	}
	redisErrTimes[msg] = now
	if len(redisErrTimes) > 64 {
		for k := range redisErrTimes {
			if len(redisErrTimes) <= 64 {
				break
			}
			delete(redisErrTimes, k)
		}
	}
	log.Printf("redis %s: %v", msg, err)
}

// ingestSample merges one event into Redis: dedup, raw tail, 15m bucket, dirty
// hour marker, all in a single atomic script call. On error the message is NOT
// acked so the QoS1 broker redelivers it once Redis recovers.
func ingestSample(ctx context.Context, rdb *redis.Client, e event, now time.Time) error {
	keys := []string{seenKey(now)}
	args := []any{e.ID, e.TS}
	for metric, value := range e.Values {
		keys = append(keys, seriesKey("raw", metric), seriesKey("15m", metric), dirtyKey(metric))
		args = append(args, value)
	}
	return rdb.Eval(ctx, luaMerge, keys, args...).Err()
}

// trimAll drops samples older than the retention windows for every metric.
func trimAll(ctx context.Context, rdb *redis.Client, now time.Time, cfg config) {
	seconds := func(f float64) float64 { return f * float64(time.Hour) / float64(time.Second) }
	cutoffs := map[string]int64{
		"raw": int64(seconds(cfg.rawHours)),
		"15m": int64(seconds(cfg.d15mDays * 24)),
		"1h":  int64(seconds(cfg.d1hDays * 24)),
	}
	pipe := rdb.Pipeline()
	base := now.Unix()
	for _, series := range plainKeys {
		cutoff := strconv.FormatInt(base-cutoffs[series], 10)
		for _, m := range metrics {
			pipe.ZRemRangeByScore(ctx, seriesKey(series, m), "-inf", cutoff)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		logRedisError("trim", err)
	}
}

// ensureBackfill rebuilds Redis aggregates from PostgreSQL whenever Redis is
// fresh (pod restart or full flush); `pi-mix:initialized` marks the rebuild.
func ensureBackfill(ctx context.Context, rdb *redis.Client, db *sql.DB, cfg config) error {
	ok, err := rdb.Exists(ctx, markerKey).Result()
	if err != nil {
		return fmt.Errorf("redis exists: %w", err)
	}
	if ok == 1 {
		return nil
	}
	log.Print("redis empty; backfilling aggregates from postgres")
	for interval, days := range map[string]float64{"15m": cfg.d15mDays, "1h": cfg.d1hDays} {
		if err := backfillInterval(ctx, db, rdb, interval, days); err != nil {
			return fmt.Errorf("backfill %s: %w", interval, err)
		}
	}
	if err := rdb.Set(ctx, markerKey, "1", 0).Err(); err != nil {
		return fmt.Errorf("redis marker: %w", err)
	}
	log.Print("redis backfill complete")
	return nil
}

func backfillInterval(ctx context.Context, db *sql.DB, rdb *redis.Client, interval string, days float64) error {
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, `
SELECT metric, extract(epoch from ts)::bigint, count, sum, min, max, last, extract(epoch from last_ts)::bigint
FROM pi_sensor_data
WHERE interval=$1 AND ts >= now() - make_interval(days => $2)
ORDER BY metric, ts ASC`, interval, int(days))
	if err != nil {
		return err
	}
	defer rows.Close()

	type zsetEntry struct {
		key    string
		score  int64
		member string
	}
	bulk := make([]zsetEntry, 0, 4096)
	for rows.Next() {
		var metric string
		var ts, count, lastTS int64
		var sum, min, max, last float64
		if err := rows.Scan(&metric, &ts, &count, &sum, &min, &max, &last, &lastTS); err != nil {
			return err
		}
		if _, known := metricSensor[metric]; !known {
			continue
		}
		mb, _ := json.Marshal(agg{C: count, S: sum, Mn: min, Mx: max, L: last, Lt: lastTS})
		bulk = append(bulk, zsetEntry{seriesKey(interval, metric), ts, string(mb)})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for i := 0; i < len(bulk); i += 2000 {
		chunk := bulk[i:]
		if len(chunk) > 2000 {
			chunk = chunk[:2000]
		}
		_, err := rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
			for _, e := range chunk {
				p.ZAdd(ctx, e.key, redis.Z{Score: float64(e.score), Member: e.member})
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// rollupClosedHours aggregates every dirty hour whose buckets are complete
// (ended at least fifteen minutes ago) into the 1h series and appends the
// closed hour (4x15m + 1h rows) to PostgreSQL. Failures keep the dirty mark so
// the next maintenance pass retries.
func rollupClosedHours(ctx context.Context, rdb *redis.Client, db *sql.DB, now time.Time) {
	cutoff := now.Add(-15 * time.Minute).Unix()
	for _, metric := range metrics {
		fields, err := rdb.HGetAll(ctx, dirtyKey(metric)).Result()
		if err != nil {
			logRedisError("hgetall "+dirtyKey(metric), err)
			continue
		}
		for hourStr := range fields {
			hour, err := strconv.ParseInt(hourStr, 10, 64)
			if err != nil || hour <= 0 || hour+3600 > cutoff {
				continue
			}
			if err := rollupHour(ctx, rdb, db, metric, hour); err != nil {
				log.Printf("rollup %s hour=%d: %v (retrying next pass)", metric, hour, err)
				continue
			}
			if err := rdb.HDel(ctx, dirtyKey(metric), hourStr).Err(); err != nil {
				logRedisError("hdel "+dirtyKey(metric), err)
			}
		}
	}
}

// rollupHour reads the four 15m buckets of a closed hour from Redis, computes
// the 1h aggregate, and writes the authoritative rows to PostgreSQL.
func rollupHour(ctx context.Context, rdb *redis.Client, db *sql.DB, metric string, hour int64) error {
	items, err := rdb.ZRangeByScoreWithScores(ctx, seriesKey("15m", metric), &redis.ZRangeBy{
		Min: strconv.FormatInt(hour, 10),
		Max: strconv.FormatInt(hour+3599, 10),
	}).Result()
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}

	sensor := metricSensor[metric]
	total := agg{Mn: math.Inf(1), Mx: math.Inf(-1)}
	rows := make([]pgRow, 0, len(items)+1)
	for _, z := range items {
		member, _ := z.Member.(string)
		var a agg
		if err := json.Unmarshal([]byte(member), &a); err != nil || a.C == 0 {
			continue
		}
		rows = append(rows, pgRow{sensor: sensor, metric: metric, interval: "15m",
			ts: int64(z.Score), count: a.C, avg: a.avg(), min: a.Mn, max: a.Mx, last: a.L, sum: a.S, lastTS: a.Lt})
		addAgg(&total, a)
	}
	if total.C == 0 {
		return nil
	}
	rows = append(rows, pgRow{sensor: sensor, metric: metric, interval: "1h",
		ts: hour, count: total.C, avg: total.avg(), min: total.Mn, max: total.Mx, last: total.L, sum: total.S, lastTS: total.Lt})
	return flushRows(ctx, db, rows)
}

// flushRows is a single multi-row upsert. Rows are authoritative snapshots
// (overwrite), not additive, because Redis is the source of truth.
func flushRows(ctx context.Context, db *sql.DB, rows []pgRow) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO pi_sensor_data(sensor,metric,interval,ts,count,avg,min,max,last,sum,last_ts) VALUES ")
	args := make([]any, 0, len(rows)*10)
	for i, r := range rows {
		if i > 0 {
			sb.WriteString(",")
		}
		b := i*10 + 1
		fmt.Fprintf(&sb, "($%d,$%d,'%s',to_timestamp($%d),$%d,$%d,$%d,$%d,$%d,$%d,to_timestamp($%d))",
			b, b+1, r.interval, b+2, b+3, b+4, b+5, b+6, b+7, b+8, b+9)
		args = append(args, r.sensor, r.metric, r.ts, r.count, r.avg, r.min, r.max, r.last, r.sum, r.lastTS)
	}
	sb.WriteString(" ON CONFLICT(sensor,metric,interval,ts) DO UPDATE SET count=EXCLUDED.count,avg=EXCLUDED.avg,min=EXCLUDED.min,max=EXCLUDED.max,last=EXCLUDED.last,sum=EXCLUDED.sum,last_ts=EXCLUDED.last_ts")
	_, err := db.ExecContext(ctx, sb.String(), args...)
	return err
}

var lastPruneUnix atomic.Int64

// prunePGAtMostDaily removes PostgreSQL rows that can no longer be backfilled
// into Redis, keeping durable storage aligned with the retention windows. The
// ten-minute maintenance cadence is throttled to once per day to protect the
// spinner HDD.
func prunePGAtMostDaily(ctx context.Context, db *sql.DB, cfg config) {
	if time.Since(time.Unix(lastPruneUnix.Load(), 0)) < 24*time.Hour {
		return
	}
	queries := []string{
		fmt.Sprintf("DELETE FROM pi_sensor_data WHERE interval='15m' AND ts < now() - make_interval(days => %d)", int(cfg.d15mDays)),
		fmt.Sprintf("DELETE FROM pi_sensor_data WHERE interval='1h' AND ts < now() - make_interval(days => %d)", int(cfg.d1hDays)),
	}
	for _, q := range queries {
		if _, err := db.ExecContext(ctx, q); err != nil {
			log.Printf("postgres prune: %v", err)
			return
		}
	}
	lastPruneUnix.Store(time.Now().Unix())
	log.Printf("postgres history pruned to retention windows")
}

func runMaintenance(ctx context.Context, rdb *redis.Client, db *sql.DB, cfg config, now time.Time) {
	trimAll(ctx, rdb, now, cfg)
	if err := ensureBackfill(ctx, rdb, db, cfg); err != nil {
		log.Printf("backfill: %v", err)
	}
	rollupClosedHours(ctx, rdb, db, now)
	prunePGAtMostDaily(ctx, db, cfg)
}

func main() {
	cfg := defaultConfig()
	rdb := redis.NewClient(&redis.Options{
		Addr:     env("REDIS_ADDR", "redis.database.svc.cluster.local:6379"),
		Username: os.Getenv("REDIS_USERNAME"),
		Password: os.Getenv("REDIS_PASSWORD"),
		Protocol: 2,
	})
	defer rdb.Close()

	u := url.URL{Scheme: "postgres", Host: env("PG_HOST", "postgres.database.svc.cluster.local") + ":" + env("PG_PORT", "5432"), Path: env("PG_DB", "pi_mix"), User: url.UserPassword(env("PG_USER", "pi_mix_writer"), os.Getenv("PG_PASSWORD"))}
	q := u.Query()
	q.Set("sslmode", env("PG_SSLMODE", "disable"))
	q.Set("connect_timeout", "5")
	u.RawQuery = q.Encode()
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	db.SetConnMaxLifetime(10 * time.Minute)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	opts := mqtt.NewClientOptions().AddBroker(env("MQTT_BROKER", "tcp://mosquitto.iot.svc.cluster.local:1883")).SetClientID(env("MQTT_CLIENT_ID", "pi-mix-ingestor"))
	opts.SetCleanSession(false).SetAutoAckDisabled(true).SetConnectRetry(true).SetConnectTimeout(5 * time.Second).SetConnectRetryInterval(3 * time.Second)
	opts.SetUsername(os.Getenv("MQTT_USERNAME")).SetPassword(os.Getenv("MQTT_PASSWORD"))
	opts.OnConnect = func(c mqtt.Client) {
		for _, topic := range strings.Split(env("MQTT_TOPIC", "pi/dht11,pi/ldr,pi/scale"), ",") {
			t := c.Subscribe(strings.TrimSpace(topic), 1, func(_ mqtt.Client, m mqtt.Message) {
				e, err := parseEvent(m.Payload(), time.Now())
				if err != nil {
					m.Ack()
					return
				}
				mctx, mcancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer mcancel()
				if err := ingestSample(mctx, rdb, e, time.Now()); err != nil {
					// Do not ack: QoS1 redelivery will re-apply once Redis recovers.
					logRedisError("merge", err)
					return
				}
				m.Ack()
			})
			if !t.WaitTimeout(5*time.Second) || t.Error() != nil {
				log.Print("MQTT subscription failed")
			}
		}
	}
	client := mqtt.NewClient(opts)
	client.Connect()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if !client.IsConnected() {
			http.Error(w, "mqtt unavailable", 503)
			return
		}
		if err := rdb.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis unavailable", 503)
			return
		}
		if err := db.PingContext(ctx); err != nil {
			http.Error(w, "postgres unavailable", 503)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	server := &http.Server{Addr: env("HTTP_ADDR", ":8090"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Print(err)
			cancel()
		}
	}()

	maintCtx, stopMaint := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var maintBusy atomic.Bool
	run := func() {
		if !maintBusy.CompareAndSwap(false, true) {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer maintBusy.Store(false)
			mctx, mcancel := context.WithTimeout(maintCtx, 4*time.Minute)
			defer mcancel()
			runMaintenance(mctx, rdb, db, cfg, time.Now())
		}()
	}
	go func() {
		time.Sleep(30 * time.Second)
		run()
	}()
	maintenance := time.NewTicker(10 * time.Minute)
	defer maintenance.Stop()

run:
	for {
		select {
		case <-maintenance.C:
			run()
		case <-ctx.Done():
			break run
		}
	}

	stopMaint()
	client.Disconnect(250)
	wg.Wait()
	shutdown, done := context.WithTimeout(context.Background(), 3*time.Second)
	defer done()
	server.Shutdown(shutdown)
}
