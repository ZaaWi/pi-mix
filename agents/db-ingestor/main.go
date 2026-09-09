package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/lib/pq"
)

// Bucket width for the persisted fine granularity.
const bucketSecs = int64(15 * 60)

// Env-driven config.
var (
	mqttBroker    = env("MQTT_BROKER", "tcp://mosquitto.iot.svc.cluster.local:1883")
	mqttTopics    = splitEnv("MQTT_TOPIC", "pi/dht11,pi/ldr")
	pgHost        = env("PG_HOST", "postgres.database.svc.cluster.local")
	pgPort        = env("PG_PORT", "5432")
	pgUser        = env("PG_USER", "appuser")
	pgPass        = env("PG_PASSWORD", "appdb")
	pgDB          = env("PG_DB", "appdb")
	spillDir      = env("SPILL_DIR", "/var/lib/db-ingestor/spill")
	drainInterval = envDuration("DRAIN_INTERVAL", "60s")
	rollEvery     = envDuration("ROLL_INTERVAL", "30s")
	gracePeriod   = envDuration("CLOSE_GRACE", "5s")
	httpAddr      = env("HTTP_ADDR", ":8090")
)

// acc is a running aggregate for one sensor+metric within one bucket.
type acc struct {
	count  int
	sum    float64
	min    float64
	max    float64
	last   float64
	lastTS int64
}

func (a *acc) add(v float64, ts int64) {
	if a.count == 0 {
		a.min, a.max = v, v
	} else {
		if v < a.min {
			a.min = v
		}
		if v > a.max {
			a.max = v
		}
	}
	a.count++
	a.sum += v
	a.last = v
	a.lastTS = ts
}

func (a *acc) addFrom(b acc) {
	if b.count == 0 {
		return
	}
	if a.count == 0 {
		*a = b
		return
	}
	if b.min < a.min {
		a.min = b.min
	}
	if b.max > a.max {
		a.max = b.max
	}
	a.count += b.count
	a.sum += b.sum
	if b.lastTS > a.lastTS {
		a.last = b.last
		a.lastTS = b.lastTS
	}
}

func (a acc) avg() float64 {
	if a.count == 0 {
		return 0
	}
	return a.sum / float64(a.count)
}

// bucket is the serializable unit persisted to spill files and PG.
type bucket struct {
	Sensor   string  `json:"sensor"`
	Metric   string  `json:"metric"`
	Interval string  `json:"interval"`
	TS       int64   `json:"ts"` // bucket start, unix seconds
	Count    int     `json:"count"`
	Avg      float64 `json:"avg"`
	Min      float64 `json:"min"`
	Max      float64 `json:"max"`
	Last     float64 `json:"last"`
}

type bucketKey struct {
	sensor string
	metric string
	start  int64
}

func (k bucketKey) String() string {
	return fmt.Sprintf("%s|%s|%d", k.sensor, k.metric, k.start)
}

func (a acc) toBucket(sensor, metric string) bucket {
	return bucket{
		Sensor:   sensor,
		Metric:   metric,
		Interval: "15m",
		TS:       0, // filled by caller
		Count:    a.count,
		Avg:      a.avg(),
		Min:      a.min,
		Max:      a.max,
		Last:     a.last,
	}
}

// spill manages one-file-per-bucket storage with atomic write+rename.
// A file existing means "this bucket is pending flush to PG".
type spill struct {
	dir string
	mu  sync.Mutex
}

func (s *spill) path(k bucketKey) string {
	return filepath.Join(s.dir, fmt.Sprintf("15m-%s-%s-%d.jsonl", k.sensor, k.metric, k.start))
}

// upsert merges a into the bucket's file (creating or updating it atomically).
func (s *spill) upsert(k bucketKey, a acc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, err := s.read(k)
	if err != nil {
		return err
	}
	merged := existing
	merged.addFrom(a)
	mergedParts := map[string]interface{}{
		"sensor":   k.sensor,
		"metric":   k.metric,
		"interval": "15m",
		"ts":       k.start,
		"count":    merged.count,
		"avg":      math.Round(merged.avg()*1000) / 1000,
		"min":      math.Round(merged.min*1000) / 1000,
		"max":      math.Round(merged.max*1000) / 1000,
		"last":     math.Round(merged.last*1000) / 1000,
	}
	data, err := json.Marshal(mergedParts)
	if err != nil {
		return err
	}

	tmp := filepath.Join(s.dir, fmt.Sprintf(".tmp-%s", k.String()))
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path(k)); err != nil {
		return err
	}
	return nil
}

func (s *spill) read(k bucketKey) (acc, error) {
	raw, err := os.ReadFile(s.path(k))
	if err != nil {
		if os.IsNotExist(err) {
			return acc{min: math.Inf(1), max: math.Inf(-1)}, nil
		}
		return acc{}, err
	}
	var b struct {
		Count int     `json:"count"`
		Avg   float64 `json:"avg"`
		Min   float64 `json:"min"`
		Max   float64 `json:"max"`
		Last  float64 `json:"last"`
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		return acc{}, fmt.Errorf("corrupt spill file %s: %w", s.path(k), err)
	}
	return acc{count: b.Count, sum: b.Avg * float64(b.Count), min: b.Min, max: b.Max, last: b.Last}, nil
}

// list returns all pending bucket keys, sorted by timestamp.
func (s *spill) list() []bucketKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		log.Printf("spill list: %v", err)
		return nil
	}
	var out []bucketKey
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "15m-") {
			continue
		}
		parts := strings.Split(strings.TrimSuffix(name, ".jsonl"), "-")
		if len(parts) < 4 {
			continue
		}
		var start int64
		if _, err := fmt.Sscanf(parts[len(parts)-1], "%d", &start); err != nil {
			continue
		}
		metric := strings.Join(parts[2:len(parts)-1], "-")
		out = append(out, bucketKey{sensor: parts[1], metric: metric, start: start})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].start < out[j].start })
	return out
}

func (s *spill) remove(k bucketKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return os.Remove(s.path(k))
}

// ingestorState holds the RAM bucket map and spill writer.
type ingestorState struct {
	mu    sync.Mutex
	buck  map[bucketKey]acc
	spill *spill
}

func (s *ingestorState) addReading(sensor, metric string, value float64, ts int64) {
	start := (ts / bucketSecs) * bucketSecs
	k := bucketKey{sensor: sensor, metric: metric, start: start}
	now := time.Now().UTC().Unix()

	// Late arrival for an already-closed window → straight to spill (merged).
	if start+bucketSecs <= now-int64(gracePeriod.Seconds()) {
		if err := s.spill.upsert(k, accWithValue(value, ts)); err != nil {
			log.Printf("spill upsert %s: %v", k, err)
		}
		return
	}

	s.mu.Lock()
	cur, ok := s.buck[k]
	if !ok {
		cur = acc{min: math.Inf(1), max: math.Inf(-1)}
	}
	cur.add(value, ts)
	s.buck[k] = cur
	s.mu.Unlock()
}

func accWithValue(v float64, ts int64) acc {
	a := acc{min: math.Inf(1), max: math.Inf(-1)}
	a.add(v, ts)
	return a
}

// roll closes finished buckets: captures their acc, evicts from RAM,
// then writes them to spill (which merges with any existing file).
func (s *ingestorState) roll(now time.Time) {
	cutoff := now.Unix() - int64(gracePeriod.Seconds())
	closed := map[bucketKey]acc{}

	s.mu.Lock()
	for k, a := range s.buck {
		if k.start+bucketSecs <= cutoff {
			closed[k] = a
		}
	}
	for k := range closed {
		delete(s.buck, k)
	}
	s.mu.Unlock()

	for k, a := range closed {
		if err := s.spill.upsert(k, a); err != nil {
			log.Printf("close bucket %s: %v", k, err)
		}
	}
}

func flushAll(s *ingestorState) {
	s.mu.Lock()
	keys := make([]bucketKey, 0, len(s.buck))
	for k := range s.buck {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	for _, k := range keys {
		s.mu.Lock()
		a, ok := s.buck[k]
		s.mu.Unlock()
		if ok {
			if err := s.spill.upsert(k, a); err != nil {
				log.Printf("flush bucket %s: %v", k, err)
			}
		}
	}
}

// drain flushes pending spill files to PG and derives 1h rows.
func drain(db *sql.DB, st *ingestorState) {
	pending := st.spill.list()
	if len(pending) == 0 {
		return
	}
	if err := db.Ping(); err != nil {
		log.Printf("PG unreachable (%v), %d spill files pending", err, len(pending))
		return
	}

	var flushErr error
	tx, err := db.Begin()
	if err != nil {
		log.Printf("drain begin: %v", err)
		return
	}
	defer tx.Rollback()

	done := map[bucketKey]bool{}
	affectedHours := map[int64]bool{}
	for _, k := range pending {
		a, err := st.spill.read(k)
		if err != nil {
			log.Printf("drain read %s: %v", k, err)
			continue
		}
		b := a.toBucket(k.sensor, k.metric)
		b.TS = k.start
		if err := upsert15m(tx, b); err != nil {
			log.Printf("upsert %s: %v", k, err)
			flushErr = err
			break
		}
		done[k] = true
		affectedHours[(k.start/3600)*3600] = true
	}

	if flushErr == nil && len(done) > 0 {
		now := time.Now().UTC()
		for hour := range affectedHours {
			if hour+3600 <= now.Unix() {
				if err := derive1h(tx, hour); err != nil {
					log.Printf("derive1h %d: %v", hour, err)
					flushErr = err
					break
				}
			}
		}
	}

	if flushErr == nil {
		if err := tx.Commit(); err != nil {
			log.Printf("drain commit: %v", err)
			return
		}
		for k := range done {
			if err := st.spill.remove(k); err != nil {
				log.Printf("spill remove %s: %v", k, err)
			}
		}
		if len(done) > 0 {
			log.Printf("flushed %d buckets, %d spill files pending", len(done), len(pending)-len(done))
		}
	}
}

const upsert15mSQL = `
INSERT INTO pi_sensor_data (sensor, metric, interval, ts, count, avg, min, max, last)
VALUES ($1, $2, '15m', to_timestamp($3), $4, $5, $6, $7, $8)
ON CONFLICT (sensor, metric, interval, ts) DO UPDATE SET
  count = EXCLUDED.count,
  avg   = EXCLUDED.avg,
  min   = EXCLUDED.min,
  max   = EXCLUDED.max,
  last  = EXCLUDED.last`

func upsert15m(tx *sql.Tx, b bucket) error {
	_, err := tx.Exec(upsert15mSQL, b.Sensor, b.Metric, b.TS, b.Count, b.Avg, b.Min, b.Max, b.Last)
	return err
}

const derive1hSQL = `
INSERT INTO pi_sensor_data (sensor, metric, interval, ts, count, avg, min, max, last)
SELECT sensor, metric, '1h', date_trunc('hour', ts),
       sum(count), avg(avg), min(min), max(max),
       (array_agg(last ORDER BY ts DESC))[1]
FROM pi_sensor_data
WHERE interval = '15m' AND ts >= $1 AND ts < $1 + interval '1 hour'
GROUP BY sensor, metric, date_trunc('hour', ts)
ON CONFLICT (sensor, metric, interval, ts) DO UPDATE SET
  count = EXCLUDED.count,
  avg   = EXCLUDED.avg,
  min   = EXCLUDED.min,
  max   = EXCLUDED.max,
  last  = EXCLUDED.last`

func derive1h(tx *sql.Tx, hourStart int64) error {
	_, err := tx.Exec(derive1hSQL, time.Unix(hourStart, 0).UTC())
	return err
}

func handleMessage(st *ingestorState) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		var payload map[string]interface{}
		if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
			log.Printf("bad payload on %s: %v", msg.Topic(), err)
			return
		}

		sensor := "unknown"
		if s, ok := payload["sensor"].(string); ok {
			sensor = s
		} else {
			parts := strings.Split(msg.Topic(), "/")
			if len(parts) > 1 {
				sensor = parts[1]
			}
		}

		// Payload ts wins; otherwise stamp receive time.
		var ts int64
		if f, ok := payload["ts"].(float64); ok {
			ts = int64(f)
		} else {
			ts = time.Now().UTC().Unix()
		}

		for key, val := range payload {
			if key == "event" || key == "sensor" || key == "ts" {
				continue
			}
			if num, ok := val.(float64); ok {
				st.addReading(sensor, key, num, ts)
			}
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if err := os.MkdirAll(spillDir, 0755); err != nil {
		log.Fatalf("spill dir: %v", err)
	}

	st := &ingestorState{buck: make(map[bucketKey]acc), spill: &spill{dir: spillDir}}
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		pgHost, pgPort, pgUser, pgPass, pgDB)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(3)
	db.SetConnMaxLifetime(10 * time.Minute)

	opts := mqtt.NewClientOptions().AddBroker(mqttBroker).SetClientID("db-ingestor")
	opts.OnConnect = func(c mqtt.Client) {
		for _, t := range mqttTopics {
			if tok := c.Subscribe(t, 0, handleMessage(st)); tok.Wait() && tok.Error() != nil {
				log.Printf("subscribe %s: %v", t, tok.Error())
			}
		}
		log.Printf("subscribed to %v", mqttTopics)
	}
	client := mqtt.NewClient(opts)
	if tok := client.Connect(); tok.Wait() && tok.Error() != nil {
		log.Fatalf("mqtt connect: %v", tok.Error())
	}

	// Roller: close finished 15-min buckets into spill files.
	go func() {
		t := time.NewTicker(rollEvery)
		defer t.Stop()
		for now := range t.C {
			st.roll(now.UTC())
		}
	}()

	// Drainer: flush pending spill files to PG, then derive 1h.
	go func() {
		t := time.NewTicker(drainInterval)
		defer t.Stop()
		for range t.C {
			drain(db, st)
		}
	}()

	// Health/status endpoint (500 when PG unreachable).
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "pg unreachable: %v\n", err)
			return
		}
		fmt.Fprintf(w, "pending spill files: %d\n", len(st.spill.list()))
	})
	go func() {
		log.Printf("http listening on %s", httpAddr)
		http.ListenAndServe(httpAddr, nil)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down, closing buckets...")
	closeNow := time.Now().UTC()
	st.roll(closeNow)
	flushAll(st)
	drain(db, st)
	client.Disconnect(250)
	log.Println("done.")
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func splitEnv(k, d string) []string {
	raw := env(k, d)
	var out []string
	for _, s := range strings.Split(raw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func envDuration(k, d string) time.Duration {
	if v := os.Getenv(k); v != "" {
		if dur, err := time.ParseDuration(v); err == nil {
			return dur
		}
	}
	dur, _ := time.ParseDuration(d)
	return dur
}