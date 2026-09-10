// Ingestor buffers telemetry into Redis for live querying and archives to PostgreSQL in bulk.
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
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type event struct {
	ID     string             `json:"event_id"`
	Sensor string             `json:"sensor"`
	TS     int64              `json:"ts"`
	Values map[string]float64 `json:"values"`
}

type pending struct {
	event event
	ack   func()
}

type batch struct {
	ID     string
	Events []event
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
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

const addSQL = `INSERT INTO pi_sensor_data(sensor,metric,interval,ts,count,avg,min,max,last,sum,last_ts)
VALUES($1,$2,'15m',to_timestamp($3),1,$4,$4,$4,$4,$4,to_timestamp($5))
ON CONFLICT(sensor,metric,interval,ts) DO UPDATE SET
 count=pi_sensor_data.count+1,
 avg=(pi_sensor_data.sum+EXCLUDED.sum)/(pi_sensor_data.count+1),
 sum=pi_sensor_data.sum+EXCLUDED.sum,
 min=LEAST(pi_sensor_data.min,EXCLUDED.min),max=GREATEST(pi_sensor_data.max,EXCLUDED.max),
 last=CASE WHEN EXCLUDED.last_ts>=pi_sensor_data.last_ts THEN EXCLUDED.last ELSE pi_sensor_data.last END,
 last_ts=GREATEST(pi_sensor_data.last_ts,EXCLUDED.last_ts)`

const hourlySQL = `INSERT INTO pi_sensor_data(sensor,metric,interval,ts,count,avg,min,max,last,sum,last_ts)
SELECT sensor,metric,'1h',date_trunc('hour',ts),sum(count)::integer,sum(sum)/sum(count),min(min),max(max),
 (array_agg(last ORDER BY last_ts DESC))[1],sum(sum),max(last_ts)
FROM pi_sensor_data WHERE interval='15m' AND ts<date_trunc('hour',now())
 AND ts>=date_trunc('hour',now())-interval '8 days'
GROUP BY sensor,metric,date_trunc('hour',ts)
ON CONFLICT(sensor,metric,interval,ts) DO UPDATE SET count=EXCLUDED.count,avg=EXCLUDED.avg,min=EXCLUDED.min,max=EXCLUDED.max,last=EXCLUDED.last,sum=EXCLUDED.sum,last_ts=EXCLUDED.last_ts
WHERE (pi_sensor_data.count,pi_sensor_data.sum,pi_sensor_data.last_ts) IS DISTINCT FROM (EXCLUDED.count,EXCLUDED.sum,EXCLUDED.last_ts)`

func applyBatch(ctx context.Context, tx *sql.Tx, b batch) error {
	result, err := tx.ExecContext(ctx, "INSERT INTO pi_sensor_ingest_batches(batch_id) VALUES($1) ON CONFLICT DO NOTHING", b.ID)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return nil
	}
	for _, e := range b.Events {
		result, err = tx.ExecContext(ctx, "INSERT INTO pi_sensor_ingest_events(event_id) VALUES($1) ON CONFLICT DO NOTHING", e.ID)
		if err != nil {
			return err
		}
		n, _ = result.RowsAffected()
		if n == 0 {
			continue
		}
		for metric, value := range e.Values {
			if _, err = tx.ExecContext(ctx, addSQL, e.Sensor, metric, e.TS/900*900, value, e.TS); err != nil {
				return err
			}
		}
	}
	return nil
}

func main() {
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

	queue := make(chan pending, 2000)
	var diskOK atomic.Bool
	diskOK.Store(true)

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
				
				// 1. Immediately push to Redis for live dashboard
				for metric, value := range e.Values {
					redisKey := fmt.Sprintf("pi-mix:raw:sensor_%s", metric)
					member := fmt.Sprintf("%d:%f", e.TS, value)
					rdb.ZAdd(context.Background(), redisKey, redis.Z{Score: float64(e.TS), Member: member})
					rdb.ZRemRangeByScore(context.Background(), redisKey, "-inf", fmt.Sprintf("%d", time.Now().Add(-25*time.Hour).Unix()))
				}
				
				// 2. Queue for Postgres bulk archive
				select {
				case queue <- pending{e, m.Ack}:
				case <-ctx.Done():
				}
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
		if !client.IsConnected() || !diskOK.Load() || len(queue) == cap(queue) {
			http.Error(w, "cannot ingest safely", 503)
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

	tick := time.NewTicker(time.Hour) // 1 Hour bulk flush interval to protect disk
	defer tick.Stop()
	
	items := make([]pending, 0, 5000)

	flushToPostgres := func() {
		if len(items) == 0 {
			return
		}
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer flushCancel()
		
		b := batch{ID: newID()}
		for _, p := range items {
			b.Events = append(b.Events, p.event)
		}
		
		tx, err := db.BeginTx(flushCtx, nil)
		if err != nil {
			diskOK.Store(false)
			log.Printf("database retry: %v", err)
			return
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(flushCtx, "SET LOCAL TIME ZONE 'UTC'"); err == nil {
			err = applyBatch(flushCtx, tx, b)
		}
		if err == nil {
			_, err = tx.ExecContext(flushCtx, hourlySQL)
		}
		if err == nil {
			_, err = tx.ExecContext(flushCtx, "DELETE FROM pi_sensor_ingest_events WHERE committed_at<now()-interval '9 days'")
		}
		if err == nil {
			err = tx.Commit()
		}
		
		if err != nil {
			diskOK.Store(false)
			log.Printf("postgres flush failed (data unacknowledged): %v", err)
		} else {
			diskOK.Store(true)
			for _, p := range items {
				p.ack()
			}
			items = items[:0]
		}
	}

run:
	for {
		input := queue
		if len(items) >= cap(items) {
			input = nil // Block input, Mosquitto will queue
		}
		select {
		case p := <-input:
			items = append(items, p)
			if len(items) >= 4500 {
				flushToPostgres()
			}
		case <-tick.C:
			flushToPostgres()
		case <-ctx.Done():
			break run
		}
	}

	client.Disconnect(250)
	flushToPostgres()
	shutdown, done := context.WithTimeout(context.Background(), 3*time.Second)
	defer done()
	server.Shutdown(shutdown)
}
