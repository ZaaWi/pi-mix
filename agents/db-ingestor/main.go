// Sensor events are checkpointed as immutable batches on the storage node.
// MQTT is acknowledged only after fsync; PostgreSQL replay is idempotent.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/lib/pq"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
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
	ID     string  `json:"id"`
	Events []event `json:"events"`
}
type spool struct {
	dir  string
	mu   sync.Mutex
	lock *os.File
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func openSpool(dir string) (*spool, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another spool writer is active: %w", err)
	}
	old, _ := filepath.Glob(filepath.Join(dir, "15m-*.jsonl"))
	if len(old) > 0 {
		f.Close()
		return nil, fmt.Errorf("legacy spill files require migration before startup; originals preserved")
	}
	return &spool{dir: dir, lock: f}, nil
}
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func (s *spool) checkpoint(items []pending) error {
	if len(items) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b := batch{ID: newID()}
	for _, p := range items {
		b.Events = append(b.Events, p.event)
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.dir, ".checkpoint-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(tmp, filepath.Join(s.dir, "batch-"+b.ID+".json")); err != nil {
		return err
	}
	if err = syncDir(s.dir); err != nil {
		return err
	}
	for _, p := range items {
		p.ack()
	}
	return nil
}
func parseEvent(raw []byte, now time.Time) (event, error) {
	if len(raw) > 4096 {
		return event{}, fmt.Errorf("payload exceeds limit")
	}
	var p map[string]any
	if err := json.Unmarshal(raw, &p); err != nil {
		return event{}, err
	}
	sensor, _ := p["sensor"].(string)
	keys := map[string][]string{"dht11": {"temp_c", "humidity_pct"}, "ldr": {"analog"}, "scale": {"weight_kg"}}
	allowed, ok := keys[sensor]
	if !ok {
		return event{}, fmt.Errorf("unsupported sensor")
	}
	ts, ok := p["ts"].(float64)
	if !ok {
		return event{}, fmt.Errorf("missing source timestamp")
	}
	if ts < float64(now.Add(-7*24*time.Hour).Unix()) || ts > float64(now.Add(5*time.Minute).Unix()) {
		return event{}, fmt.Errorf("timestamp outside accepted window")
	}
	id, _ := p["event_id"].(string)
	if len(id) > 128 {
		return event{}, fmt.Errorf("event ID too long")
	}
	if id == "" {
		id = newID()
	} // Legacy publishers: at-least-once, until upgraded.
	e := event{ID: id, Sensor: sensor, TS: int64(ts), Values: map[string]float64{}}
	for _, key := range allowed {
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
func (s *spool) drain(db *sql.DB) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	files, err := filepath.Glob(filepath.Join(s.dir, "batch-*.json"))
	if err != nil {
		return err
	}
	if len(files) > 60 {
		files = files[:60]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SET LOCAL TIME ZONE 'UTC'"); err != nil {
		return err
	}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var b batch
		if err = json.Unmarshal(raw, &b); err != nil {
			return fmt.Errorf("invalid spool %s: %w", path, err)
		}
		if b.ID == "" {
			return fmt.Errorf("empty batch identity")
		}
		if err = applyBatch(ctx, tx, b); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, hourlySQL); err != nil {
		return err
	}
	// Event acceptance is limited to seven days; retain deduplication beyond that.
	if _, err = tx.ExecContext(ctx, "DELETE FROM pi_sensor_ingest_events WHERE committed_at<now()-interval '9 days'"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	for _, path := range files {
		if err = os.Remove(path); err != nil {
			return err
		}
	}
	return syncDir(s.dir)
}

func main() {
	s, err := openSpool(env("SPILL_DIR", "/var/lib/db-ingestor/spill"))
	if err != nil {
		log.Fatal(err)
	}
	defer s.lock.Close()
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
	opts := mqtt.NewClientOptions().AddBroker(env("MQTT_BROKER", "tcp://mosquitto.iot.svc.cluster.local:1883")).SetClientID(env("MQTT_CLIENT_ID", "pi-mix-postgres"))
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
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		c, done := context.WithTimeout(r.Context(), 2*time.Second)
		defer done()
		err := db.PingContext(c)
		files, _ := filepath.Glob(filepath.Join(s.dir, "batch-*.json"))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"mqtt_connected": client.IsConnected(), "postgres_connected": err == nil, "pending_batches": len(files), "checkpoint_ok": diskOK.Load()})
	})
	server := &http.Server{Addr: env("HTTP_ADDR", ":8090"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Print(err)
			cancel()
		}
	}()
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for {
			if err := s.drain(db); err != nil {
				log.Printf("database retry: %v", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	items := make([]pending, 0, 100)
	checkpoint := func() {
		if err := s.checkpoint(items); err != nil {
			diskOK.Store(false)
			log.Printf("checkpoint failed (data unacknowledged): %v", err)
		} else {
			diskOK.Store(true)
			items = items[:0]
		}
	}
run:
	for {
		input := queue
		if len(items) >= 2000 {
			input = nil
		}
		select {
		case p := <-input:
			items = append(items, p)
		case <-tick.C:
			checkpoint()
		case <-ctx.Done():
			break run
		}
	}
	// Stop producers, include all already queued events, then persist before exit.
	client.Disconnect(250)
	for {
		select {
		case p := <-queue:
			items = append(items, p)
		default:
			goto flushed
		}
	}
flushed:
	checkpoint()
	<-drainDone
	shutdown, done := context.WithTimeout(context.Background(), 3*time.Second)
	defer done()
	server.Shutdown(shutdown)
}
