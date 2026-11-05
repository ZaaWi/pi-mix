package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// fakeMsg implements mqtt.Message; Ack records the ack.
type fakeMsg struct {
	acked   atomic.Bool
	topic   string
	payload []byte
}

func (*fakeMsg) Duplicate() bool   { return false }
func (*fakeMsg) Dup() bool         { return false }
func (*fakeMsg) Qos() byte         { return 1 }
func (*fakeMsg) Retained() bool    { return false }
func (f *fakeMsg) Topic() string   { return f.topic }
func (*fakeMsg) MessageID() uint16 { return 1 }
func (f *fakeMsg) Payload() []byte { return f.payload }
func (f *fakeMsg) Ack()            { f.acked.Store(true) }

func resetStall() {
	stuckMu.Lock()
	stuckMsg = nil
	stuckSince = time.Time{}
	stuckMu.Unlock()
}

func TestMergeStallWatchdogAcksAfterGrace(t *testing.T) {
	resetStall()
	m := &fakeMsg{}
	markMergeStall(m)
	if m.acked.Load() {
		t.Fatal("acked before grace period elapsed")
	}
	releaseMergeStall()
	if !m.acked.Load() {
		t.Fatal("parked message was not acked after grace")
	}
	m2 := &fakeMsg{}
	markMergeStall(m2)
	releaseMergeStall()
	if !m2.acked.Load() {
		t.Fatal("second parked message was not acked")
	}
}

func TestMergeStallOnlyOneParked(t *testing.T) {
	resetStall()
	m1, m2 := &fakeMsg{}, &fakeMsg{}
	markMergeStall(m1)
	markMergeStall(m2)
	releaseMergeStall()
	if !m1.acked.Load() {
		t.Fatal("first parked message was not acked")
	}
	if m2.acked.Load() {
		t.Fatal("second message was acked by the same watchdog")
	}
}

func TestMergeStallClearedOnSuccess(t *testing.T) {
	resetStall()
	m := &fakeMsg{}
	markMergeStall(m)
	clearMergeStall()
	releaseMergeStall()
	if m.acked.Load() {
		t.Fatal("cleared message was acked")
	}
}

func TestParseCudyEvent(t *testing.T) {
	now := time.Now()
	topic := "cudy/telemetry/rsrp"
	good := map[string]any{"ts": now.Unix(), "value": -74.0, "unit": "dBm"}
	e, err := parseCudyEvent(mustJSON(t, good), topic, now)
	if err != nil {
		t.Fatal(err)
	}
	if e.Sensor != "cudy" || len(e.Values) != 1 || e.Values["cudy_rsrp"] != -74 {
		t.Fatalf("unexpected event: %+v", e)
	}
	if want := fmt.Sprintf("cudy:rsrp:%d", now.Unix()); e.ID != want {
		t.Fatalf("event id = %q, want %q (deterministic for dedup)", e.ID, want)
	}

	// Same (metric, ts) must yield the same id so redelivery dedups.
	e2, err := parseCudyEvent(mustJSON(t, good), topic, now)
	if err != nil {
		t.Fatal(err)
	}
	if e.ID != e2.ID {
		t.Fatalf("redelivered sample produced different event id")
	}

	cases := []struct {
		name  string
		topic string
		body  map[string]any
	}{
		{"unknown metric", "cudy/telemetry/nope", good},
		{"wrong depth", "cudy/nope", good},
		{"wrong root", "pi/telemetry/rsrp", good},
		{"non-numeric value", topic, map[string]any{"ts": now.Unix(), "value": "cold", "unit": "dBm"}},
		{"old timestamp", topic, map[string]any{"ts": now.Add(-8 * 24 * time.Hour).Unix(), "value": -74}},
		{"future timestamp", topic, map[string]any{"ts": now.Add(10 * time.Minute).Unix(), "value": -74}},
	}
	for _, c := range cases {
		if _, err := parseCudyEvent(mustJSON(t, c.body), c.topic, now); err == nil {
			t.Errorf("%s: expected error", c.name)
		}
	}
	if _, err := parseCudyEvent(make([]byte, 4097), topic, now); err == nil {
		t.Fatal("expected payload size rejection")
	}

	// ts fallback: missing ts uses the ingest time.
	if _, err := parseCudyEvent(mustJSON(t, map[string]any{"value": 31, "unit": "dBm"}), "cudy/telemetry/rssi", now); err != nil {
		t.Fatalf("missing ts should fall back to now: %v", err)
	}
}

func TestRouteHandlerUsesTopicParser(t *testing.T) {
	resetStall()
	// A malformed cudy payload must take the cudy parse path and be acked
	// (parse error fast-path), never reaching Redis.
	m := &fakeMsg{topic: "cudy/telemetry/rsrp", payload: mustJSON(t, map[string]any{"value": "nope"})}
	routeHandler("cudy/telemetry/+", nil)(nil, m)
	if !m.acked.Load() {
		t.Fatal("malformed cudy payload should be acked, not merged")
	}

	// Same malformed-ish payload under pi/dht11 must use parseEvent, which
	// also rejects it and acks without Redis.
	m2 := &fakeMsg{topic: "pi/dht11", payload: mustJSON(t, map[string]any{"sensor": "dht11"})}
	routeHandler("pi/dht11", nil)(nil, m2)
	if !m2.acked.Load() {
		t.Fatal("malformed pi payload should be acked, not merged")
	}
}

func TestParseEvent(t *testing.T) {
	now := time.Now()
	good := map[string]any{"sensor": "dht11", "ts": now.Unix(), "temp_c": 21.5, "humidity_pct": 60.0, "event_id": "x1"}
	e, err := parseEvent(mustJSON(t, good), now)
	if err != nil {
		t.Fatal(err)
	}
	if e.ID != "x1" || e.Sensor != "dht11" || len(e.Values) != 2 || e.Values["temp_c"] != 21.5 {
		t.Fatalf("unexpected event: %+v", e)
	}

	cases := []map[string]any{
		{"sensor": "nope", "ts": now.Unix(), "temp_c": 1.0},                           // unknown sensor
		{"sensor": "dht11", "ts": now.Add(-8 * 24 * time.Hour).Unix(), "temp_c": 1.0}, // old
		{"sensor": "dht11", "ts": now.Add(10 * time.Minute).Unix(), "temp_c": 1.0},    // future
		{"sensor": "ldr", "ts": now.Unix()},                                           // no values
	}
	for i, c := range cases {
		if _, err := parseEvent(mustJSON(t, c), now); err == nil {
			t.Fatalf("case %d: expected error for %v", i, c)
		}
	}
	if _, err := parseEvent(make([]byte, 4097), now); err == nil {
		t.Fatal("expected payload size rejection")
	}
}

func TestAggAvgAndAdd(t *testing.T) {
	a := agg{C: 2, S: 44, Mn: 21, Mx: 23, L: 23, Lt: 100}
	if got := a.avg(); got != 22 {
		t.Fatalf("avg = %v, want 22", got)
	}
	total := agg{Mn: +100, Mx: -100}
	for _, c := range []agg{{C: 1, S: 10, Mn: 10, Mx: 10, L: 10, Lt: 50}, {C: 3, S: 30, Mn: 8, Mx: 12, L: 12, Lt: 60}, {C: 2, S: 20, Mn: 9, Mx: 11, L: 9, Lt: 45}} {
		addAgg(&total, c)
	}
	if total.C != 6 || total.S != 60 || total.Mn != 8 || total.Mx != 12 || total.L != 12 || total.Lt != 60 {
		t.Fatalf("merge wrong: %+v", total)
	}
}

func mustJSON(t *testing.T, in map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping Redis integration")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func testPG(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set; skipping PostgreSQL integration")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestLuaMergeIntegration(t *testing.T) {
	rdb := testRedis(t)
	db := testPG(t)
	ctx := context.Background()

	hour := time.Now().Add(-3 * time.Hour).Truncate(time.Hour)
	base := hour.Unix() + 15*60 // middle 15m bucket of a closed hour
	now := time.Now()
	keys := []string{seriesKey("raw", "temp_c"), seriesKey("15m", "temp_c"), seriesKey("1h", "temp_c"), dirtyKey("temp_c"), seenKey(now)}
	for _, k := range keys {
		rdb.Del(ctx, k)
	}
	defer func() {
		for _, k := range keys {
			rdb.Del(context.Background(), k)
		}
		db.ExecContext(context.Background(), `DELETE FROM pi_sensor_data WHERE sensor='dht11' AND metric='temp_c' AND ts>=to_timestamp($1) AND ts<=to_timestamp($2)`, hour.Unix(), hour.Unix()+3599)
	}()

	e1 := event{ID: "evt-1", Sensor: "dht11", TS: base, Values: map[string]float64{"temp_c": 21.0}}
	e2 := event{ID: "evt-2", Sensor: "dht11", TS: base + 60, Values: map[string]float64{"temp_c": 23.0}}
	if err := ingestSample(ctx, rdb, e1, now); err != nil {
		t.Fatal(err)
	}
	if err := ingestSample(ctx, rdb, e2, now); err != nil {
		t.Fatal(err)
	}
	if err := ingestSample(ctx, rdb, e1, now); err != nil { // duplicate must dedup
		t.Fatal(err)
	}

	// 15m: exactly one aggregate member with both samples folded in.
	items, err := rdb.ZRangeByScoreWithScores(ctx, seriesKey("15m", "temp_c"), &redis.ZRangeBy{Min: "0", Max: "+inf"}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one 15m bucket, got %d", len(items))
	}
	var a agg
	member, _ := items[0].Member.(string)
	if err := json.Unmarshal([]byte(member), &a); err != nil {
		t.Fatal(err)
	}
	want := agg{C: 2, S: 44, Mn: 21, Mx: 23, L: 23, Lt: base + 60}
	if a != want {
		t.Fatalf("15m agg = %+v, want %+v", a, want)
	}

	// Raw: two samples, one per event.
	raw, err := rdb.ZRangeByScore(ctx, seriesKey("raw", "temp_c"), &redis.ZRangeBy{Min: "0", Max: "+inf"}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 2 {
		t.Fatalf("expected 2 raw samples, got %d", len(raw))
	}

	// Dirty: the owning hour marked twice (both samples, same hour).
	dirty, err := rdb.HGetAll(ctx, dirtyKey("temp_c")).Result()
	if err != nil {
		t.Fatal(err)
	}
	hourStr := strconv.FormatInt(hour.Unix(), 10)
	if dirty[hourStr] != "2" || len(dirty) != 1 {
		t.Fatalf("unexpected dirty hash: %v", dirty)
	}

	// Roll-up a closed hour writes 1x15m + 1x1h rows; 1h avg = 22.
	if err := rollupHour(ctx, rdb, db, "temp_c", hour.Unix()); err != nil {
		t.Fatal(err)
	}
	var count15, count1h int
	var avg1h float64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pi_sensor_data WHERE sensor='dht11' AND metric='temp_c' AND interval='15m' AND ts=to_timestamp($1)`, base).Scan(&count15); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pi_sensor_data WHERE sensor='dht11' AND metric='temp_c' AND interval='1h' AND ts=to_timestamp($1)`, hour.Unix()).Scan(&count1h); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT avg FROM pi_sensor_data WHERE sensor='dht11' AND metric='temp_c' AND interval='1h' AND ts=to_timestamp($1)`, hour.Unix()).Scan(&avg1h); err != nil {
		t.Fatal(err)
	}
	if count15 != 1 || count1h != 1 || avg1h != 22.0 {
		t.Fatalf("rows: 15m=%d 1h=%d avg=%v", count15, count1h, avg1h)
	}

	// HDEL on success: another call must be a no-op (all buckets already gone).
	if err := rollupHour(ctx, rdb, db, "temp_c", hour.Unix()); err != nil {
		t.Fatal(err)
	}
}

func TestLuaMergeCollapsesDuplicateBucketMembers(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	now := time.Now()
	bucket := now.Unix() / bucketSecs * bucketSecs
	ts := bucket + 120
	keys := []string{seriesKey("raw", "temp_c"), seriesKey("15m", "temp_c"), dirtyKey("temp_c"), seenKey(now)}
	for _, k := range keys {
		rdb.Del(ctx, k)
	}
	defer func() {
		for _, k := range keys {
			rdb.Del(context.Background(), k)
		}
	}()

	m1, _ := json.Marshal(agg{C: 1, S: 10, Mn: 10, Mx: 10, L: 10, Lt: bucket})
	m2, _ := json.Marshal(agg{C: 5, S: 50, Mn: 10, Mx: 10, L: 10, Lt: bucket + 60})
	if err := rdb.ZAdd(ctx, seriesKey("15m", "temp_c"),
		redis.Z{Score: float64(bucket), Member: string(m1)},
		redis.Z{Score: float64(bucket), Member: string(m2)}).Err(); err != nil {
		t.Fatal(err)
	}

	e := event{ID: "dup-merge", Sensor: "dht11", TS: ts, Values: map[string]float64{"temp_c": 10}}
	if err := ingestSample(ctx, rdb, e, now); err != nil {
		t.Fatal(err)
	}

	items, err := rdb.ZRangeByScoreWithScores(ctx, seriesKey("15m", "temp_c"), &redis.ZRangeBy{Min: "0", Max: "+inf"}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one bucket after merge, got %d", len(items))
	}
	var got agg
	member, _ := items[0].Member.(string)
	if err := json.Unmarshal([]byte(member), &got); err != nil {
		t.Fatal(err)
	}
	want := agg{C: 6, S: 60, Mn: 10, Mx: 10, L: 10, Lt: ts}
	if got != want {
		t.Fatalf("merged agg = %+v, want %+v", got, want)
	}
}

func TestLuaBackfillCollapsesDuplicateMembers(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	key := "pi-mix:15m:sensor_test_backfill"
	score := int64(1795000000)
	rdb.Del(ctx, key)
	defer rdb.Del(context.Background(), key)

	live1, _ := json.Marshal(agg{C: 2, S: 20, Mn: 10, Mx: 10, L: 10, Lt: score + 10})
	live2, _ := json.Marshal(agg{C: 9, S: 90, Mn: 10, Mx: 10, L: 10, Lt: score + 20})
	incoming, _ := json.Marshal(agg{C: 5, S: 50, Mn: 10, Mx: 10, L: 10, Lt: score + 30})
	if err := rdb.ZAdd(ctx, key,
		redis.Z{Score: float64(score), Member: string(live1)},
		redis.Z{Score: float64(score), Member: string(live2)}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Eval(ctx, luaBackfill, []string{key}, score, string(incoming)).Err(); err != nil {
		t.Fatal(err)
	}

	items, err := rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one backfilled member, got %d", len(items))
	}
	var got agg
	member, _ := items[0].Member.(string)
	if err := json.Unmarshal([]byte(member), &got); err != nil {
		t.Fatal(err)
	}
	if got.C != 9 {
		t.Fatalf("expected the more complete live aggregate (c=9), got %+v", got)
	}

	better, _ := json.Marshal(agg{C: 12, S: 120, Mn: 10, Mx: 10, L: 10, Lt: score + 40})
	if err := rdb.Eval(ctx, luaBackfill, []string{key}, score, string(better)).Err(); err != nil {
		t.Fatal(err)
	}
	items, err = rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one member after replacement, got %d", len(items))
	}
	member, _ = items[0].Member.(string)
	if err := json.Unmarshal([]byte(member), &got); err != nil {
		t.Fatal(err)
	}
	if got.C != 12 {
		t.Fatalf("expected the more complete backfill aggregate (c=12), got %+v", got)
	}
}

func TestRollupCollapsesDuplicateMembersAndUpdates1h(t *testing.T) {
	rdb := testRedis(t)
	db := testPG(t)
	ctx := context.Background()
	now := time.Now()
	hour := now.Add(-5 * time.Hour).Truncate(time.Hour)
	base := hour.Unix() + 15*60
	hourStr := strconv.FormatInt(hour.Unix(), 10)
	endStr := strconv.FormatInt(hour.Unix()+3599, 10)

	rdb.ZRemRangeByScore(ctx, seriesKey("15m", "temp_c"), hourStr, endStr)
	rdb.ZRemRangeByScore(ctx, seriesKey("1h", "temp_c"), hourStr, hourStr)
	rdb.HDel(ctx, dirtyKey("temp_c"), hourStr)
	db.ExecContext(ctx, `DELETE FROM pi_sensor_data WHERE sensor='dht11' AND metric='temp_c' AND ts>=to_timestamp($1) AND ts<=to_timestamp($2)`, hour.Unix(), hour.Unix()+3599)
	defer func() {
		rdb.ZRemRangeByScore(context.Background(), seriesKey("15m", "temp_c"), hourStr, endStr)
		rdb.ZRemRangeByScore(context.Background(), seriesKey("1h", "temp_c"), hourStr, hourStr)
		rdb.HDel(context.Background(), dirtyKey("temp_c"), hourStr)
		db.ExecContext(context.Background(), `DELETE FROM pi_sensor_data WHERE sensor='dht11' AND metric='temp_c' AND ts>=to_timestamp($1) AND ts<=to_timestamp($2)`, hour.Unix(), hour.Unix()+3599)
	}()

	m1, _ := json.Marshal(agg{C: 1, S: 10, Mn: 10, Mx: 10, L: 10, Lt: base})
	m2, _ := json.Marshal(agg{C: 5, S: 50, Mn: 10, Mx: 10, L: 10, Lt: base + 60})
	if err := rdb.ZAdd(ctx, seriesKey("15m", "temp_c"),
		redis.Z{Score: float64(base), Member: string(m1)},
		redis.Z{Score: float64(base), Member: string(m2)}).Err(); err != nil {
		t.Fatal(err)
	}

	if err := rollupHour(ctx, rdb, db, "temp_c", hour.Unix()); err != nil {
		t.Fatal(err)
	}

	var count15, count1h int64
	if err := db.QueryRowContext(ctx, `SELECT count FROM pi_sensor_data WHERE sensor='dht11' AND metric='temp_c' AND interval='15m' AND ts=to_timestamp($1)`, base).Scan(&count15); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count FROM pi_sensor_data WHERE sensor='dht11' AND metric='temp_c' AND interval='1h' AND ts=to_timestamp($1)`, hour.Unix()).Scan(&count1h); err != nil {
		t.Fatal(err)
	}
	if count15 != 5 || count1h != 5 {
		t.Fatalf("rolled up counts = 15m:%d 1h:%d, want 5/5", count15, count1h)
	}

	items, err := rdb.ZRangeByScoreWithScores(ctx, seriesKey("1h", "temp_c"), &redis.ZRangeBy{Min: hourStr, Max: hourStr}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one 1h Redis member, got %d", len(items))
	}
	var a agg
	member, _ := items[0].Member.(string)
	if err := json.Unmarshal([]byte(member), &a); err != nil {
		t.Fatal(err)
	}
	if a.C != 5 {
		t.Fatalf("1h Redis aggregate = %+v, want c=5", a)
	}
}

func TestFlushRowsDeduplicatesConflictKeys(t *testing.T) {
	db := testPG(t)
	ctx := context.Background()
	ts := time.Now().Add(-90 * time.Minute).Truncate(15 * time.Minute).Unix()
	sensor, metric, interval := "test_dedupe", "m", "15m"
	_, _ = db.ExecContext(ctx, `DELETE FROM pi_sensor_data WHERE sensor=$1 AND metric=$2 AND interval=$3 AND ts=to_timestamp($4)`, sensor, metric, interval, ts)
	defer db.ExecContext(context.Background(), `DELETE FROM pi_sensor_data WHERE sensor=$1 AND metric=$2 AND interval=$3 AND ts=to_timestamp($4)`, sensor, metric, interval, ts)

	rows := []pgRow{
		{sensor: sensor, metric: metric, interval: interval, ts: ts, count: 2, avg: 2, min: 2, max: 2, last: 2, sum: 4, lastTS: ts + 10},
		{sensor: sensor, metric: metric, interval: interval, ts: ts, count: 5, avg: 5, min: 5, max: 5, last: 5, sum: 25, lastTS: ts + 20},
	}
	if err := flushRows(ctx, db, rows); err != nil {
		t.Fatal(err)
	}

	var n, count, lastTS int64
	if err := db.QueryRowContext(ctx, `SELECT count(*), max(count), max(extract(epoch from last_ts)::bigint) FROM pi_sensor_data WHERE sensor=$1 AND metric=$2 AND interval=$3 AND ts=to_timestamp($4)`, sensor, metric, interval, ts).Scan(&n, &count, &lastTS); err != nil {
		t.Fatal(err)
	}
	if n != 1 || count != 5 || lastTS != ts+20 {
		t.Fatalf("deduped row: n=%d count=%d last_ts=%d, want 1/5/%d", n, count, lastTS, ts+20)
	}
}

func TestBackfillIntegration(t *testing.T) {
	rdb := testRedis(t)
	db := testPG(t)
	ctx := context.Background()

	key := seriesKey("15m", "temp_c")
	rdb.Del(ctx, markerKey, key)
	defer func() {
		rdb.Del(context.Background(), markerKey, key)
		db.ExecContext(context.Background(), `DELETE FROM pi_sensor_data WHERE sensor='test_ing'`)
	}()
	ts := time.Now().Add(-2 * time.Hour).Truncate(time.Hour)
	if _, err := db.ExecContext(ctx, `INSERT INTO pi_sensor_data(sensor,metric,interval,ts,count,avg,min,max,last,sum,last_ts)
VALUES('test_ing','temp_c','15m',to_timestamp($1),2,22,21,23,23,44,to_timestamp($1))`, ts.Unix()); err != nil {
		t.Fatal(err)
	}

	if err := ensureBackfill(ctx, rdb, db, defaultConfig()); err != nil {
		t.Fatal(err)
	}
	items, err := rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{Min: "0", Max: "+inf"}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 backfilled bucket, got %d", len(items))
	}
	var a agg
	member, _ := items[0].Member.(string)
	if err := json.Unmarshal([]byte(member), &a); err != nil {
		t.Fatal(err)
	}
	if a.C != 2 || a.S != 44 || a.Mn != 21 || a.Mx != 23 {
		t.Fatalf("backfilled agg = %+v", a)
	}
}
