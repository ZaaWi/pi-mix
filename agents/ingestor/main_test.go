package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

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
