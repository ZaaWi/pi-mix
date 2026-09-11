package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestParseHistoryArgs(t *testing.T) {
	base := "/api/history?query=%7B__name__%3D%22sensor_temp_c%22%7D&start=100&end=200&step=15s"
	r := httptest.NewRequest("GET", base, nil)
	start, end, metric, err := parseHistoryArgs(r)
	if err != nil || start != 100 || end != 200 || metric != "sensor_temp_c" {
		t.Fatalf("got (%d,%d,%q,%v)", start, end, metric, err)
	}

	for _, bad := range []string{
		"/api/history",
		"/api/history?start=10&end=5",
		"/api/history?start=-1&end=5&query=x",
		"/api/history?start=1&end=5&query=nomatch",
	} {
		if _, _, _, err := parseHistoryArgs(httptest.NewRequest("GET", bad, nil)); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}

func TestMetricToSensor(t *testing.T) {
	cases := map[string]struct {
		sensor, name string
		ok           bool
	}{
		"sensor_temp_c":       {"dht11", "temp_c", true},
		"sensor_humidity_pct": {"dht11", "humidity_pct", true},
		"sensor_analog":       {"ldr", "analog", true},
		"sensor_weight_kg":    {"scale", "weight_kg", true},
		"sensor_bits":         {"ir", "bits", true},
		"temp_c":              {"", "", false},
		"sensor_unknown":      {"", "", false},
	}
	for in, want := range cases {
		sensor, name, ok := metricToSensor(in)
		if sensor != want.sensor || name != want.name || ok != want.ok {
			t.Errorf("%q -> (%q,%q,%v), want (%q,%q,%v)", in, sensor, name, ok, want.sensor, want.name, want.ok)
		}
	}
}

func TestPickSeries(t *testing.T) {
	cases := map[int64]string{
		0:               "raw",
		5 * 3600:        "raw",
		6 * 3600:        "raw",
		6*3600 + 1:      "15m",
		30 * 24 * 3600:  "15m",
		30*24*3600 + 1:  "1h",
		365 * 24 * 3600: "1h",
	}
	for width, want := range cases {
		if got := pickSeries(width); got != want {
			t.Errorf("pickSeries(%d) = %q, want %q", width, got, want)
		}
	}
}

func TestSeriesKey(t *testing.T) {
	if seriesKey("15m", "temp_c") != "pi-mix:15m:sensor_temp_c" {
		t.Fatalf("unexpected key: %s", seriesKey("15m", "temp_c"))
	}
}

func TestParseRedisMembers(t *testing.T) {
	members := []string{"100:25.500000", "101:26.100000", "garbage", "102:nan", "103:27,5"}
	pts := parseRedisMembers(members)
	if len(pts) != 2 {
		t.Fatalf("expected 2 parsed points, got %d (%v)", len(pts), pts)
	}
	if pts[0].ts != 100 || pts[0].val != 25.5 || pts[1].val != 26.1 {
		t.Fatalf("unexpected points: %+v", pts)
	}
}

func TestParseAggMembers(t *testing.T) {
	a := agg{C: 2, S: 51, Mn: 25, Mx: 26, L: 26, Lt: 200}
	mb, _ := json.Marshal(a)
	items := []redis.Z{{Score: 150, Member: string(mb)}, {Score: 151, Member: "not-json"}}
	pts := parseAggMembers(items)
	if len(pts) != 1 || pts[0].ts != 150 || pts[0].val != 25.5 {
		t.Fatalf("unexpected points: %+v", pts)
	}
}

func TestDecimate(t *testing.T) {
	small := make([]point, 100)
	for i := range small {
		small[i] = point{ts: int64(i), val: float64(i)}
	}
	if got, want := len(decimate(small)), 100; got != want {
		t.Fatalf("decimate should not touch small slices: got %d want %d", got, want)
	}

	big := make([]point, 10000)
	for i := range big {
		big[i] = point{ts: int64(i), val: float64(i)}
	}
	out := decimate(big)
	if len(out) > maxHistoryPoints+1 {
		t.Fatalf("decimated size %d exceeds cap", len(out))
	}
	if out[0].ts != 0 || out[len(out)-1].ts != 9999 {
		t.Fatalf("decimate must keep first and last: %v..%v", out[0].ts, out[len(out)-1].ts)
	}
	for i := 1; i < len(out); i++ {
		if out[i].ts <= out[i-1].ts {
			t.Fatalf("decimated output not increasing")
		}
	}
}

func TestHistoryResponse(t *testing.T) {
	pts := []point{{1700000000, 21.5}}
	resp := historyResponse("sensor_temp_c", "dht11", pts)
	data := resp["data"].(map[string]any)
	series := data["result"].([]any)[0].(map[string]any)
	vals := series["values"].([][]any)
	if len(vals) != 1 || vals[0][0].(int64) != 1700000000 || vals[0][1].(float64) != 21.5 {
		t.Fatalf("unexpected history response: %v", resp)
	}
}

// TestResolveHistoryIntegration verifies the series cascade against a real
// Redis (GitHub Actions redis service) seeded with raw/15m/1h aggregates.
func TestResolveHistoryIntegration(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping Redis integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	now := time.Now().Truncate(time.Minute)
	rawTs := now.Unix() - 120
	a := agg{C: 3, S: 66, Mn: 21, Mx: 23, L: 23, Lt: now.Unix() - 60}
	mb, _ := json.Marshal(a)

	keys := []string{
		seriesKey("raw", "temp_c"),
		seriesKey("15m", "temp_c"),
		seriesKey("1h", "temp_c"),
	}
	for _, k := range keys {
		rdb.Del(ctx, k)
	}
	defer func() {
		for _, k := range keys {
			rdb.Del(context.Background(), k)
		}
	}()

	rdb.ZAdd(ctx, seriesKey("raw", "temp_c"), redis.Z{Score: float64(rawTs), Member: "1000:22.0"})
	rdb.ZAdd(ctx, seriesKey("15m", "temp_c"), redis.Z{Score: float64(now.Unix() - 3600), Member: string(mb)})
	rdb.ZAdd(ctx, seriesKey("1h", "temp_c"), redis.Z{Score: float64(now.Unix() - 7200), Member: string(mb)})

	// Narrow window resolves to raw.
	pts, src, err := resolveHistory(ctx, rdb, "temp_c", rawTs-1, rawTs+1)
	if err != nil || src != "raw" || len(pts) != 1 || pts[0].val != 22.0 {
		t.Fatalf("raw: pts=%v src=%q err=%v", pts, src, err)
	}
	// Mid window resolves to 15m (avg = 66/3 = 22).
	pts, src, err = resolveHistory(ctx, rdb, "temp_c", now.Unix()-7200, now.Unix())
	if err != nil || src != "15m" || len(pts) != 1 || pts[0].val != 22.0 {
		t.Fatalf("15m: pts=%v src=%q err=%v", pts, src, err)
	}
	// Wide window resolves to 1h.
	pts, src, err = resolveHistory(ctx, rdb, "temp_c", now.Unix()-3*24*3600, now.Unix())
	if err != nil || src != "1h" || len(pts) != 1 || pts[0].val != 22.0 {
		t.Fatalf("1h: pts=%v src=%q err=%v", pts, src, err)
	}
	// Empty Redis and unknown metric are not fatal: nil slice, no error.
	pts, src, err = resolveHistory(ctx, rdb, "weight_kg", now.Unix()-3600, now.Unix())
	if err != nil || src != "" || len(pts) != 0 {
		t.Fatalf("empty: pts=%v src=%q err=%v", pts, src, err)
	}
}
