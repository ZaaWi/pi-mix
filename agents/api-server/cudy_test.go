package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func resetCudyState() {
	cudyMu.Lock()
	cudyAreas = make(map[string]map[string]interface{})
	cudyMu.Unlock()
}

func TestHandleCudyStateFiltersSensitiveFields(t *testing.T) {
	resetCudyState()
	defer resetCudyState()

	payload := []byte(`{"ts":1700000000,"connected":true,"public_ip":"203.0.113.5","imsi":"123","imei":"456","iccid":"789","rsrp":-70}`)
	handleCudyState("cudy/state/cellular", payload)

	cudyMu.RLock()
	got := cudyAreas["cellular"]
	cudyMu.RUnlock()
	if got == nil {
		t.Fatal("cellular snapshot was not cached")
	}
	for _, field := range cudySensitiveFields {
		if _, ok := got[field]; ok {
			t.Fatalf("sensitive field %q leaked into the dashboard state", field)
		}
	}
	if got["public_ip"] != "203.0.113.5" {
		t.Fatalf("public_ip missing: %v", got)
	}
	if got["rsrp"] != float64(-70) {
		t.Fatalf("rsrp missing: %v", got)
	}
}

func TestHandleCudyStateRejectsUnlistedAreas(t *testing.T) {
	resetCudyState()
	defer resetCudyState()

	for _, topic := range []string{"cudy/state/arp", "cudy/state/routes", "cudy/state/nope", "cudy/state/"} {
		handleCudyState(topic, []byte(`{"entries":[]}`))
	}
	cudyMu.RLock()
	n := len(cudyAreas)
	cudyMu.RUnlock()
	if n != 0 {
		t.Fatalf("unexpected areas cached: %v", cudyAreas)
	}
}

func TestHandleStateIncludesCudy(t *testing.T) {
	resetCudyState()
	defer resetCudyState()

	handleCudyState("cudy/state/usage", []byte(`{"ts":1700000000,"monthly_bytes":123}`))
	rec := httptest.NewRecorder()
	handleState(rec, httptest.NewRequest("GET", "/api/state", nil))

	var body map[string]map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["cudy"] == nil {
		t.Fatalf("cudy missing from /api/state: %v", body)
	}
	if body["cudy"]["usage"] == nil {
		t.Fatalf("usage area missing: %v", body["cudy"])
	}
}

func TestHandleStatusIncludesCudy(t *testing.T) {
	resetCudyState()
	defer resetCudyState()

	handleCudyState("cudy/state/cellular", []byte(fmt.Sprintf(`{"ts":%d,"connected":true}`, time.Now().Unix())))
	rec := httptest.NewRecorder()
	handleStatus(rec, httptest.NewRequest("GET", "/api/status", nil))

	var body struct {
		Sensors map[string]struct {
			Stale bool `json:"stale"`
		} `json:"sensors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body.Sensors["cudy"]; !ok {
		t.Fatalf("cudy missing from /api/status: %v", body.Sensors)
	}
	if body.Sensors["cudy"].Stale {
		t.Fatal("fresh cudy snapshot reported stale")
	}
}

func resetCudyEvents() {
	cudyEventRing = newEventRing(500)
	cudyEventDedup.mu.Lock()
	cudyEventDedup.last = make(map[string]int64)
	cudyEventDedup.mu.Unlock()
	resetCudyDerived()
}

func eventKinds(events []cudyEvent) []string {
	out := make([]string, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.kind())
	}
	return out
}

func hasKind(events []cudyEvent, kind string) bool {
	for _, ev := range events {
		if ev.kind() == kind {
			return true
		}
	}
	return false
}

func TestHandleCudyEventStoresAndNormalizes(t *testing.T) {
	resetCudyEvents()
	defer resetCudyEvents()

	handleCudyEvent("cudy/events/public_ip", []byte(`{"id":"abc","ts":1700000000,"previous":"1.1.1.1","current":"2.2.2.2","imsi":"secret"}`))
	events := cudyEventRing.since(0, "", 0)
	if len(events) != 1 {
		t.Fatalf("expected one event, got %v", eventKinds(events))
	}
	ev := events[0]
	if ev.kind() != "public_ip" || ev.id() != "abc" || ev.ts() != 1700000000 {
		t.Fatalf("unexpected event: %v", ev)
	}
	if _, ok := ev["imsi"]; ok {
		t.Fatal("sensitive field leaked into event timeline")
	}
	if ev["current"] != "2.2.2.2" {
		t.Fatalf("payload fields lost: %v", ev)
	}
}

func TestHandleCudyEventIgnoresMalformed(t *testing.T) {
	resetCudyEvents()
	defer resetCudyEvents()

	handleCudyEvent("cudy/events/", []byte(`{}`))
	handleCudyEvent("cudy/events/a/b", []byte(`{}`))
	handleCudyEvent("cudy/events/x", []byte(`not json`))
	if events := cudyEventRing.since(0, "", 0); len(events) != 0 {
		t.Fatalf("malformed events stored: %v", eventKinds(events))
	}
}

func TestCudyEventDedupAcrossCollectorAndDerived(t *testing.T) {
	resetCudyEvents()
	defer resetCudyEvents()

	handleCudyEvent("cudy/events/cell_change", []byte(`{"id":"collector-1","ts":1700000000,"cell_id":"A","pcid":10,"band":7}`))
	// A derived event for the same transition must be suppressed.
	emitCudyEvent(cudyEvent{"kind": "cell_change", "cell_id": "A", "pcid": float64(10), "band": float64(7), "derived": true})
	if events := cudyEventRing.since(0, "", 0); len(events) != 1 {
		t.Fatalf("expected duplicate to be suppressed, got %v", eventKinds(events))
	}
}

func TestDeriveCudyEventsFromState(t *testing.T) {
	resetCudyEvents()
	defer resetCudyEvents()

	first := map[string]interface{}{
		"connected": true, "public_ip": "1.1.1.1",
		"cell_id": "A", "pcid": float64(10), "band": float64(7),
		"ca_count": float64(2), "scc_count": float64(1),
	}
	deriveCudyEvents("cellular", first)
	if events := cudyEventRing.since(0, "", 0); len(events) != 0 {
		t.Fatalf("first snapshot should only seed, got %v", eventKinds(events))
	}

	second := map[string]interface{}{
		"connected": false, "public_ip": "2.2.2.2",
		"cell_id": "B", "pcid": float64(11), "band": float64(3),
		"ca_count": float64(1), "scc_count": float64(0),
	}
	deriveCudyEvents("cellular", second)
	events := cudyEventRing.since(0, "", 0)
	for _, kind := range []string{"connectivity", "public_ip", "cell_change", "ca_change"} {
		if !hasKind(events, kind) {
			t.Fatalf("missing %s event: %v", kind, eventKinds(events))
		}
	}

	deriveCudyEvents("cellular", second)
	if events := cudyEventRing.since(0, "", 0); len(events) != 4 {
		t.Fatalf("unchanged snapshot should not emit, got %v", eventKinds(events))
	}
}

func TestDeriveClientJoinLeave(t *testing.T) {
	resetCudyEvents()
	defer resetCudyEvents()

	deriveCudyEvents("clients", map[string]interface{}{
		"clients": []interface{}{
			map[string]interface{}{"mac": "aa", "hostname": "one"},
			map[string]interface{}{"mac": "bb", "hostname": "two"},
		},
	})
	if events := cudyEventRing.since(0, "", 0); len(events) != 0 {
		t.Fatalf("first client snapshot should only seed, got %v", eventKinds(events))
	}

	deriveCudyEvents("clients", map[string]interface{}{
		"clients": []interface{}{
			map[string]interface{}{"mac": "aa", "hostname": "one"},
			map[string]interface{}{"mac": "cc", "hostname": "three"},
		},
	})
	events := cudyEventRing.since(0, "", 0)
	if !hasKind(events, "client_join") || !hasKind(events, "client_leave") {
		t.Fatalf("expected join and leave, got %v", eventKinds(events))
	}
}

func TestDeriveClientEventsSkipsTruncatedPayload(t *testing.T) {
	resetCudyEvents()
	defer resetCudyEvents()

	deriveCudyEvents("clients", map[string]interface{}{
		"clients": []interface{}{
			map[string]interface{}{"mac": "aa"},
			map[string]interface{}{"mac": "bb"},
		},
	})
	// A truncated snapshot omits clients; it must not look like a leave.
	deriveCudyEvents("clients", map[string]interface{}{
		"truncated": true,
		"clients":   []interface{}{map[string]interface{}{"mac": "aa"}},
	})
	if events := cudyEventRing.since(0, "", 0); len(events) != 0 {
		t.Fatalf("truncated snapshot emitted events: %v", eventKinds(events))
	}
	// The next complete snapshot reseeds, so the known client set is the
	// baseline again rather than a flood of joins.
	deriveCudyEvents("clients", map[string]interface{}{
		"clients": []interface{}{
			map[string]interface{}{"mac": "aa"},
			map[string]interface{}{"mac": "bb"},
		},
	})
	if events := cudyEventRing.since(0, "", 0); len(events) != 0 {
		t.Fatalf("reseed emitted events: %v", eventKinds(events))
	}
}

func TestClientSampleFromState(t *testing.T) {
	body := map[string]interface{}{
		"ts": float64(1700000000),
		"clients": []interface{}{
			map[string]interface{}{"mac": "aa", "hostname": "one", "up_kbps": float64(1), "down_kbps": float64(2), "imsi": "x"},
		},
	}
	sample, ok := clientSampleFromState(body)
	if !ok || sample.TS != 1700000000 || len(sample.Clients) != 1 {
		t.Fatalf("unexpected sample: %+v ok=%v", sample, ok)
	}
	if _, ok := sample.Clients[0]["imsi"]; ok {
		t.Fatal("unexpected field retained in client sample")
	}
	if sample.Clients[0]["down_kbps"] != float64(2) {
		t.Fatalf("rate lost: %v", sample.Clients[0])
	}
}

func TestClientHistoryRingBounds(t *testing.T) {
	ring := newClientHistory(2)
	ring.add(clientSample{TS: 1})
	ring.add(clientSample{TS: 2})
	ring.add(clientSample{TS: 3})
	got := ring.since(0)
	if len(got) != 2 || got[0].TS != 2 || got[1].TS != 3 {
		t.Fatalf("ring did not evict oldest: %+v", got)
	}
	if got := ring.since(3); len(got) != 1 || got[0].TS != 3 {
		t.Fatalf("since filter failed: %+v", got)
	}
}

func TestHandleCudyEventsEndpoint(t *testing.T) {
	resetCudyEvents()
	defer resetCudyEvents()

	handleCudyEvent("cudy/events/connectivity", []byte(`{"id":"a","ts":1700000000,"state":"up"}`))
	handleCudyEvent("cudy/events/public_ip", []byte(`{"id":"b","ts":1700000001,"current":"2.2.2.2"}`))

	rec := httptest.NewRecorder()
	handleCudyEvents(rec, httptest.NewRequest("GET", "/api/cudy/events?since=1&kind=public_ip", nil))
	if rec.Code != 200 {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Events []map[string]interface{} `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Events) != 1 || body.Events[0]["kind"] != "public_ip" {
		t.Fatalf("unexpected events: %v", body.Events)
	}
}

func TestHandleCudyClientsHistoryEndpoint(t *testing.T) {
	now := time.Now().Unix()
	cudyClientRing = newClientHistory(10)
	defer func() { cudyClientRing = newClientHistory(cudyClientMax) }()
	cudyClientRing.add(clientSample{TS: now - 10, Clients: []map[string]interface{}{{"mac": "aa"}}})
	cudyClientRing.add(clientSample{TS: now, Clients: []map[string]interface{}{{"mac": "bb"}}})

	rec := httptest.NewRecorder()
	handleCudyClientsHistory(rec, httptest.NewRequest("GET", fmt.Sprintf("/api/cudy/clients/history?since=%d", now-5), nil))
	var body struct {
		Samples []clientSample `json:"samples"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Samples) != 1 || body.Samples[0].TS != now {
		t.Fatalf("unexpected samples: %+v", body.Samples)
	}
}

func TestCudyBurnSummaryWithoutRedis(t *testing.T) {
	out := cudyBurnSummary(context.Background(), nil, 0, 100)
	if out["available"] != false {
		t.Fatalf("expected unavailable without redis: %v", out)
	}
}

func TestCudyDriftSummary(t *testing.T) {
	out := cudyDriftSummary(
		map[string]interface{}{"total_bytes": float64(1000)},
		map[string]interface{}{"total_used_bytes": float64(1001)},
	)
	if out["available"] != true || out["delta_bytes"] != float64(1) {
		t.Fatalf("unexpected drift: %v", out)
	}
	if out["pct"] != float64(0.1) {
		t.Fatalf("unexpected drift pct: %v", out["pct"])
	}
}

func TestHandleCudyUsageEnforcedFlag(t *testing.T) {
	resetCudyState()
	resetCudyEvents()
	defer resetCudyState()
	defer resetCudyEvents()

	handleCudyState("cudy/state/usage", []byte(`{"ts":1700000000,"total_bytes":1000}`))
	handleCudyState("cudy/state/usage_config", []byte(`{"ts":1700000000,"monthly_stats_enabled":false,"monthly_limit_bytes":0,"monthly_allowance_bytes":10737418240}`))

	rec := httptest.NewRecorder()
	handleCudyUsage(rec, httptest.NewRequest("GET", "/api/cudy/usage", nil))
	var body struct {
		Enforced bool                   `json:"enforced"`
		Drift    map[string]interface{} `json:"drift"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Enforced {
		t.Fatal("router monthly stats are disabled; enforced must be false")
	}

	handleCudyState("cudy/state/usage_config", []byte(`{"ts":1700000000,"monthly_stats_enabled":true,"monthly_limit_bytes":1000}`))
	rec = httptest.NewRecorder()
	handleCudyUsage(rec, httptest.NewRequest("GET", "/api/cudy/usage", nil))
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Enforced {
		t.Fatal("enabled monthly stats with a positive limit must be enforced")
	}
}

func TestParseAggMembersMode(t *testing.T) {
	a := agg{C: 2, S: 51, Mn: 25, Mx: 26, L: 26, Lt: 200}
	mb, _ := json.Marshal(a)
	items := []redis.Z{{Score: 150, Member: string(mb)}}

	cases := map[string]float64{"avg": 25.5, "last": 26, "max": 26, "min": 25}
	for mode, want := range cases {
		pts := parseAggMembersMode(items, mode)
		if len(pts) != 1 || pts[0].val != want {
			t.Fatalf("%s: got %+v, want %v", mode, pts, want)
		}
	}
}

func TestHandleHistoryRejectsInvalidAgg(t *testing.T) {
	req := httptest.NewRequest("GET", `/api/history?query={__name__="sensor_cudy_rsrp"}&start=1&end=2&agg=bogus`, nil)
	rec := httptest.NewRecorder()
	handleHistory(rec, req)
	if rec.Code != 400 {
		t.Fatalf("expected 400 for invalid agg, got %d: %s", rec.Code, rec.Body.String())
	}
}
