package main

import (
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
