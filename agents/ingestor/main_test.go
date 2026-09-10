package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSourceTimestamp(t *testing.T) {
	lines, err := metricLines([]byte(`{"sensor":"dht11","ts":1000,"temp_c":20,"event_id":"a","untrusted":42}`), time.Unix(1010, 0))
	if err != nil || len(lines) != 1 || !strings.HasSuffix(lines[0], " 1000000") {
		t.Fatal(lines, err)
	}
}
func TestInvalidSensor(t *testing.T) {
	if _, err := metricLines([]byte(`{"sensor":"bad\"sensor","ts":1000,"temp_c":20}`), time.Unix(1010, 0)); err == nil {
		t.Fatal("accepted unknown sensor")
	}
}
func TestFailedDeliveryDoesNotAck(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer s.Close()
	old := vmAgentURL
	vmAgentURL = s.URL
	defer func() { vmAgentURL = old }()
	acked := false
	b := []delivery{{lines: []string{"x 1 1000"}, ack: func() { acked = true }}}
	if push(context.Background(), b) == nil || acked {
		t.Fatal("failed delivery was acknowledged")
	}
}
