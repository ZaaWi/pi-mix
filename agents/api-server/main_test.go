package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestHistoryEscaping(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("query") != "sensor_temp_c + 1" {
			t.Errorf("query corrupted: %s", r.URL.RawQuery)
		}
		w.Write([]byte(`{"status":"success"}`))
	}))
	defer s.Close()
	old := vmURL
	vmURL = s.URL
	defer func() { vmURL = old }()
	q := url.Values{"query": {"sensor_temp_c + 1"}, "start": {"100"}, "end": {"200"}, "step": {"1m"}}
	w := httptest.NewRecorder()
	handleHistory(w, httptest.NewRequest("GET", "/api/history?"+q.Encode(), nil))
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestHistoryUnavailable(t *testing.T) {
	old := vmURL
	vmURL = "http://127.0.0.1:1"
	defer func() { vmURL = old }()
	w := httptest.NewRecorder()
	handleHistory(w, httptest.NewRequest("GET", "/api/history?query=sensor_temp_c&start=100&end=200&step=1m", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}
func TestHistoryExcessiveRange(t *testing.T) {
	w := httptest.NewRecorder()
	handleHistory(w, httptest.NewRequest("GET", "/api/history?query=x&start=0&end=999999999&step=1s", nil))
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
}
