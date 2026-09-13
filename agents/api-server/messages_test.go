package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPostMessageValidation(t *testing.T) {
	historyCache = nil
	cases := []struct {
		name string
		body string
		want int
	}{
		{"empty body", "", 400},
		{"bad json", `{"title":`, 400},
		{"missing title", `{"message":"hi"}`, 400},
		{"missing message", `{"title":"hi"}`, 400},
		{"blank title", `{"title":"   ","message":"hi"}`, 400},
		{"negative ts", `{"title":"a","message":"b","timestamp":-1}`, 400},
	}
	for _, c := range cases {
		r := httptest.NewRequest("POST", "/api/messages", strings.NewReader(c.body))
		w := httptest.NewRecorder()
		handlePostMessage(w, r)
		if w.Code != c.want {
			t.Errorf("%s: got %d, want %d", c.name, w.Code, c.want)
		}
	}
}

func TestHandleMessagesMethodNotAllowed(t *testing.T) {
	for _, method := range []string{"PUT", "DELETE", "PATCH"} {
		r := httptest.NewRequest(method, "/api/messages", nil)
		w := httptest.NewRecorder()
		handleMessages(w, r)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: got %d, want %d", method, w.Code, http.StatusMethodNotAllowed)
		}
	}
}

// TestMessagesIntegration verifies round-trip storage and newest-first ordering
// against a real Redis (GitHub Actions redis service).
func TestMessagesIntegration(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping Redis integration")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	rdb.Del(ctx, messagesKey)
	defer rdb.Del(ctx, messagesKey)

	now := time.Now().Unix()
	if err := addMessage(ctx, rdb, message{Title: "old", Message: "first", Timestamp: now - 2*3600}); err != nil {
		t.Fatalf("addMessage: %v", err)
	}
	if err := addMessage(ctx, rdb, message{Title: "new", Message: "last", Desc: "d", Timestamp: now}); err != nil {
		t.Fatalf("addMessage: %v", err)
	}
	if err := addMessage(ctx, rdb, message{Title: "mid", Message: "second", Timestamp: now - 3600}); err != nil {
		t.Fatalf("addMessage: %v", err)
	}

	msgs, err := getMessages(ctx, rdb, 50)
	if err != nil {
		t.Fatalf("getMessages: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	// Newest first.
	if msgs[0].Title != "new" || msgs[1].Title != "mid" || msgs[2].Title != "old" {
		t.Fatalf("unexpected order: %+v", msgs)
	}
	if msgs[0].Desc != "d" {
		t.Fatalf("desc not round-tripped: %+v", msgs[0])
	}

	limited, err := getMessages(ctx, rdb, 2)
	if err != nil {
		t.Fatalf("getMessages limit: %v", err)
	}
	if len(limited) != 2 || limited[0].Title != "new" {
		t.Fatalf("limit failed: %+v", limited)
	}

	// A message whose timestamp falls outside the retention window is
	// dropped by the trim that runs on the next write.
	if err := addMessage(ctx, rdb, message{Title: "stale", Message: "s", Timestamp: now - messagesRetention - 1}); err != nil {
		t.Fatalf("addMessage stale: %v", err)
	}
	msgs, err = getMessages(ctx, rdb, 50)
	if err != nil {
		t.Fatalf("getMessages after trim: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("retention trim changed message count: got %d, want 3 (%+v)", len(msgs), msgs)
	}
	for _, m := range msgs {
		if m.Title == "stale" {
			t.Fatalf("retention trim did not drop stale message: %+v", msgs)
		}
	}
}

// TestPostMessageHandler covers the handler round-trip using the global cache.
func TestPostMessageHandler(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping Redis integration")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	rdb.Del(ctx, messagesKey)
	defer rdb.Del(ctx, messagesKey)
	prev := historyCache
	historyCache = rdb
	defer func() { historyCache = prev }()

	body := `{"title":"hello","message":"world","desc":"note","color":"#22c55e"}`
	r := httptest.NewRequest("POST", "/api/messages", strings.NewReader(body))
	w := httptest.NewRecorder()
	handlePostMessage(w, r)
	if w.Code != http.StatusCreated {
		t.Fatalf("POST: got %d, want %d", w.Code, http.StatusCreated)
	}

	r = httptest.NewRequest("GET", "/api/messages", nil)
	w = httptest.NewRecorder()
	handleGetMessages(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("GET: got %d, want %d", w.Code, http.StatusOK)
	}
	if !strings.Contains(w.Body.String(), `"title":"hello"`) {
		t.Fatalf("unexpected GET body: %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"color":"#22c55e"`) {
		t.Fatalf("color not round-tripped: %s", w.Body.String())
	}
}
