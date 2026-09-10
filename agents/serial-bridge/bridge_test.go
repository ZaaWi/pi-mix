package main

import (
	"encoding/json"
	"testing"
)

func TestControlAuthorization(t *testing.T) {
	t.Setenv("BRIDGE_CONTROL_TOKEN", "abc123")
	for _, raw := range []string{`{}`, `{"control_token":"wrong"}`, `{"control_token":42}`} {
		if authorized(json.RawMessage(raw)) {
			t.Fatal("accepted unauthorized request")
		}
	}
	if !authorized(json.RawMessage(`{"control_token":"abc123"}`)) {
		t.Fatal("rejected token")
	}
	t.Setenv("BRIDGE_CONTROL_TOKEN", "")
	if authorized(json.RawMessage(`{"control_token":""}`)) {
		t.Fatal("empty token enabled controls")
	}
}
