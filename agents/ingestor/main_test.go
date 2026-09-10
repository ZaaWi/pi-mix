package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCheckpointDurability(t *testing.T) {
	s, err := openSpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.lock.Close()
	acked := false
	p := pending{event{ID: "one", Sensor: "ldr", TS: 1000, Values: map[string]float64{"analog": 1}}, func() { acked = true }}
	if err = s.checkpoint([]pending{p}); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(s.dir, "batch-*.json"))
	if !acked || len(files) != 1 {
		t.Fatal(acked, files)
	}
	var b batch
	raw, _ := os.ReadFile(files[0])
	if json.Unmarshal(raw, &b) != nil || b.Events[0].ID != "one" {
		t.Fatal("checkpoint lost identity")
	}
}
func TestSpoolFailureDoesNotAck(t *testing.T) {
	s := &spool{dir: filepath.Join(t.TempDir(), "missing")}
	acked := false
	if s.checkpoint([]pending{{event{ID: "one"}, func() { acked = true }}}) == nil || acked {
		t.Fatal("unsafe ack")
	}
}
func TestSpoolSingleWriter(t *testing.T) {
	dir := t.TempDir()
	s, err := openSpool(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.lock.Close()
	if second, err := openSpool(dir); err == nil {
		second.lock.Close()
		t.Fatal("allowed two writers")
	}
}
func TestRejectInvalidEvents(t *testing.T) {
	for _, raw := range []string{`{"sensor":"../bad","ts":1000,"analog":1}`, `{"sensor":"ldr","ts":0,"analog":1}`, `{"sensor":"ldr","ts":9999999999,"analog":1}`} {
		if _, err := parseEvent([]byte(raw), time.Now()); err == nil {
			t.Fatal(raw)
		}
	}
}
func TestDatabaseReplayAndLateArrival(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set TEST_PG_DSN to a disposable database with schema/migration applied")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sensor := "test-" + newID()
	base := time.Now().Add(-2*time.Hour).Unix() / 3600 * 3600
	e := event{ID: newID(), Sensor: sensor, TS: base + 1, Values: map[string]float64{"analog": 10}}
	a := batch{ID: newID(), Events: []event{e}}
	for i := 0; i < 2; i++ {
		if err = applyBatch(ctx, tx, a); err != nil {
			t.Fatal(err)
		}
	}
	if err = applyBatch(ctx, tx, batch{ID: newID(), Events: []event{e}}); err != nil {
		t.Fatal(err)
	}
	late := event{ID: newID(), Sensor: sensor, TS: base, Values: map[string]float64{"analog": 30}}
	if err = applyBatch(ctx, tx, batch{ID: newID(), Events: []event{late}}); err != nil {
		t.Fatal(err)
	}
	var count int
	var avg, last float64
	if err = tx.QueryRow("SELECT count,avg,last FROM pi_sensor_data WHERE sensor=$1 AND interval='15m'", sensor).Scan(&count, &avg, &last); err != nil {
		t.Fatal(err)
	}
	if count != 2 || avg != 20 || last != 10 {
		t.Fatal(count, avg, last)
	}
	later := event{ID: newID(), Sensor: sensor, TS: base + 901, Values: map[string]float64{"analog": 50}}
	if err = applyBatch(ctx, tx, batch{ID: newID(), Events: []event{later}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(hourlySQL); err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRow("SELECT count,avg,last FROM pi_sensor_data WHERE sensor=$1 AND interval='1h'", sensor).Scan(&count, &avg, &last); err != nil {
		t.Fatal(err)
	}
	if count != 3 || avg != 30 || last != 50 {
		t.Fatal(count, avg, last)
	}
}
