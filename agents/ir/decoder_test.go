package main

import (
	"testing"
	"time"
)

func syntheticNEC(value uint32) []Edge {
	var edges []Edge
	t := time.Duration(0)

	// Each segment records one edge at its start:
	//   LOW (carrier burst)  → FALLING edge (HIGH→LOW)
	//   HIGH (space)         → RISING edge  (LOW→HIGH)

	pulse := func(rising bool, dur time.Duration) {
		edges = append(edges, Edge{Timestamp: t, Rising: rising})
		t += dur
	}

	// Start: 9ms carrier burst (LOW) + 4.5ms space (HIGH)
	pulse(false, 9*time.Millisecond)      // FALLING: start of carrier
	pulse(true, 4500*time.Microsecond)    // RISING: start of space

	// 32 data bits
	for i := 0; i < 32; i++ {
		pulse(false, 560*time.Microsecond) // FALLING: start of bit carrier
		if value&(1<<i) != 0 {
			pulse(true, 1680*time.Microsecond) // RISING: "1" space
		} else {
			pulse(true, 560*time.Microsecond) // RISING: "0" space
		}
	}

	// Trailing burst (closes the last bit's space segment)
	pulse(false, 560*time.Microsecond)

	return edges
}

func TestDecodeButton1(t *testing.T) {
	val := uint32(0x9850060A)
	edges := syntheticNEC(val)
	frames, err := DecodeFrames(edges)
	if err != nil {
		t.Fatalf("DecodeFrames error: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	if frames[0].Value != val {
		t.Fatalf("expected 0x%08X, got 0x%08X", val, frames[0].Value)
	}
	if frames[0].Hex != "9850060A" {
		t.Fatalf("expected hex 9850060A, got %s", frames[0].Hex)
	}
}

func TestDecodeButton2(t *testing.T) {
	val := uint32(0x8890040A)
	edges := syntheticNEC(val)
	frames, err := DecodeFrames(edges)
	if err != nil {
		t.Fatalf("DecodeFrames error: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	if frames[0].Value != val {
		t.Fatalf("expected 0x%08X, got 0x%08X", val, frames[0].Value)
	}
}

func TestDecodeAllZeros(t *testing.T) {
	val := uint32(0x00000000)
	edges := syntheticNEC(val)
	frames, err := DecodeFrames(edges)
	if err != nil {
		t.Fatalf("DecodeFrames error: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	if frames[0].Value != 0 {
		t.Fatalf("expected 0, got 0x%08X", frames[0].Value)
	}
}

func TestDecodeAllOnes(t *testing.T) {
	val := uint32(0xFFFFFFFF)
	edges := syntheticNEC(val)
	frames, err := DecodeFrames(edges)
	if err != nil {
		t.Fatalf("DecodeFrames error: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	if frames[0].Value != 0xFFFFFFFF {
		t.Fatalf("expected 0xFFFFFFFF, got 0x%08X", frames[0].Value)
	}
}

func TestDecodeMultipleFrames(t *testing.T) {
	val1 := uint32(0x9850060A)
	val2 := uint32(0x8890040A)
	e1 := syntheticNEC(val1)
	e2 := syntheticNEC(val2)
	edges := append(e1, e2...)
	frames, err := DecodeFrames(edges)
	if err != nil {
		t.Fatalf("DecodeFrames error: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame (first only), got %d", len(frames))
	}
	if frames[0].Value != val1 {
		t.Fatalf("frame 0: expected 0x%08X, got 0x%08X", val1, frames[0].Value)
	}
}

func TestDecodeInsuficientEdges(t *testing.T) {
	edges := []Edge{{Timestamp: 0, Rising: true}}
	_, err := DecodeFrames(edges)
	if err != ErrInsufficientEdges {
		t.Fatalf("expected ErrInsufficientEdges, got %v", err)
	}
}

func TestDecodeNoStart(t *testing.T) {
	edges := []Edge{
		{Timestamp: 0, Rising: false},
		{Timestamp: 500 * time.Microsecond, Rising: true},
		{Timestamp: 1000 * time.Microsecond, Rising: false},
		{Timestamp: 1500 * time.Microsecond, Rising: true},
	}
	frames, err := DecodeFrames(edges)
	if err != nil {
		t.Fatalf("DecodeFrames error: %v", err)
	}
	if len(frames) != 0 {
		t.Fatalf("expected 0 frames for noise, got %d", len(frames))
	}
}

func TestDecodeNoiseBeforeStart(t *testing.T) {
	val := uint32(0x9850060A)
	signal := syntheticNEC(val)
	noise := []Edge{
		{Timestamp: 100 * time.Microsecond, Rising: false},
		{Timestamp: 200 * time.Microsecond, Rising: true},
		{Timestamp: 300 * time.Microsecond, Rising: false},
	}
	edges := append(noise, signal...)
	frames, err := DecodeFrames(edges)
	if err != nil {
		t.Fatalf("DecodeFrames error: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame despite noise, got %d", len(frames))
	}
	if frames[0].Value != val {
		t.Fatalf("expected 0x%08X, got 0x%08X", val, frames[0].Value)
	}
}

func TestDecodeJitter(t *testing.T) {
	val := uint32(0x9850060A)
	var edges []Edge
	cur := time.Duration(0)

	jitter := func(base time.Duration) time.Duration {
		pct := int64(base) * 15 / 100
		offset := pct * int64(int(base)%3-1) / 2
		return base + time.Duration(offset)
	}

	pulse := func(rising bool, dur time.Duration) {
		edges = append(edges, Edge{Timestamp: cur, Rising: rising})
		cur += dur
	}

	pulse(false, jitter(9*time.Millisecond))
	pulse(true, jitter(4500*time.Microsecond))

	for i := 0; i < 32; i++ {
		pulse(false, jitter(560*time.Microsecond))
		if val&(1<<i) != 0 {
			pulse(true, jitter(1680*time.Microsecond))
		} else {
			pulse(true, jitter(560*time.Microsecond))
		}
	}
	pulse(false, jitter(560*time.Microsecond))

	frames, err := DecodeFrames(edges)
	if err != nil && err != ErrNoStart {
		t.Fatalf("DecodeFrames error: %v", err)
	}
	if len(frames) == 0 {
		t.Fatal("expected at least 1 frame despite jitter")
	}
	if frames[0].Value != val {
		t.Fatalf("expected 0x%08X, got 0x%08X", val, frames[0].Value)
	}
}

func TestDecodeFrameFromPinctrlData(t *testing.T) {
	pinctrlData := []struct {
		level string
		durUs int
	}{
		{"hi", 906352},
		{"lo", 8982},
		{"hi", 4501},
		{"lo", 656},
		{"hi", 1647},
		{"lo", 656},
		{"hi", 545},
		{"lo", 630},
		{"hi", 568},
		{"lo", 656},
		{"hi", 1646},
		{"lo", 655},
		{"hi", 1647},
		{"lo", 626},
		{"hi", 573},
		{"lo", 656},
		{"hi", 544},
		{"lo", 628},
		{"hi", 550},
		{"lo", 679},
		{"hi", 544},
		{"lo", 630},
		{"hi", 1676},
		{"lo", 629},
		{"hi", 568},
		{"lo", 628},
		{"hi", 1672},
		{"lo", 656},
		{"hi", 546},
		{"lo", 629},
		{"hi", 572},
		{"lo", 626},
		{"hi", 577},
		{"lo", 628},
		{"hi", 568},
		{"lo", 628},
		{"hi", 574},
		{"lo", 627},
		{"hi", 575},
		{"lo", 652},
		{"hi", 546},
		{"lo", 628},
		{"hi", 574},
		{"lo", 628},
		{"hi", 575},
		{"lo", 626},
		{"hi", 1671},
		{"lo", 630},
		{"hi", 1672},
		{"lo", 628},
		{"hi", 573},
		{"lo", 630},
		{"hi", 572},
		{"lo", 626},
		{"hi", 575},
		{"lo", 627},
		{"hi", 575},
		{"lo", 624},
		{"hi", 576},
		{"lo", 621},
		{"hi", 1708},
		{"lo", 593},
		{"hi", 578},
		{"lo", 621},
		{"hi", 1707},
		{"lo", 597},
		{"hi", 602},
		{"lo", 598},
	}

	var edges []Edge
	elapsed := time.Duration(0)
	// pinctrlData[0] is idle time (initial stable state). The real signal
	// starts at pinctrlData[1]: each entry records one edge at its start
	// + the duration the signal stays in the new state.
	for _, p := range pinctrlData[1:] {
		edges = append(edges, Edge{Timestamp: elapsed, Rising: p.level == "hi"})
		elapsed += time.Duration(p.durUs) * time.Microsecond
	}

	frames, err := DecodeFrames(edges)
	if err != nil {
		t.Fatalf("DecodeFrames error: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame from pinctrl data, got %d", len(frames))
	}
	if frames[0].Value != 0x50600A19 {
		t.Fatalf("expected 0x50600A19 from pinctrl data, got 0x%08X (%s)", frames[0].Value, frames[0].Hex)
	}
}
