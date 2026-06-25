package main

import (
	"errors"
	"fmt"
	"time"
)

type Edge struct {
	Timestamp time.Duration
	Rising    bool
}

type Frame struct {
	Value uint32
	Hex   string
}

var (
	ErrNoStart       = errors.New("no valid NEC start pattern found")
	ErrShortFrame    = errors.New("frame ended before 32 bits")
	ErrInvalidPulse  = errors.New("pulse width out of NEC range")
	ErrInsufficientEdges = errors.New("need at least 4 edges for a valid frame")
)

const (
	necStartLowMin  = 7500 * time.Microsecond
	necStartLowMax  = 10500 * time.Microsecond
	necStartHighMin = 3500 * time.Microsecond
	necStartHighMax = 5500 * time.Microsecond

	necBitLowMin     = 300 * time.Microsecond
	necBitLowMax     = 900 * time.Microsecond
	necBitHighZero   = 1200 * time.Microsecond
	necBitHighMin    = 300 * time.Microsecond
	necBitHighMax    = 2000 * time.Microsecond
)

type segment struct {
	dur time.Duration
	low bool
}

func segmentsFromEdges(edges []Edge) []segment {
	segs := make([]segment, 0, len(edges)-1)
	for i := 1; i < len(edges); i++ {
		dur := edges[i].Timestamp - edges[i-1].Timestamp
		low := edges[i].Rising
		segs = append(segs, segment{dur: dur, low: low})
	}
	return segs
}

func isStartLow(s segment) bool {
	return s.low && s.dur >= necStartLowMin && s.dur <= necStartLowMax
}

func isStartHigh(s segment) bool {
	return !s.low && s.dur >= necStartHighMin && s.dur <= necStartHighMax
}

func isBitLow(s segment) bool {
	return s.low && s.dur >= necBitLowMin && s.dur <= necBitLowMax
}

func DecodeFrames(edges []Edge) ([]Frame, error) {
	if len(edges) < 4 {
		return nil, ErrInsufficientEdges
	}

	segs := segmentsFromEdges(edges)
	var frames []Frame

	i := 0
	for i < len(segs)-33 {
		if isStartLow(segs[i]) && isStartHigh(segs[i+1]) {
			bits, ok := decodeBits(segs[i+2:])
			if ok {
				val := bitsToUint32(bits)
				frames = append(frames, Frame{
					Value: val,
					Hex:   fmt.Sprintf("%08X", val),
				})
				i += 66
				continue
			}
		}
		i++
	}
	return frames, nil
}

func decodeBits(segs []segment) ([]int, bool) {
	bits := make([]int, 0, 32)
	for j := 0; j < len(segs)-1 && len(bits) < 32; j += 2 {
		if j+1 >= len(segs) {
			return nil, false
		}
		lo := segs[j]
		hi := segs[j+1]
		if !isBitLow(lo) {
			return nil, false
		}
		if hi.low {
			return nil, false
		}
		if hi.dur >= necBitHighZero {
			bits = append(bits, 1)
		} else {
			bits = append(bits, 0)
		}
	}
	if len(bits) != 32 {
		return nil, false
	}
	return bits, true
}

func bitsToUint32(bits []int) uint32 {
	var val uint32
	for i, b := range bits {
		if b != 0 {
			val |= 1 << i
		}
	}
	return val
}
