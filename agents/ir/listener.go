package main

import (
	"log"
	"sync"
	"time"

	"github.com/warthog618/go-gpiocdev"
)

type Listener struct {
	chip   string
	offset int
	line   *gpiocdev.Line
	edges  []Edge
	mu     sync.Mutex
	ready  chan struct{}
	closed bool
}

func NewListener(cfg Config) *Listener {
	return &Listener{
		chip:   cfg.GPIOChip,
		offset: cfg.GPIOOffset,
		ready:  make(chan struct{}, 1),
	}
}

func (l *Listener) Start() (<-chan []Edge, error) {
	out := make(chan []Edge, 4)

	handler := func(evt gpiocdev.LineEvent) {
		l.mu.Lock()
		l.edges = append(l.edges, Edge{
			Timestamp: evt.Timestamp,
			Rising:    evt.Type == gpiocdev.LineEventRisingEdge,
		})
		l.mu.Unlock()

		select {
		case l.ready <- struct{}{}:
		default:
		}
	}

	line, err := gpiocdev.RequestLine(l.chip, l.offset,
		gpiocdev.WithBothEdges,
		gpiocdev.WithEventHandler(handler),
	)
	if err != nil {
		return nil, err
	}
	l.line = line

	go l.loop(out)

	return out, nil
}

func (l *Listener) loop(out chan<- []Edge) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	defer close(out)

	for {
		select {
		case <-l.ready:
		case <-ticker.C:
		}

		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return
		}
		if len(l.edges) < 4 {
			l.mu.Unlock()
			continue
		}
		batch := make([]Edge, len(l.edges))
		copy(batch, l.edges)
		l.edges = l.edges[:0]
		l.mu.Unlock()

		out <- batch
	}
}

func (l *Listener) Close() {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	if l.line != nil {
		if err := l.line.Close(); err != nil {
			log.Printf("close line: %v", err)
		}
	}
}
