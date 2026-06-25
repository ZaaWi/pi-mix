package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cfg := LoadConfig()
	pub := NewPublisher(cfg)
	listener := NewListener(cfg)

	edgesCh, err := listener.Start()
	if err != nil {
		log.Fatalf("listener start: %v", err)
	}
	log.Printf("listening on %s:%d", cfg.GPIOChip, cfg.GPIOOffset)

	var pending []Edge

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	flushPending := func() []Edge {
		if len(pending) > 1000 {
			pending = pending[len(pending)-500:]
		}
		return pending
	}

	for {
		select {
		case batch, ok := <-edgesCh:
			if !ok {
				return
			}
			pending = append(pending, batch...)
			batch = flushPending()
			frames, err := DecodeFrames(batch)
			if err != nil && err != ErrNoStart {
				log.Printf("decode error: %v", err)
			}
			for _, f := range frames {
				pub.Publish(f)
			}

		case <-sigCh:
			log.Println("shutting down")
			listener.Close()
			os.Exit(0)

		case <-time.After(5 * time.Second):
			_ = flushPending()
		}
	}
}
