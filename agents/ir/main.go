package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	cfg := LoadConfig()
	pub := NewPublisher(cfg)
	listener := NewListener(cfg)

	edgesCh, err := listener.Start()
	if err != nil {
		log.Fatalf("listener start: %v", err)
	}
	log.Printf("listening on %s:%d", cfg.GPIOChip, cfg.GPIOOffset)

	var pending []Edge
	var lastPublish time.Time

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	trim := func(n int) {
		if len(pending) > n {
			pending = pending[len(pending)-n:]
		}
	}

	for {
		select {
		case batch, ok := <-edgesCh:
			if !ok {
				return
			}
			pending = append(pending, batch...)
			trim(500)
			frames, err := DecodeFrames(pending)
			if err != nil && err != ErrNoStart {
				log.Printf("decode error: %v", err)
			}
			if len(frames) > 0 {
				f := frames[0]
				if f.Value&0x00FFFFFF != 0 {
					if time.Since(lastPublish) > 500*time.Millisecond {
						pub.Publish(f)
						lastPublish = time.Now()
					}
				}
			}
			if len(frames) > 0 {
				pending = pending[:0]
			}

		case <-sigCh:
			log.Println("shutting down")
			listener.Close()
			os.Exit(0)

		case <-time.After(5 * time.Second):
			trim(500)
		}
	}
}
