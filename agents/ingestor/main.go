package main

import (
	"context"
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

var vmAgentURL = env("VMAGENT_URL", "http://vmagent.iot.svc.cluster.local:8429/api/v1/import/prometheus")
var transport = &http.Client{Timeout: 8 * time.Second}

type delivery struct {
	lines []string
	ack   func()
}

func metricLines(raw []byte, now time.Time) ([]string, error) {
	if len(raw) > 4096 {
		return nil, fmt.Errorf("payload too large")
	}
	var p map[string]any
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	sensor, _ := p["sensor"].(string)
	allowed := map[string][]string{"dht11": {"temp_c", "humidity_pct"}, "ldr": {"analog"}, "scale": {"weight_kg"}, "ir": {"bits"}}
	keys, ok := allowed[sensor]
	if !ok {
		return nil, fmt.Errorf("unknown sensor")
	}
	ts := float64(now.Unix())
	if v, ok := p["ts"].(float64); ok {
		ts = v
	}
	if ts < float64(now.Add(-7*24*time.Hour).Unix()) || ts > float64(now.Add(5*time.Minute).Unix()) {
		return nil, fmt.Errorf("timestamp outside accepted range")
	}
	var lines []string
	for _, key := range keys {
		if v, ok := p[key].(float64); ok && !math.IsNaN(v) && !math.IsInf(v, 0) {
			lines = append(lines, fmt.Sprintf("sensor_%s{sensor=%q} %g %d", key, sensor, v, int64(ts*1000)))
		}
	}
	return lines, nil
}

func push(ctx context.Context, batch []delivery) error {
	var lines []string
	for _, d := range batch {
		lines = append(lines, d.lines...)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, vmAgentURL, strings.NewReader(strings.Join(lines, "\n")+"\n"))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := transport.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("vmagent returned %s", resp.Status)
	}
	return nil
}

// ACK only after vmagent accepts the entire batch; retries retain source times.
func consume(ctx context.Context, q <-chan delivery) {
	timer := time.NewTicker(10 * time.Second)
	defer timer.Stop()
	batch := make([]delivery, 0, 100)
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		for {
			if err := push(ctx, batch); err == nil {
				for _, d := range batch {
					d.ack()
				}
				batch = batch[:0]
				return true
			} else {
				log.Printf("history delivery retry: %v", err)
			}
			select {
			case <-ctx.Done():
				return false
			case <-time.After(3 * time.Second):
			}
		}
	}
	for {
		select {
		case d := <-q:
			batch = append(batch, d)
			if len(batch) >= 100 && !flush() {
				return
			}
		case <-timer.C:
			if !flush() {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	queue := make(chan delivery, 10000)
	opts := mqtt.NewClientOptions().AddBroker(env("MQTT_BROKER", "tcp://mosquitto.iot.svc.cluster.local:1883")).SetClientID(env("MQTT_CLIENT_ID", "pi-mix-history"))
	opts.SetCleanSession(false).SetAutoAckDisabled(true).SetConnectRetry(true).SetConnectTimeout(5 * time.Second).SetConnectRetryInterval(3 * time.Second)
	opts.SetUsername(os.Getenv("MQTT_USERNAME")).SetPassword(os.Getenv("MQTT_PASSWORD"))
	opts.OnConnect = func(c mqtt.Client) {
		t := c.Subscribe("pi/#", 1, func(_ mqtt.Client, m mqtt.Message) {
			lines, err := metricLines(m.Payload(), time.Now())
			if err != nil || len(lines) == 0 {
				m.Ack()
				return
			}
			select {
			case queue <- delivery{lines, m.Ack}:
			case <-ctx.Done():
			}
		})
		if !t.WaitTimeout(5*time.Second) || t.Error() != nil {
			log.Print("MQTT subscribe failed")
		}
	}
	client := mqtt.NewClient(opts)
	client.Connect()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !client.IsConnected() || len(queue) == cap(queue) {
			http.Error(w, "ingestion unavailable", 503)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("health server: %v", err)
			cancel()
		}
	}()
	consume(ctx, queue)
	client.Disconnect(250)
	shutdown, done := context.WithTimeout(context.Background(), 3*time.Second)
	defer done()
	server.Shutdown(shutdown)
}
