package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	mqttBroker = env("MQTT_BROKER", "tcp://mosquitto.iot.svc.cluster.local:1883")
	mqttTopic  = env("MQTT_TOPIC", "pi/#")
	// VictoriaMetrics Prometheus import endpoint
	vmAgentURL = env("VMAGENT_URL", "http://vmagent.iot.svc.cluster.local:8429/api/v1/import/prometheus")
	batchSize  = 100
	flushIntv  = 10 * time.Second
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// MetricsBuffer batches strings in memory
type MetricsBuffer struct {
	lines []string
	mu    sync.Mutex
}

func (b *MetricsBuffer) Add(line string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lines = append(b.lines, line)
}

func (b *MetricsBuffer) Flush() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.lines) == 0 {
		return nil
	}
	lines := b.lines
	b.lines = make([]string, 0, batchSize)
	return lines
}

func main() {
	buffer := &MetricsBuffer{lines: make([]string, 0, batchSize)}

	opts := mqtt.NewClientOptions().AddBroker(mqttBroker).SetClientID("ingestor")
	opts.OnConnect = func(c mqtt.Client) {
		log.Printf("Connected to MQTT broker: %s", mqttBroker)
		if token := c.Subscribe(mqttTopic, 0, handleMessage(buffer)); token.Wait() && token.Error() != nil {
			log.Fatalf("Failed to subscribe: %v", token.Error())
		}
		log.Printf("Subscribed to topic: %s", mqttTopic)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error connecting to MQTT: %v", token.Error())
	}

	// Background ticker to flush metrics periodically to save SD card
	ticker := time.NewTicker(flushIntv)
	go func() {
		for range ticker.C {
			flushMetrics(buffer)
		}
	}()

	// Listen for OS signals to trigger a graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down, flushing remaining buffer...")
	client.Disconnect(250)
	flushMetrics(buffer) // final flush
	log.Println("Shutdown complete.")
}

func handleMessage(buffer *MetricsBuffer) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		var payload map[string]interface{}
		if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
			log.Printf("Failed to unmarshal payload: %v", err)
			return
		}

		sensorName := "unknown"
		if s, ok := payload["sensor"].(string); ok {
			sensorName = s
		} else {
			// Fallback: Parse from topic (e.g., pi/dht11)
			parts := strings.Split(msg.Topic(), "/")
			if len(parts) > 1 {
				sensorName = parts[1]
			}
		}

		for key, val := range payload {
			// Ignore structural fields, only grab telemetry
			if key == "event" || key == "sensor" {
				continue
			}
			if num, ok := val.(float64); ok {
				// Convert to Prometheus text format: sensor_temperature_c{sensor="dht11"} 27.8
				line := fmt.Sprintf("sensor_%s{sensor=\"%s\"} %f", key, sensorName, num)
				buffer.Add(line)
			}
		}
	}
}

func flushMetrics(buffer *MetricsBuffer) {
	lines := buffer.Flush()
	if len(lines) == 0 {
		return
	}

	data := strings.Join(lines, "\n") + "\n"
	resp, err := http.Post(vmAgentURL, "text/plain", bytes.NewBufferString(data))
	if err != nil {
		log.Printf("Failed to push metrics to vmagent: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		log.Printf("vmagent returned non-200 status: %s", resp.Status)
	} else {
		log.Printf("Successfully pushed %d metrics to vmagent", len(lines))
	}
}
