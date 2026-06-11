package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	mqttBroker = env("MQTT_BROKER", "tcp://mosquitto.iot.svc.cluster.local:1883")
	mqttTopic  = env("MQTT_TOPIC", "pi/#")
	vmURL      = env("VM_URL", "http://victoriametrics.iot.svc.cluster.local:8428")
	port       = env("PORT", "8080")
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// Global state cache (Device Twin)
var (
	stateMu sync.RWMutex
	state   = make(map[string]map[string]interface{})
)

// SSE Clients for live streaming
var (
	clientsMu sync.Mutex
	clients   = make(map[chan string]bool)
)

func main() {
	// Connect to MQTT purely to listen for live data
	opts := mqtt.NewClientOptions().AddBroker(mqttBroker).SetClientID("api-server")
	opts.OnConnect = func(c mqtt.Client) {
		log.Printf("Connected to MQTT broker: %s", mqttBroker)
		if token := c.Subscribe(mqttTopic, 0, handleMessage); token.Wait() && token.Error() != nil {
			log.Printf("Failed to subscribe: %v", token.Error())
		}
	}
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error connecting to MQTT: %v", token.Error())
	}

	// HTTP Routes
	http.HandleFunc("/api/state", handleState)
	http.HandleFunc("/api/stream", handleStream)
	http.HandleFunc("/api/history", handleHistory)

	handler := corsMiddleware(http.DefaultServeMux)

	log.Printf("API Server listening on :%s", port)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func handleMessage(client mqtt.Client, msg mqtt.Message) {
	var payload map[string]interface{}
	if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
		return
	}

	sensorName := "unknown"
	if s, ok := payload["sensor"].(string); ok {
		sensorName = s
	} else {
		parts := strings.Split(msg.Topic(), "/")
		if len(parts) > 1 {
			sensorName = parts[1]
		}
	}

	// 1. Update the local memory cache
	stateMu.Lock()
	state[sensorName] = payload
	stateMu.Unlock()

	// 2. Broadcast to all connected web dashboards instantly
	data, _ := json.Marshal(map[string]interface{}{
		"sensor": sensorName,
		"data":   payload,
	})

	clientsMu.Lock()
	for ch := range clients {
		select {
		case ch <- string(data):
		default:
		}
	}
	clientsMu.Unlock()
}

// Returns the last known state of all sensors immediately (solves cold-start)
func handleState(w http.ResponseWriter, r *http.Request) {
	stateMu.RLock()
	defer stateMu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}

// Server-Sent Events (SSE) stream for live updates
func handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan string, 10)
	clientsMu.Lock()
	clients[ch] = true
	clientsMu.Unlock()

	defer func() {
		clientsMu.Lock()
		delete(clients, ch)
		clientsMu.Unlock()
		close(ch)
	}()

	for {
		select {
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case <-r.Context().Done():
			return
		}
	}
}

// Proxies historical data requests to VictoriaMetrics, safely handles offline HDD
func handleHistory(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	step := r.URL.Query().Get("step")

	targetURL := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%s&end=%s&step=%s", vmURL, query, start, end, step)
	
	resp, err := http.Get(targetURL)
	if err != nil {
		// VictoriaMetrics is offline because HDD is unmounted!
		// Return a graceful empty dataset so the UI doesn't crash.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]},"note":"VictoriaMetrics is offline (HDD disconnected)"}`))
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
