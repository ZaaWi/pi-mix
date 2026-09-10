package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redis/go-redis/v9"
)

var (
	mqttBroker   = env("MQTT_BROKER", "tcp://mosquitto.iot.svc.cluster.local:1883")
	mqttTopic    = env("MQTT_TOPIC", "pi/#")
	vmURL        = env("VM_URL", "http://victoriametrics.iot.svc.cluster.local:8428")
	port         = env("PORT", "8080")
	backend      = &http.Client{Timeout: 8 * time.Second}
	liveClient   mqtt.Client
	historyCache *redis.Client
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
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		historyCache = redis.NewClient(&redis.Options{Addr: addr, Username: os.Getenv("REDIS_USERNAME"), Password: os.Getenv("REDIS_PASSWORD"), Protocol: 2, DialTimeout: 300 * time.Millisecond, ReadTimeout: 300 * time.Millisecond, WriteTimeout: 300 * time.Millisecond, PoolTimeout: 300 * time.Millisecond, MaxRetries: -1, PoolSize: 4, ContextTimeoutEnabled: true, DisableIdentity: true})
		defer historyCache.Close()
	}
	// Connect to MQTT purely to listen for live data
	opts := mqtt.NewClientOptions().AddBroker(mqttBroker).SetClientID(env("MQTT_CLIENT_ID", "pi-mix-api"))
	opts.SetConnectRetry(true).SetConnectTimeout(5 * time.Second).SetConnectRetryInterval(3 * time.Second)
	opts.SetUsername(os.Getenv("MQTT_USERNAME")).SetPassword(os.Getenv("MQTT_PASSWORD"))
	opts.OnConnect = func(c mqtt.Client) {
		log.Printf("Connected to MQTT broker: %s", mqttBroker)
		if token := c.Subscribe(mqttTopic, 0, handleMessage); token.Wait() && token.Error() != nil {
			log.Printf("Failed to subscribe: %v", token.Error())
		}
	}
	liveClient = mqtt.NewClient(opts)
	liveClient.Connect()

	// HTTP Routes
	http.HandleFunc("/api/state", handleState)
	http.HandleFunc("/api/stream", handleStream)
	http.HandleFunc("/api/history", handleHistory)
	http.HandleFunc("/api/status", handleStatus)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !liveClient.IsConnected() {
			http.Error(w, "mqtt unavailable", 503)
			return
		}
		w.Write([]byte("ok\n"))
	})

	handler := corsMiddleware(http.DefaultServeMux)

	log.Printf("API Server listening on :%s", port)
	server := &http.Server{Addr: ":" + port, Handler: handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 8192}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func handleMessage(client mqtt.Client, msg mqtt.Message) {
	if len(msg.Payload()) > 4096 {
		return
	}
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

	if sensorName != "dht11" && sensorName != "ldr" && sensorName != "scale" && sensorName != "ir" {
		return
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
	if len(clients) >= 64 {
		clientsMu.Unlock()
		http.Error(w, "too many streams", 503)
		return
	}
	clients[ch] = true
	clientsMu.Unlock()
	rc := http.NewResponseController(w)
	rc.SetWriteDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprint(w, ": connected\n\n")
	rc.Flush()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	defer func() {
		clientsMu.Lock()
		delete(clients, ch)
		clientsMu.Unlock()
		close(ch)
	}()

	for {
		select {
		case msg := <-ch:
			rc.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := fmt.Fprintf(w, "data: %s\n\n", msg); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case <-heartbeat.C:
			rc.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
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

	startN, e1 := strconv.ParseInt(start, 10, 64)
	endN, e2 := strconv.ParseInt(end, 10, 64)
	stepD, e3 := time.ParseDuration(step)
	if e1 != nil || e2 != nil || e3 != nil || startN < 0 || endN < 0 || stepD < time.Second || endN < startN || endN-startN > 366*86400 || float64(endN-startN)/stepD.Seconds() > 11000 || len(query) > 512 || len(query) == 0 {
		http.Error(w, "invalid or excessive history range", 400)
		return
	}
	targetURL := strings.TrimRight(vmURL, "/") + "/api/v1/query_range?" + url.Values{"query": {query}, "start": {start}, "end": {end}, "step": {step}}.Encode()
	key := fmt.Sprintf("pi-mix:history:%x", sha256.Sum256([]byte(targetURL)))
	if historyCache != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 350*time.Millisecond)
		cached, err := historyCache.Get(ctx, key).Bytes()
		cancel()
		if err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-History-Cache", "hit")
			w.Write(cached)
			return
		}
	}

	req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL, nil)
	resp, err := backend.Do(req)
	if err != nil {
		// Keep live data usable while the storage node is unavailable.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"error","error":"History storage is unavailable"}`))
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	if err != nil || len(body) > 8<<20 {
		http.Error(w, "history response unavailable or too large", http.StatusBadGateway)
		return
	}
	if historyCache != nil && resp.StatusCode == http.StatusOK && json.Valid(body) {
		ctx, cancel := context.WithTimeout(r.Context(), 350*time.Millisecond)
		historyCache.Set(ctx, key, body, 20*time.Second)
		cancel()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	stateMu.RLock()
	defer stateMu.RUnlock()
	ages := map[string]any{}
	for _, name := range []string{"dht11", "ldr", "scale", "ir"} {
		p := state[name]
		ts, _ := p["ts"].(float64)
		ages[name] = map[string]any{"last_seen": ts, "stale": ts == 0 || time.Now().Unix()-int64(ts) > 30}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"mqtt_connected": liveClient != nil && liveClient.IsConnected(), "sensors": ages})
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
