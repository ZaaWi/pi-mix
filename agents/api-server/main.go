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
	
	startN, e1 := strconv.ParseInt(start, 10, 64)
	endN, e2 := strconv.ParseInt(end, 10, 64)
	
	if e1 != nil || e2 != nil || startN < 0 || endN < 0 || endN < startN || len(query) == 0 {
		http.Error(w, "invalid range", 400)
		return
	}
	
	// Parse metric name from {__name__="sensor_temp_c"}
	metric := ""
	if strings.Contains(query, "__name__=\"") {
		parts := strings.Split(query, "__name__=\"")
		if len(parts) > 1 {
			metric = strings.Split(parts[1], "\"")[0]
		}
	}
	if metric == "" {
		http.Error(w, "invalid query", 400)
		return
	}
	
	if historyCache == nil {
		http.Error(w, "History storage is unavailable", 503)
		return
	}

	redisKey := fmt.Sprintf("pi-mix:raw:%s", metric)
	
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	
	// Fetch from Redis ZSET
	results, err := historyCache.ZRangeByScore(ctx, redisKey, &redis.ZRangeBy{
		Min: strconv.FormatInt(startN, 10),
		Max: strconv.FormatInt(endN, 10),
	}).Result()
	
	if err != nil {
		http.Error(w, "History fetch failed", 500)
		return
	}
	
	// Format to match Prometheus API response
	values := make([][]interface{}, 0, len(results))
	for _, member := range results {
		parts := strings.SplitN(member, ":", 2)
		if len(parts) == 2 {
			ts, _ := strconv.ParseInt(parts[0], 10, 64)
			values = append(values, []interface{}{ts, parts[1]})
		}
	}
	
	// Determine sensor from metric for the label
	sensorLabel := "unknown"
	if strings.HasPrefix(metric, "sensor_") {
		m := strings.TrimPrefix(metric, "sensor_")
		if m == "temp_c" || m == "humidity_pct" {
			sensorLabel = "dht11"
		} else if m == "analog" {
			sensorLabel = "ldr"
		} else if m == "weight_kg" {
			sensorLabel = "scale"
		} else if m == "bits" {
			sensorLabel = "ir"
		}
	}

	response := map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": "matrix",
			"result": []interface{}{
				map[string]interface{}{
					"metric": map[string]interface{}{
						"__name__": metric,
						"sensor":   sensorLabel,
					},
					"values": values,
				},
			},
		},
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-History-Source", "redis")
	json.NewEncoder(w).Encode(response)
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
