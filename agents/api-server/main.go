package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
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
	http.HandleFunc("/api/messages", handleMessages)
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

// History is served entirely from Redis, which is the primary history store
// (RAM only, no disk writes). The ingestor maintains three resolutions:
//
//	raw   individual samples,    retained 6h
//	15m   per-bucket aggregates, retained 30d
//	1h    per-hour aggregates,   retained 90d
//
// The API picks the finest resolution that covers the request width and falls
// back to a coarser one on a freshly-rebuilt Redis. PostgreSQL is never read:
// it stores the same aggregates only for durable, cold-boot backfill.
func handleHistory(w http.ResponseWriter, r *http.Request) {
	startN, endN, metric, err := parseHistoryArgs(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	sensor, metricName, ok := metricToSensor(metric)
	if !ok {
		http.Error(w, "invalid query", 400)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	pts, source, err := resolveHistory(ctx, historyCache, metricName, startN, endN)
	if err != nil {
		http.Error(w, "History storage is unavailable", 503)
		return
	}
	complete := len(pts) > 0 && pts[0].ts <= startN+historyCoverTol && pts[len(pts)-1].ts >= endN-historyTailTol
	if len(pts) == 0 {
		source = "redis"
	}
	writeHistory(w, metric, sensor, decimate(pts), source, complete)
}

const (
	historyCoverTol   = 60          // seconds; pamper sub-minute range starts
	historyTailTol    = 2 * 60 * 60 // seconds; newest closed bucket trails now
	maxHistoryPoints  = 3000
	rawRetentionSecs  = 6 * 3600
	d15mRetentionSecs = 30 * 24 * 3600
)

type point struct {
	ts  int64
	val float64
}

// pickSeries selects the finest resolution that can cover the whole window.
func pickSeries(widthSec int64) string {
	switch {
	case widthSec <= rawRetentionSecs:
		return "raw"
	case widthSec <= d15mRetentionSecs:
		return "15m"
	default:
		return "1h"
	}
}

func seriesKey(series, metricName string) string { return "pi-mix:" + series + ":sensor_" + metricName }

// resolveHistory queries the chosen resolution first, falling back to coarser
// ones when a fresh Redis has not yet backfilled that resolution.
func resolveHistory(ctx context.Context, rdb *redis.Client, metricName string, startN, endN int64) (pts []point, source string, err error) {
	if rdb == nil {
		return nil, "", fmt.Errorf("redis unavailable")
	}
	chosen := pickSeries(endN - startN)
	order := []string{chosen}
	for _, s := range []string{"1h", "15m", "raw"} {
		if s != chosen {
			order = append(order, s)
		}
	}
	fatal := false
	for _, series := range order {
		p, qerr := seriesPoints(ctx, rdb, series, metricName, startN, endN)
		if qerr != nil {
			log.Printf("history: redis %s: %v", series, qerr)
			fatal = true
			continue
		}
		if len(p) == 0 {
			continue
		}
		return p, series, nil
	}
	if fatal {
		return nil, "", fmt.Errorf("redis unavailable")
	}
	return nil, "", nil
}

func seriesPoints(ctx context.Context, rdb *redis.Client, series, metricName string, startN, endN int64) ([]point, error) {
	rangeSpec := &redis.ZRangeBy{Min: strconv.FormatInt(startN, 10), Max: strconv.FormatInt(endN, 10)}
	key := seriesKey(series, metricName)
	if series == "raw" {
		members, err := rdb.ZRangeByScore(ctx, key, rangeSpec).Result()
		return parseRedisMembers(members), err
	}
	items, err := rdb.ZRangeByScoreWithScores(ctx, key, rangeSpec).Result()
	return parseAggMembers(items), err
}

// agg mirrors the member JSON written by the ingestor into the 15m/1h ZSETs.
type agg struct {
	C  int64   `json:"c"`
	S  float64 `json:"s"`
	Mn float64 `json:"mn"`
	Mx float64 `json:"mx"`
	L  float64 `json:"l"`
	Lt int64   `json:"lt"`
}

func parseAggMembers(items []redis.Z) []point {
	pts := make([]point, 0, len(items))
	for _, z := range items {
		member, _ := z.Member.(string)
		var a agg
		if err := json.Unmarshal([]byte(member), &a); err != nil || a.C <= 0 {
			continue
		}
		val := a.S / float64(a.C)
		if math.IsNaN(val) || math.IsInf(val, 0) {
			continue
		}
		pts = append(pts, point{ts: int64(z.Score), val: val})
	}
	return pts
}

func parseHistoryArgs(r *http.Request) (startN, endN int64, metric string, err error) {
	query := r.URL.Query().Get("query")
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	startN, e1 := strconv.ParseInt(start, 10, 64)
	endN, e2 := strconv.ParseInt(end, 10, 64)
	if e1 != nil || e2 != nil || startN < 0 || endN < 0 || endN < startN || len(query) == 0 {
		return 0, 0, "", fmt.Errorf("invalid range")
	}
	// Parse metric name from {__name__="sensor_temp_c"}
	if strings.Contains(query, "__name__=\"") {
		parts := strings.Split(query, "__name__=\"")
		if len(parts) > 1 {
			metric = strings.Split(parts[1], "\"")[0]
		}
	}
	if metric == "" {
		return 0, 0, "", fmt.Errorf("invalid query")
	}
	return startN, endN, metric, nil
}

func metricToSensor(metric string) (sensor, name string, ok bool) {
	if !strings.HasPrefix(metric, "sensor_") {
		return "", "", false
	}
	m := strings.TrimPrefix(metric, "sensor_")
	known := map[string]string{"temp_c": "dht11", "humidity_pct": "dht11", "analog": "ldr", "weight_kg": "scale", "bits": "ir"}
	sensor, ok = known[m]
	if !ok {
		return "", "", false
	}
	return sensor, m, true
}

func parseRedisMembers(members []string) []point {
	points := make([]point, 0, len(members))
	for _, m := range members {
		parts := strings.SplitN(m, ":", 2)
		if len(parts) != 2 {
			continue
		}
		ts, e1 := strconv.ParseInt(parts[0], 10, 64)
		val, e2 := strconv.ParseFloat(parts[1], 64)
		if e1 != nil || e2 != nil || math.IsNaN(val) || math.IsInf(val, 0) {
			continue
		}
		points = append(points, point{ts: ts, val: val})
	}
	return points
}

func decimate(points []point) []point {
	if len(points) <= maxHistoryPoints {
		return points
	}
	step := (len(points) + maxHistoryPoints - 1) / maxHistoryPoints
	out := make([]point, 0, maxHistoryPoints+1)
	for i := 0; i < len(points); i += step {
		out = append(out, points[i])
	}
	if out[len(out)-1].ts != points[len(points)-1].ts {
		out = append(out, points[len(points)-1])
	}
	return out
}

func writeHistory(w http.ResponseWriter, metric, sensor string, points []point, source string, complete bool) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-History-Source", source)
	w.Header().Set("X-History-Complete", strconv.FormatBool(complete))
	json.NewEncoder(w).Encode(historyResponse(metric, sensor, points))
}

func historyResponse(metric, sensor string, points []point) map[string]any {
	values := make([][]any, 0, len(points))
	for _, p := range points {
		values = append(values, []any{p.ts, p.val})
	}
	return map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "matrix",
			"result": []any{
				map[string]any{
					"metric": map[string]any{"__name__": metric, "sensor": sensor},
					"values": values,
				},
			},
		},
	}
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

// Notifications/messages are stored in a ZSET (score = timestamp) so they
// work within the pi-mix Redis ACL, which only permits zadd/zrangebyscore/
// zremrangebyscore/expire on `pi-mix:*` keys. Members are JSON documents.
const (
	messagesKey       = "pi-mix:messages"
	messagesRetention = int64(30 * 24 * 3600) // seconds; older entries are trimmed
)

type message struct {
	Title     string `json:"title"`
	Message   string `json:"message"`
	Desc      string `json:"desc"`
	Timestamp int64  `json:"timestamp"`
}

func handleMessages(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleGetMessages(w, r)
	case http.MethodPost:
		handlePostMessage(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// getMessages returns stored messages, newest first, capped at limit.
func getMessages(ctx context.Context, rdb *redis.Client, limit int) ([]message, error) {
	if rdb == nil {
		return nil, fmt.Errorf("redis unavailable")
	}
	members, err := rdb.ZRangeByScore(ctx, messagesKey, &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
	if err != nil {
		return nil, err
	}
	msgs := make([]message, 0, len(members))
	for _, m := range members {
		var msg message
		if json.Unmarshal([]byte(m), &msg) != nil || msg.Timestamp <= 0 {
			continue
		}
		msgs = append(msgs, msg)
	}
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].Timestamp > msgs[j].Timestamp })
	if limit > 0 && len(msgs) > limit {
		msgs = msgs[:limit]
	}
	return msgs, nil
}

// addMessage stores a message and trims entries older than the retention window.
func addMessage(ctx context.Context, rdb *redis.Client, msg message) error {
	if rdb == nil {
		return fmt.Errorf("redis unavailable")
	}
	if msg.Timestamp <= 0 {
		return fmt.Errorf("invalid timestamp")
	}
	member, _ := json.Marshal(msg)
	trimBefore := strconv.FormatInt(time.Now().Unix()-messagesRetention, 10)
	_, err := rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZAdd(ctx, messagesKey, redis.Z{Score: float64(msg.Timestamp), Member: string(member)})
		p.ZRemRangeByScore(ctx, messagesKey, "-inf", trimBefore)
		return nil
	})
	return err
}

func handleGetMessages(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 100 {
		limit = 100
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	msgs, err := getMessages(ctx, historyCache, limit)
	if err != nil {
		http.Error(w, "History storage is unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"messages": msgs})
}

func handlePostMessage(w http.ResponseWriter, r *http.Request) {
	var msg message
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
	if err != nil {
		http.Error(w, "invalid body", 400)
		return
	}
	if err := json.Unmarshal(body, &msg); err != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	msg.Title = strings.TrimSpace(msg.Title)
	msg.Message = strings.TrimSpace(msg.Message)
	if msg.Title == "" || msg.Message == "" {
		http.Error(w, "title and message are required", 400)
		return
	}
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().Unix()
	}
	if msg.Timestamp < 0 {
		http.Error(w, "invalid timestamp", 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := addMessage(ctx, historyCache, msg); err != nil {
		http.Error(w, "History storage is unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{"message": msg})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
