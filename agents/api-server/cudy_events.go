package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// cudyEvent is one flattened cudy/events/<kind> payload. Events are published
// non-retained, so the API persists them in Redis and keeps a small in-memory
// ring as an immediate-read fallback after a restart.
type cudyEvent map[string]interface{}

func (e cudyEvent) id() string {
	s, _ := e["id"].(string)
	return s
}

func (e cudyEvent) kind() string {
	s, _ := e["kind"].(string)
	return s
}

func (e cudyEvent) ts() int64 {
	switch v := e["ts"].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case json.Number:
		n, _ := v.Int64()
		return n
	}
	return 0
}

func cudyEventString(e cudyEvent, key string) string {
	switch v := e[key].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	}
	return ""
}

// eventRing is a bounded, newest-last list of events.
type eventRing struct {
	mu     sync.Mutex
	max    int
	events []cudyEvent
	ids    map[string]bool
}

func newEventRing(max int) *eventRing {
	return &eventRing{max: max, ids: make(map[string]bool)}
}

// add stores an event unless its id was already seen. It returns false for a
// duplicate id, matching the Redis ZSET's identical-member deduplication.
func (r *eventRing) add(ev cudyEvent) bool {
	id := ev.id()
	r.mu.Lock()
	defer r.mu.Unlock()
	if id != "" && r.ids[id] {
		return false
	}
	r.events = append(r.events, ev)
	if id != "" {
		r.ids[id] = true
	}
	for len(r.events) > r.max {
		dropped := r.events[0]
		r.events = r.events[1:]
		if droppedID := dropped.id(); droppedID != "" {
			delete(r.ids, droppedID)
		}
	}
	return true
}

// since returns events at or after since, newest first, optionally filtered by
// kind. limit <= 0 means no limit.
func (r *eventRing) since(since int64, kind string, limit int) []cudyEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]cudyEvent, 0, len(r.events))
	for i := len(r.events) - 1; i >= 0; i-- {
		ev := r.events[i]
		if ev.ts() < since {
			continue
		}
		if kind != "" && ev.kind() != kind {
			continue
		}
		out = append(out, ev)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

var cudyEventRing = newEventRing(500)

// cudyEventDedup suppresses the same transition arriving from both the
// collector's event topic and the API's state-diff derivation. QoS0 events can
// be missed, so deriving from retained state is the safety net.
var cudyEventDedup = struct {
	mu   sync.Mutex
	last map[string]int64
}{last: make(map[string]int64)}

const cudyEventDedupWindow = int64(120)

func cudyEventSignature(ev cudyEvent) string {
	kind := ev.kind()
	switch kind {
	case "connectivity":
		return kind + "|" + cudyEventString(ev, "state")
	case "public_ip":
		return kind + "|" + cudyEventString(ev, "current")
	case "cell_change":
		return kind + "|" + cudyEventString(ev, "cell_id") + "|" + cudyEventString(ev, "pcid")
	case "ca_change":
		return kind + "|" + cudyEventString(ev, "ca_count") + "|" + cudyEventString(ev, "scc_count")
	case "client_join", "client_leave":
		return kind + "|" + cudyEventString(ev, "mac")
	}
	if id := ev.id(); id != "" {
		return kind + "|id:" + id
	}
	return kind + "|" + strconv.FormatInt(ev.ts(), 10)
}

func cudyEventDuplicate(ev cudyEvent, now int64) bool {
	sig := cudyEventSignature(ev)
	cudyEventDedup.mu.Lock()
	defer cudyEventDedup.mu.Unlock()
	if ts, ok := cudyEventDedup.last[sig]; ok && now-ts < cudyEventDedupWindow {
		return true
	}
	cudyEventDedup.last[sig] = now
	if len(cudyEventDedup.last) > 2000 {
		for k, ts := range cudyEventDedup.last {
			if now-ts > 3600 {
				delete(cudyEventDedup.last, k)
			}
		}
	}
	return false
}

// handleCudyEvent ingests one cudy/events/<kind> message.
func handleCudyEvent(topic string, payload []byte) {
	kind := strings.TrimPrefix(topic, "cudy/events/")
	if kind == "" || strings.Contains(kind, "/") {
		return
	}
	var ev cudyEvent
	if err := json.Unmarshal(payload, &ev); err != nil || ev == nil {
		return
	}
	if ev.kind() == "" {
		ev["kind"] = kind
	}
	emitCudyEvent(ev)
}

// emitCudyEvent normalizes, deduplicates, stores and broadcasts one event.
func emitCudyEvent(ev cudyEvent) {
	if ev == nil || ev.kind() == "" {
		return
	}
	now := time.Now().Unix()
	if ev.ts() == 0 {
		ev["ts"] = float64(now)
	}
	if ev.id() == "" {
		ev["id"] = fmt.Sprintf("%s-%d", ev.kind(), ev.ts())
	}
	for _, field := range cudySensitiveFields {
		delete(ev, field)
	}
	if cudyEventDuplicate(ev, now) {
		return
	}
	cudyEventRing.add(ev)
	persistCudyEvent(ev)
	broadcast(map[string]interface{}{"sensor": "cudy_event", "data": ev})
}

// persistCudyEvent writes the event to the Redis ZSET and trims the retention
// horizon. Plain pipeline only: the pi-mix ACL grants no MULTI/EXEC.
func persistCudyEvent(ev cudyEvent) {
	if historyCache == nil {
		return
	}
	member, err := json.Marshal(ev)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trimBefore := strconv.FormatInt(time.Now().Unix()-cudyEventsRetention, 10)
	pipe := historyCache.Pipeline()
	pipe.ZAdd(ctx, cudyEventsKey, redis.Z{Score: float64(ev.ts()), Member: string(member)})
	pipe.ZRemRangeByScore(ctx, cudyEventsKey, "-inf", trimBefore)
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("cudy event persist failed: %v", err)
	}
}

// getCudyEvents merges Redis (durable) with the in-process ring (recent). The
// ring covers the window between a restart and the first Redis read.
func getCudyEvents(ctx context.Context, rdb *redis.Client, since int64, kind string, limit int) ([]cudyEvent, error) {
	merged := make(map[string]cudyEvent)
	order := make([]cudyEvent, 0, 256)

	appendEvent := func(ev cudyEvent) {
		if ev == nil || ev.ts() < since {
			return
		}
		if kind != "" && ev.kind() != kind {
			return
		}
		id := ev.id()
		if id != "" {
			if _, ok := merged[id]; ok {
				return
			}
			merged[id] = ev
		}
		order = append(order, ev)
	}

	var redisErr error
	if rdb != nil {
		members, err := rdb.ZRangeByScore(ctx, cudyEventsKey, &redis.ZRangeBy{
			Min: strconv.FormatInt(since, 10),
			Max: "+inf",
		}).Result()
		if err != nil {
			redisErr = err
		} else {
			for _, m := range members {
				var ev cudyEvent
				if json.Unmarshal([]byte(m), &ev) == nil {
					appendEvent(ev)
				}
			}
		}
	}
	for _, ev := range cudyEventRing.since(since, kind, 0) {
		appendEvent(ev)
	}

	sort.SliceStable(order, func(i, j int) bool { return order[i].ts() > order[j].ts() })
	if limit > 0 && len(order) > limit {
		order = order[:limit]
	}
	if len(order) == 0 && redisErr != nil {
		return nil, redisErr
	}
	return order, nil
}

func handleCudyEvents(w http.ResponseWriter, r *http.Request) {
	now := time.Now().Unix()
	since := now - 7*24*3600
	if v := r.URL.Query().Get("since"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 && n <= now {
			since = n
		}
	}
	limit := 200
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 1000 {
		limit = 1000
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	events, err := getCudyEvents(ctx, historyCache, since, r.URL.Query().Get("kind"), limit)
	if err != nil {
		http.Error(w, "event storage is unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"events": events})
}

func handleCudyClientsHistory(w http.ResponseWriter, r *http.Request) {
	now := time.Now().Unix()
	since := now - cudyClientWindow
	if v := r.URL.Query().Get("since"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 && n <= now {
			since = n
		}
	}
	if min := now - cudyClientWindow; since < min {
		since = min
	}
	samples := cudyClientRing.since(since)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"window_seconds": cudyClientWindow,
		"samples":        samples,
	})
}

// clientSampleFromState keeps only the fields the dashboard charts need. The
// collector caps its own payload at 4096 bytes; this is a second guard.
func clientSampleFromState(body map[string]interface{}) (clientSample, bool) {
	ts := int64(0)
	if v, ok := body["ts"].(float64); ok {
		ts = int64(v)
	}
	if ts == 0 {
		ts = time.Now().Unix()
	}
	raw, ok := body["clients"].([]interface{})
	if !ok {
		return clientSample{}, false
	}
	sample := clientSample{TS: ts, Clients: make([]map[string]interface{}, 0, len(raw))}
	fields := []string{"hostname", "conn_type", "ip", "mac", "up_kbps", "down_kbps", "signal", "online_seconds", "internet"}
	for _, item := range raw {
		src, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		dst := make(map[string]interface{}, len(fields))
		for _, f := range fields {
			if v, ok := src[f]; ok {
				dst[f] = v
			}
		}
		if _, ok := dst["mac"]; !ok {
			if _, ok := dst["hostname"]; !ok {
				continue
			}
		}
		sample.Clients = append(sample.Clients, dst)
		if len(sample.Clients) >= 64 {
			break
		}
	}
	return sample, true
}

// derivedCudy tracks the previous retained-state values used to synthesize
// events when the non-retained collector event was missed.
type derivedCudy struct {
	connected    bool
	hasConnected bool
	publicIP     string
	cellID       string
	pcid         interface{}
	band         interface{}
	caCount      interface{}
	sccCount     interface{}
	clientsInit  bool
	clients      map[string]bool
}

var (
	cudyDerivedMu sync.Mutex
	cudyDerived   = derivedCudy{clients: make(map[string]bool)}
)

func resetCudyDerived() {
	cudyDerivedMu.Lock()
	cudyDerived = derivedCudy{clients: make(map[string]bool)}
	cudyDerivedMu.Unlock()
}

func upDown(v bool) string {
	if v {
		return "up"
	}
	return "down"
}

// deriveCudyEvents compares one retained state snapshot with the previous one
// and emits transitions. The first snapshot only seeds the baseline.
func deriveCudyEvents(area string, body map[string]interface{}) {
	cudyDerivedMu.Lock()
	defer cudyDerivedMu.Unlock()
	now := time.Now().Unix()

	switch area {
	case "cellular":
		if connected, ok := body["connected"].(bool); ok {
			if cudyDerived.hasConnected && cudyDerived.connected != connected {
				emitCudyEvent(cudyEvent{
					"kind":      "connectivity",
					"ts":        float64(now),
					"state":     upDown(connected),
					"previous":  upDown(cudyDerived.connected),
					"public_ip": cudyEventString(cudyEvent(body), "public_ip"),
					"cell_id":   cudyEventString(cudyEvent(body), "cell_id"),
					"derived":   true,
				})
			}
			cudyDerived.connected = connected
			cudyDerived.hasConnected = true
		}

		if ip := cudyEventString(cudyEvent(body), "public_ip"); ip != "" {
			if cudyDerived.publicIP != "" && ip != cudyDerived.publicIP {
				emitCudyEvent(cudyEvent{
					"kind":     "public_ip",
					"ts":       float64(now),
					"previous": cudyDerived.publicIP,
					"current":  ip,
					"derived":  true,
				})
			}
			cudyDerived.publicIP = ip
		}

		cellID := cudyEventString(cudyEvent(body), "cell_id")
		if cellID != "" {
			pcid, hasPCID := body["pcid"]
			band, hasBand := body["band"]
			if cudyDerived.cellID != "" && (cellID != cudyDerived.cellID || pcid != cudyDerived.pcid || band != cudyDerived.band) {
				emitCudyEvent(cudyEvent{
					"kind":             "cell_change",
					"ts":               float64(now),
					"cell_id":          cellID,
					"pcid":             pcid,
					"band":             band,
					"previous_cell_id": cudyDerived.cellID,
					"previous_pcid":    cudyDerived.pcid,
					"previous_band":    cudyDerived.band,
					"derived":          true,
				})
			}
			cudyDerived.cellID = cellID
			if hasPCID {
				cudyDerived.pcid = pcid
			}
			if hasBand {
				cudyDerived.band = band
			}
		}

		caCount, hasCA := body["ca_count"]
		sccCount, hasSCC := body["scc_count"]
		if hasCA {
			if cudyDerived.caCount != nil && caCount != cudyDerived.caCount {
				emitCudyEvent(cudyEvent{
					"kind":               "ca_change",
					"ts":                 float64(now),
					"ca_count":           caCount,
					"scc_count":          sccCount,
					"pcc":                body["pcc"],
					"scc_list":           body["scc_list"],
					"previous_ca_count":  cudyDerived.caCount,
					"previous_scc_count": cudyDerived.sccCount,
					"derived":            true,
				})
			}
			cudyDerived.caCount = caCount
			if hasSCC {
				cudyDerived.sccCount = sccCount
			}
		}

	case "clients":
		// A capped payload drops clients from the tail, so diffing it would
		// emit false leave/join events. Wait for a complete snapshot and
		// reseed; the collector's own event topic still covers transitions.
		if truncated, _ := body["truncated"].(bool); truncated {
			cudyDerived.clientsInit = false
			return
		}
		raw, ok := body["clients"].([]interface{})
		if !ok {
			return
		}
		current := make(map[string]bool)
		names := make(map[string]string)
		for _, item := range raw {
			m, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			mac, _ := m["mac"].(string)
			if mac == "" {
				continue
			}
			current[mac] = true
			if name, _ := m["hostname"].(string); name != "" {
				names[mac] = name
			}
		}
		if !cudyDerived.clientsInit {
			cudyDerived.clients = current
			cudyDerived.clientsInit = true
			return
		}
		for mac := range current {
			if !cudyDerived.clients[mac] {
				emitCudyEvent(cudyEvent{
					"kind":     "client_join",
					"ts":       float64(now),
					"mac":      mac,
					"hostname": names[mac],
					"derived":  true,
				})
			}
		}
		for mac := range cudyDerived.clients {
			if !current[mac] {
				emitCudyEvent(cudyEvent{
					"kind":    "client_leave",
					"ts":      float64(now),
					"mac":     mac,
					"derived": true,
				})
			}
		}
		cudyDerived.clients = current
	}
}

// cudyProjection turns a byte delta over span seconds into a per-day rate and
// a 30-day projection. It rejects resets and zero-length windows.
func cudyProjection(delta float64, span int64) (perDay, projected30d float64, ok bool) {
	if span <= 0 || delta < 0 {
		return 0, 0, false
	}
	perDay = delta / float64(span) * 86400
	return perDay, perDay * 30, true
}

// cudyBurnSummary derives a byte/day rate from the counter history. The router
// has no calendar-scoped counter, so this is based on observed total_bytes
// deltas over the requested window.
func cudyBurnSummary(ctx context.Context, rdb *redis.Client, start, end int64) map[string]any {
	out := map[string]any{
		"available":      false,
		"window_seconds": end - start,
		"start":          start,
		"end":            end,
	}
	if rdb == nil {
		return out
	}
	// Counters must use the bucket's last value; averaging a cumulative counter
	// would understate the delta between buckets.
	points, source, err := resolveHistoryMode(ctx, rdb, "cudy_total_bytes", start, end, "last")
	if err != nil || len(points) < 2 {
		return out
	}
	first, last := points[0], points[len(points)-1]
	delta := last.val - first.val
	span := last.ts - first.ts
	out["available"] = true
	out["source"] = source
	out["samples"] = len(points)
	out["start_ts"] = first.ts
	out["end_ts"] = last.ts
	out["start_bytes"] = first.val
	out["end_bytes"] = last.val
	out["delta_bytes"] = delta
	out["reset"] = delta < 0
	if perDay, projected, ok := cudyProjection(delta, span); ok {
		out["bytes_per_day"] = perDay
		out["projected_30d_bytes"] = projected
	}
	return out
}

func cudyDriftSummary(usage, config map[string]interface{}) map[string]any {
	out := map[string]any{"available": false}
	telemetry, ok := usage["total_bytes"].(float64)
	if !ok {
		return out
	}
	router, ok := config["total_used_bytes"].(float64)
	if !ok {
		return out
	}
	delta := router - telemetry
	out["available"] = true
	out["router_total_bytes"] = router
	out["telemetry_total_bytes"] = telemetry
	out["delta_bytes"] = delta
	if telemetry != 0 {
		out["pct"] = delta / telemetry * 100
	}
	return out
}

func handleCudyUsage(w http.ResponseWriter, r *http.Request) {
	days := 7.0
	if v := r.URL.Query().Get("days"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 && f <= 90 {
			days = f
		}
	}
	now := time.Now().Unix()
	start := now - int64(days*86400)

	cudyMu.RLock()
	usage := cudyAreas["usage"]
	config := cudyAreas["usage_config"]
	cudyMu.RUnlock()

	enforced := false
	if enabled, _ := config["monthly_stats_enabled"].(bool); enabled {
		if limit, _ := config["monthly_limit_bytes"].(float64); limit > 0 {
			enforced = true
		}
	}

	cycleStart := int64(0)
	if v := r.URL.Query().Get("since"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 && n <= now {
			cycleStart = n
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	response := map[string]any{
		"ts":       now,
		"usage":    usage,
		"config":   config,
		"enforced": enforced,
		"burn":     cudyBurnSummary(ctx, historyCache, start, now),
		"drift":    cudyDriftSummary(usage, config),
	}
	if cycleStart > 0 {
		response["cycle"] = cudyBurnSummary(ctx, historyCache, cycleStart, now)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
