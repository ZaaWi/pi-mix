package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Publisher struct {
	client  mqtt.Client
	topic   string
	actions *ActionMap
	bridge  string
}

type ActionMap struct {
	Codes map[string]Action `json:"codes"`
}

type Action struct {
	Type   string `json:"type"`
	Host   string `json:"host,omitempty"`
	Method string `json:"method,omitempty"`
	Params any    `json:"params,omitempty"`
	URL    string `json:"url,omitempty"`
}

func NewPublisher(cfg Config) *Publisher {
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MQTTBroker).
		SetClientID("ir-agent").
		SetKeepAlive(30 * time.Second).
		SetPingTimeout(10 * time.Second).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetAutoReconnect(true)

	opts.OnConnect = func(c mqtt.Client) {
		log.Printf("connected to MQTT broker: %s", cfg.MQTTBroker)
	}
	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		log.Printf("mqtt disconnected: %v", err)
	}

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.WaitTimeout(3 * time.Second) && token.Error() != nil {
		log.Printf("mqtt initial connect failed, retrying in background: %v", token.Error())
	}

	actions := loadActions(cfg.ActionFile)

	return &Publisher{
		client:  client,
		topic:   cfg.MQTTTopic,
		actions: actions,
		bridge:  cfg.BridgeHost + ":" + cfg.BridgePort,
	}
}

func (p *Publisher) Publish(frame Frame) {
	payload := map[string]any{
		"event":    "reading",
		"sensor":   "ir",
		"code":     frame.Hex,
		"protocol": "nec",
		"bits":     32,
	}

	data, _ := json.Marshal(payload)
	if p.client != nil && p.client.IsConnected() {
		p.client.Publish(p.topic, 1, false, data)
	}
	log.Printf("IR: %s", string(data))

	if p.actions != nil {
		if action, ok := p.actions.Codes[frame.Hex]; ok {
			p.dispatch(action, frame)
		}
	}
}

func (p *Publisher) dispatch(a Action, f Frame) {
	switch a.Type {
	case "rpc":
		p.callRPC(a)
	case "http":
		log.Printf("http action not implemented: %s", a.URL)
	default:
		log.Printf("unknown action type: %s", a.Type)
	}
}

func (p *Publisher) callRPC(a Action) {
	host := a.Host
	if host == "" {
		host = p.bridge
	}

	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  a.Method,
		"params":  a.Params,
	}
	if a.Params == nil {
		req["params"] = map[string]any{}
	}

	data, _ := json.Marshal(req)

	conn, err := net.DialTimeout("tcp", host, 5*time.Second)
	if err != nil {
		log.Printf("RPC dial %s: %v", host, err)
		return
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(append(data, '\n')); err != nil {
		log.Printf("RPC write %s: %v", host, err)
		return
	}

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		log.Printf("RPC read %s: %v", host, err)
		return
	}
	log.Printf("RPC -> %s %s <- %s", host, a.Method, strings.TrimSpace(string(buf[:n])))
}

func loadActions(path string) *ActionMap {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("read actions file: %v", err)
		}
		return nil
	}
	var m ActionMap
	if err := json.Unmarshal(data, &m); err != nil {
		log.Printf("parse actions file: %v", err)
		return nil
	}
	log.Printf("loaded %d action mappings", len(m.Codes))
	return &m
}

func (f Frame) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"code":     f.Hex,
		"protocol": "nec",
		"bits":     32,
	})
}

func (f Frame) String() string {
	return fmt.Sprintf("nec:%s", f.Hex)
}
