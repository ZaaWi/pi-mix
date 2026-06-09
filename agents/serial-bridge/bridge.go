package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.bug.st/serial"
	"github.com/warthog618/gpiod"
)

var (
	serialPort  = env("BRIDGE_SERIAL_PORT", "/dev/ttyAMA0")
	bindAddr    = env("BRIDGE_BIND", "0.0.0.0")
	bindPort    = env("BRIDGE_PORT", "9600")
	baudRate    = envInt("BRIDGE_BAUD", 9600)
	readTimeout = envInt("BRIDGE_READ_TIMEOUT", 2)
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		fmt.Sscanf(v, "%d", &n)
		return n
	}
	return d
}

type Bridge struct {
	mu   sync.Mutex
	port io.ReadWriteCloser
}

func (b *Bridge) open() error {
	mode := serial.Mode{BaudRate: baudRate, DataBits: 8, Parity: serial.NoParity, StopBits: serial.OneStopBit}
	p, err := serial.Open(serialPort, &mode)
	if err != nil {
		return fmt.Errorf("serial open: %w", err)
	}

	sp, ok := p.(serial.Port)
	if ok {
		sp.SetDTR(false)
		sp.SetReadTimeout(time.Duration(readTimeout) * time.Second)
	}

	b.port = p
	log.Println("serial port opened")
	return nil
}

func (b *Bridge) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.port != nil {
		b.port.Close()
		b.port = nil
	}
}

func (b *Bridge) cmd(command string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.port == nil {
		return "", fmt.Errorf("port not open")
	}

	sp, ok := b.port.(serial.Port)
	if ok {
		sp.ResetInputBuffer()
	}

	if _, err := b.port.Write([]byte(command + "\n")); err != nil {
		return "", fmt.Errorf("write: %w", err)
	}

	return readBurst(b.port, time.Duration(readTimeout)*time.Second)
}

func readBurst(p io.ReadWriteCloser, timeout time.Duration) (string, error) {
	sp, ok := p.(serial.Port)

	if ok {
		sp.SetReadTimeout(timeout)
	}

	buf := make([]byte, 256)
	n, err := p.Read(buf)
	if err != nil {
		return "", fmt.Errorf("read: %w", err)
	}
	if n == 0 {
		return "", fmt.Errorf("timeout: no response")
	}

	var out bytes.Buffer
	out.Write(buf[:n])

	if ok {
		sp.SetReadTimeout(100 * time.Millisecond)
	}
	for {
		n, err := p.Read(buf)
		if err != nil || n == 0 {
			break
		}
		out.Write(buf[:n])
	}

	return strings.TrimSpace(out.String()), nil
}

func resetPulse() error {
	c, err := gpiod.NewChip("gpiochip0")
	if err != nil {
		return fmt.Errorf("open chip: %w", err)
	}
	defer c.Close()

	l, err := c.RequestLine(18, gpiod.AsOutput(1))
	if err != nil {
		return fmt.Errorf("request line: %w", err)
	}
	time.Sleep(100 * time.Millisecond)
	l.SetValue(0)
	time.Sleep(200 * time.Millisecond)
	l.Close()
	return nil
}

func (b *Bridge) flash(hexData []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.port != nil {
		b.port.Close()
		b.port = nil
	}

	tmpFile := "/tmp/firmware.hex"
	if err := os.WriteFile(tmpFile, hexData, 0644); err != nil {
		return fmt.Errorf("write hex: %w", err)
	}

	if err := resetPulse(); err != nil {
		return fmt.Errorf("reset pulse: %w", err)
	}
	time.Sleep(200 * time.Millisecond)

	cmd := exec.Command("avrdude",
		"-c", "arduino",
		"-p", "m328p",
		"-P", serialPort,
		"-b", "115200",
		"-D",
		"-U", "flash:w:"+tmpFile+":i",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("avrdude: %w: %s", err, out)
	}
	log.Printf("flash OK: %s", strings.TrimSpace(string(out)))

	time.Sleep(500 * time.Millisecond)
	return b.open()
}

type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type Response struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id"`
	Result  any    `json:"result,omitempty"`
	Error   *Error `json:"error,omitempty"`
}

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func respond(conn net.Conn, id any, result any, code int, msg string) {
	resp := Response{JSONRPC: "2.0", ID: id}
	if code != 0 {
		resp.Error = &Error{Code: code, Message: msg}
	} else {
		resp.Result = result
	}
	json.NewEncoder(conn).Encode(resp)
}

func handle(conn net.Conn, b *Bridge) {
	defer conn.Close()
	sc := bufio.NewScanner(conn)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var req Request
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			respond(conn, nil, nil, -32700, fmt.Sprintf("parse error: %v", err))
			continue
		}
		handleRPC(conn, &req, b)
	}
}

func handleRPC(conn net.Conn, req *Request, b *Bridge) {
	writeErr := func(code int, msg string) {
		respond(conn, req.ID, nil, code, msg)
	}

	switch req.Method {
	case "ping", "ping_dht11", "ping_ldr":
		r, err := b.cmd("PING")
		if err != nil {
			writeErr(-32002, err.Error())
			return
		}
		if r == "PONG" {
			respond(conn, req.ID, map[string]string{"status": "ok"}, 0, "")
		} else {
			writeErr(-32000, fmt.Sprintf("unexpected: %s", r))
		}

	case "read_dht11":
		r, err := b.cmd("DHT:READ")
		if err != nil {
			writeErr(-32002, err.Error())
			return
		}
		if strings.HasPrefix(r, "ERR:") {
			writeErr(-32000, r[4:])
			return
		}
		result := parseDHT(r)
		if result == nil {
			writeErr(-32000, fmt.Sprintf("parse error: %s", r))
		} else {
			respond(conn, req.ID, result, 0, "")
		}

	case "read_ldr":
		r, err := b.cmd("LDR:READ")
		if err != nil {
			writeErr(-32002, err.Error())
			return
		}
		if strings.HasPrefix(r, "ERR:") {
			writeErr(-32000, r[4:])
			return
		}
		var analog int
		if n, _ := fmt.Sscanf(r, "L:%d", &analog); n == 1 {
			respond(conn, req.ID, map[string]int{"analog": analog}, 0, "")
		} else {
			writeErr(-32000, fmt.Sprintf("parse error: %s", r))
		}

	case "read_all":
		r, err := b.cmd("READ")
		if err != nil {
			writeErr(-32002, err.Error())
			return
		}
		if strings.HasPrefix(r, "ERR:") {
			writeErr(-32000, r[4:])
			return
		}
		result := parseAll(r)
		if result == nil {
			writeErr(-32000, fmt.Sprintf("parse error: %s", r))
		} else {
			respond(conn, req.ID, result, 0, "")
		}

	case "flash_firmware":
		var params struct {
			Hex      string `json:"hex"`
			Encoding string `json:"encoding"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			writeErr(-32602, fmt.Sprintf("invalid params: %v", err))
			return
		}
		hexData := []byte(params.Hex)
		if params.Encoding == "base64" {
			var err error
			hexData, err = base64.StdEncoding.DecodeString(params.Hex)
			if err != nil {
				writeErr(-32602, fmt.Sprintf("base64 decode: %v", err))
				return
			}
		}
		if err := b.flash(hexData); err != nil {
			writeErr(-32000, fmt.Sprintf("flash failed: %v", err))
			return
		}
		respond(conn, req.ID, map[string]string{"status": "ok"}, 0, "")

	default:
		writeErr(-32601, fmt.Sprintf("method not found: %s", req.Method))
	}
}

func parseDHT(s string) map[string]float64 {
	var t, h float64
	if n, _ := fmt.Sscanf(s, "T:%f,H:%f", &t, &h); n == 2 {
		return map[string]float64{"temp_c": t, "humidity_pct": h}
	}
	return nil
}

func parseAll(s string) map[string]any {
	result := make(map[string]any)
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "T":
			var v float64
			if _, err := fmt.Sscanf(kv[1], "%f", &v); err == nil {
				result["temp_c"] = v
			}
		case "H":
			var v float64
			if _, err := fmt.Sscanf(kv[1], "%f", &v); err == nil {
				result["humidity_pct"] = v
			}
		case "L":
			var v int
			if _, err := fmt.Sscanf(kv[1], "%d", &v); err == nil {
				result["analog"] = v
			}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	b := &Bridge{}
	if err := b.open(); err != nil {
		log.Fatalf("open serial: %v", err)
	}
	defer b.close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down")
		b.close()
		os.Exit(0)
	}()

	addr := bindAddr + ":" + bindPort
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("listening on %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handle(conn, b)
	}
}
