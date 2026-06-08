#!/usr/bin/env python3
"""Serial bridge — owns /dev/ttyAMA0, exposes JSON-RPC over TCP.

Uses a persistent bash subprocess with exec 3<> to keep the serial port
open continuously. Prevents Arduino reset on each command by never closing
the port between read/write cycles.
"""

import json
import os
import select
import socket
import subprocess
import threading
import time

SERIAL_PORT = os.getenv("BRIDGE_SERIAL_PORT", "/dev/ttyAMA0")
BAUD_RATE = os.getenv("BRIDGE_BAUD", "9600")
BIND_ADDR = os.getenv("BRIDGE_BIND", "0.0.0.0")
BIND_PORT = int(os.getenv("BRIDGE_PORT", "9600"))
READ_TIMEOUT = float(os.getenv("BRIDGE_READ_TIMEOUT", "2"))

serial_lock = threading.Lock()
ser_proc = None


def serial_start():
    global ser_proc
    script = (
        f"exec 3<>{SERIAL_PORT}; "
        f"stty -F {SERIAL_PORT} raw -echo {BAUD_RATE} 2>/dev/null; "
        f"sleep 2; "
        f"while read -t 60 cmd; do "
        f"  printf '%s\\n' \"$cmd\" >&3; "
        f"  read -t {READ_TIMEOUT} line <&3; "
        f"  printf '%s\\n' \"$line\"; "
        f"done"
    )
    ser_proc = subprocess.Popen(
        ["bash", "-c", script],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    log("serial process started, waiting for boot (2s)...")
    time.sleep(3)


def serial_cmd(command):
    global ser_proc
    if ser_proc is None or ser_proc.poll() is not None:
        serial_start()
    if ser_proc is None:
        return None

    with serial_lock:
        try:
            ser_proc.stdin.write((command + "\n").encode())
            ser_proc.stdin.flush()
            line = ser_proc.stdout.readline()
            return line.strip().decode(errors="replace") if line else None
        except Exception as e:
            log(f"serial error: {e}")
            ser_proc = None
            return None


def rpc_ping(params):
    line = serial_cmd("PING")
    if line is None:
        return None, -32002, "serial unavailable"
    if line == "PONG":
        return {"status": "ok"}, None, None
    return None, -32000, f"unexpected response: {line}"


def rpc_ping_dht11(params):
    line = serial_cmd("DHT:PING")
    if line is None:
        return None, -32002, "serial unavailable"
    if line == "PONG":
        return {"status": "ok"}, None, None
    return None, -32000, f"unexpected response: {line}"


def rpc_ping_ldr(params):
    line = serial_cmd("LDR:PING")
    if line is None:
        return None, -32002, "serial unavailable"
    if line == "PONG":
        return {"status": "ok"}, None, None
    return None, -32000, f"unexpected response: {line}"


def rpc_read_dht11(params):
    line = serial_cmd("DHT:READ")
    if line is None:
        return None, -32002, "serial unavailable"
    if line.startswith("ERR:"):
        return None, -32000, line[4:]
    if ",H:" in line and line.startswith("T:"):
        try:
            parts = line.split(",")
            temp = float(parts[0].split(":")[1])
            humi = float(parts[1].split(":")[1])
            return {"temp_c": temp, "humidity_pct": humi}, None, None
        except (IndexError, ValueError) as e:
            return None, -32000, f"parse error: {e}"
    return None, -32000, f"unexpected response: {line}"


def rpc_read_ldr(params):
    line = serial_cmd("LDR:READ")
    if line is None:
        return None, -32002, "serial unavailable"
    if line.startswith("ERR:"):
        return None, -32000, line[4:]
    if line.startswith("L:"):
        try:
            analog = int(line.split(":")[1])
            return {"analog": analog}, None, None
        except (IndexError, ValueError) as e:
            return None, -32000, f"parse error: {e}"
    return None, -32000, f"unexpected response: {line}"


def rpc_read_all(params):
    line = serial_cmd("READ")
    if line is None:
        return None, -32002, "serial unavailable"
    if line.startswith("ERR:"):
        return None, -32000, line[4:]
    result = {}
    for part in line.split(","):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        if k == "T":
            try:
                result["temp_c"] = float(v)
            except ValueError:
                pass
        elif k == "H":
            try:
                result["humidity_pct"] = float(v)
            except ValueError:
                pass
        elif k == "L":
            try:
                result["analog"] = int(v)
            except ValueError:
                pass
    if not result:
        return None, -32000, f"parse error: {line}"
    return result, None, None


METHODS = {
    "ping": rpc_ping,
    "ping_dht11": rpc_ping_dht11,
    "ping_ldr": rpc_ping_ldr,
    "read_dht11": rpc_read_dht11,
    "read_ldr": rpc_read_ldr,
    "read_all": rpc_read_all,
}


def log(msg):
    print(json.dumps({"event": "bridge", "msg": msg}), flush=True)


def handle_client(conn, addr):
    log(f"connection from {addr}")
    buf = ""
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buf += data.decode()
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    req = json.loads(line)
                except json.JSONDecodeError as e:
                    resp = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": f"parse error: {e}"}}
                    conn.sendall((json.dumps(resp) + "\n").encode())
                    continue

                req_id = req.get("id")
                method = req.get("method", "")
                params = req.get("params", {})

                if method not in METHODS:
                    resp = {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"method not found: {method}"}}
                    conn.sendall((json.dumps(resp) + "\n").encode())
                    continue

                result, err_code, err_msg = METHODS[method](params)
                if err_code:
                    resp = {"jsonrpc": "2.0", "id": req_id, "error": {"code": err_code, "message": err_msg}}
                else:
                    resp = {"jsonrpc": "2.0", "id": req_id, "result": result}
                conn.sendall((json.dumps(resp) + "\n").encode())
    except ConnectionResetError:
        pass
    except Exception as e:
        log(f"handler error: {e}")
    finally:
        conn.close()
        log(f"connection closed: {addr}")


def main():
    serial_start()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((BIND_ADDR, BIND_PORT))
    srv.listen(5)
    log(f"listening on {BIND_ADDR}:{BIND_PORT}")

    while True:
        conn, addr = srv.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()


if __name__ == "__main__":
    main()
