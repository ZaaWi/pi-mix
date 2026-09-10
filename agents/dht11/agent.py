#!/usr/bin/env python3
"""DHT11 sensor agent: serial-bridge to MQTT, with bounded RAM buffering."""

import json
import uuid
import math
import os
import signal
import socket
import threading
import time

import paho.mqtt.client as mqtt

BRIDGE_HOST = os.getenv("BRIDGE_HOST", "serial-bridge.iot.svc.cluster.local")
BRIDGE_PORT = int(os.getenv("BRIDGE_PORT", "9600"))
POLL_INTERVAL = max(0.1, float(os.getenv("DHT11_INTERVAL", "5.0")))
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto.iot.svc.cluster.local")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "pi/dht11")


DEBUG_READINGS = os.getenv("DEBUG_READINGS", "").lower() in ("1", "true", "yes")
_error_times = {}


def log_error(msg):
    # Avoid filling the Pi's container logs during a sustained outage.
    now = time.monotonic()
    if now - _error_times.get(msg, float("-inf")) >= 30:
        print(json.dumps({"event": "error", "msg": msg}), flush=True)
        _error_times[msg] = now
    if len(_error_times) > 128:
        _error_times.clear()


class Publisher:
    def __init__(self):
        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            self.client = mqtt.Client()
        self.connected = False
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_connect_fail = self._on_connect_fail
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.client.max_queued_messages_set(max(1, int(os.getenv("MQTT_MAX_QUEUED", "128"))))
        self.client.max_inflight_messages_set(20)
        self.client.connect_timeout = 5
        if os.getenv("MQTT_USERNAME"):
            self.client.username_pw_set(os.environ["MQTT_USERNAME"], os.getenv("MQTT_PASSWORD"))
        # connect_async performs no network I/O. The network loop retries even
        # when the broker is unavailable at process startup.
        self.client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=30)
        self.client.loop_start()

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        self.connected = getattr(reason_code, "value", reason_code) == 0
        if self.connected:
            print(json.dumps({"event": "mqtt_connected"}), flush=True)
        else:
            log_error(f"mqtt connection refused: {reason_code}")

    def _on_disconnect(self, client, userdata, *args):
        self.connected = False

    def _on_connect_fail(self, client, userdata):
        self.connected = False
        log_error("mqtt connection failed; retrying")

    def publish(self, payload):
        payload.setdefault("event_id", uuid.uuid4().hex)
        payload.setdefault("ts", int(time.time()))
        # QoS 1 is queued in bounded RAM by Paho while offline. Queue saturation
        # drops new readings; there is intentionally no sensor history on SD.
        try:
            info = self.client.publish(MQTT_TOPIC, json.dumps(payload, allow_nan=False), qos=1)
            if info.rc not in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
                log_error(f"mqtt publish rejected: {info.rc}")
        except (ValueError, RuntimeError, OSError) as exc:
            log_error(f"mqtt publish failed: {exc}")
        if DEBUG_READINGS:
            print(json.dumps(payload), flush=True)

    def close(self):
        self.client.disconnect()
        # loop_stop can wait for an in-progress connect. Do not let shutdown
        # exceed the pod's grace period during a broker or DNS outage.
        stopper = threading.Thread(target=self.client.loop_stop, daemon=True)
        stopper.start()
        stopper.join(timeout=3)
        self.connected = False


def rpc_call(method, params=None):
    req = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}})
    try:
        deadline = time.monotonic() + 5
        with socket.create_connection((BRIDGE_HOST, BRIDGE_PORT), timeout=5) as conn:
            conn.settimeout(max(0.001, deadline - time.monotonic()))
            conn.sendall((req + "\n").encode())
            data = bytearray()
            while b"\n" not in data:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("bridge response deadline exceeded")
                conn.settimeout(remaining)
                chunk = conn.recv(min(4096, 16385 - len(data)))
                if not chunk:
                    raise ValueError("bridge closed before completing response")
                data.extend(chunk)
                if len(data) > 16384:
                    raise ValueError("bridge response too large")
        resp = json.loads(bytes(data).split(b"\n", 1)[0])
        if not isinstance(resp, dict) or resp.get("id") != 1 or resp.get("jsonrpc") != "2.0":
            raise ValueError("invalid bridge response")
        if resp.get("error") is not None:
            return None, str(resp["error"].get("message", "bridge RPC error"))
        result = resp.get("result")
        if not isinstance(result, dict):
            raise ValueError("bridge result must be an object")
        return result, None
    except (OSError, ValueError, UnicodeError, AttributeError) as exc:
        return None, str(exc)


def validate_reading(result):
    for key, low, high in (("temp_c", -10, 60), ("humidity_pct", 0, 100)):
        value = result.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not low <= value <= high:
            raise ValueError(f"invalid {key}")
    return {"temp_c": result["temp_c"], "humidity_pct": result["humidity_pct"]}


def main():
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    pub = Publisher()
    try:
        while not stop.is_set():
            result, err = rpc_call("read_dht11")
            if err:
                log_error(f"dht11 read failed: {err}")
                stop.wait(3)
                continue
            try:
                values = validate_reading(result)
            except ValueError as exc:
                log_error(str(exc))
                stop.wait(3)
                continue
            pub.publish({
                "event": "reading",
                "sensor": "dht11",
                "ts": int(time.time()),
                **values,
            })
            stop.wait(POLL_INTERVAL)
    finally:
        pub.close()


if __name__ == "__main__":
    main()
