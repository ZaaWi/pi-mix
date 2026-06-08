#!/usr/bin/env python3
"""LDR sensor agent — calls serial-bridge JSON-RPC, publishes to MQTT."""

import json
import os
import socket
import time

import paho.mqtt.client as mqtt

BRIDGE_HOST = os.getenv("BRIDGE_HOST", "serial-bridge.iot.svc.cluster.local")
BRIDGE_PORT = int(os.getenv("BRIDGE_PORT", "9600"))
POLL_INTERVAL = float(os.getenv("LDR_INTERVAL", "5.0"))
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto.iot.svc.cluster.local")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "pi/ldr")


def rpc_call(method, params=None):
    if params is None:
        params = {}
    req = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    try:
        s = socket.create_connection((BRIDGE_HOST, BRIDGE_PORT), timeout=5)
        s.sendall((req + "\n").encode())
        data = s.recv(4096)
        s.close()
        resp = json.loads(data.decode().strip())
        if "error" in resp and resp["error"] is not None:
            return None, resp["error"]["message"]
        return resp.get("result"), None
    except (socket.timeout, ConnectionRefusedError, OSError, json.JSONDecodeError) as e:
        return None, str(e)


class Publisher:
    def __init__(self):
        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            self.client = mqtt.Client()
        self.connected = False
        try:
            self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
            self.client.loop_start()
            self.connected = True
        except Exception as e:
            print(json.dumps({"event": "error", "msg": f"mqtt connect failed: {e}"}), flush=True)

    def publish(self, payload):
        if self.connected:
            try:
                self.client.publish(MQTT_TOPIC, json.dumps(payload), qos=1)
            except Exception as e:
                print(json.dumps({"event": "error", "msg": f"mqtt publish failed: {e}"}), flush=True)
        print(json.dumps(payload), flush=True)


def log_error(msg):
    print(json.dumps({"event": "error", "msg": msg}), flush=True)


def wait_for_bridge():
    while True:
        result, err = rpc_call("ping")
        if err is None:
            log_error("bridge online")
            return
        log_error(f"bridge not reachable: {err}")
        time.sleep(3)


def main():
    pub = Publisher()
    wait_for_bridge()

    while True:
        result, err = rpc_call("ping_ldr")
        if err:
            log_error(f"ldr ping failed: {err}")
            time.sleep(3)
            continue

        result, err = rpc_call("read_ldr")
        if err:
            log_error(f"ldr read failed: {err}")
            time.sleep(3)
            continue

        payload = {
            "event": "reading",
            "sensor": "ldr",
            "analog": result["analog"],
        }
        pub.publish(payload)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
