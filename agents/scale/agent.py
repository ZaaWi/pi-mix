#!/usr/bin/env python3
"""Scale sensor agent — BLE via bleak (BlueZ D-Bus), publishes to MQTT.

Uses bleak instead of btmon+bluetoothctl because btmon needs raw HCI monitor
sockets which don't work in k3s containers (EAFNOSUPPORT even with privileged:true).
bleak communicates with host bluetoothd over D-Bus, which works fine.
"""

import asyncio
from collections import deque
import json
import uuid
import os
import signal
import threading
import time

from bleak import BleakScanner
import paho.mqtt.client as mqtt

MAC = os.getenv("SCALE_MAC", "50:FB:19:29:54:37").upper()
SCAN_SEC = int(os.getenv("SCAN_SEC", "5"))
DEDUP_SEC = int(os.getenv("SCALE_DEDUP_SEC", "30"))
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto.iot.svc.cluster.local")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "pi/scale")


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


def decode_scale_data(data):
    """Parse EasyTouch scale manufacturer data."""
    b = bytes(data)
    if len(b) < 7:
        return None
    w = ((b[0] << 8) | b[1]) / 100.0
    if not 0 < w <= 500:
        return None
    locked = (b[2] << 8) | b[3]
    st = b[6]
    if (st == 0x25 or locked == 0x1770) and w > 1.0:
        return ("WEIGHT", w)
    if st == 0x24 and (b[0] or b[1]):
        return ("STANDING", w)
    return None


def detection_callback(device, advertising_data):
    if device.address.upper() != MAC:
        return
    if not advertising_data or not advertising_data.manufacturer_data:
        return

    for mfr_id, mfr_data in advertising_data.manufacturer_data.items():
        r = decode_scale_data(mfr_data)
        if r is not None:
            return (r[0], r[1], time.time())
    return None


async def scan():
    results = deque(maxlen=128)

    def callback(d, ad):
        r = detection_callback(d, ad)
        if r:
            results.append(r)

    scanner = BleakScanner(detection_callback=callback)
    try:
        await asyncio.wait_for(scanner.start(), timeout=10)
        await asyncio.sleep(max(1, SCAN_SEC))
    finally:
        await asyncio.wait_for(scanner.stop(), timeout=5)
    return results


async def main_async():
    pub = Publisher()
    seen_weights = {}
    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    print(json.dumps({"event": "startup", "msg": "scale agent starting"}), flush=True)
    try:
        while True:
            try:
                results = await scan()
            except Exception as exc:
                log_error(f"scan failed: {exc}")
                await asyncio.sleep(3)
                continue
            now = time.time()
            seen_weights = {w: ts for w, ts in seen_weights.items() if now - ts < DEDUP_SEC}
            for kind, weight, ts in results:
                if kind != "WEIGHT":
                    continue
                last = seen_weights.get(round(weight, 2))
                if last and ts - last < DEDUP_SEC:
                    continue
                seen_weights[round(weight, 2)] = ts
                pub.publish({
                    "event": "reading",
                    "sensor": "scale",
                    "ts": int(ts),
                    "weight_kg": round(weight, 2),
                    "status": "locked",
                })
    finally:
        pub.close()


def main():
    try:
        asyncio.run(main_async())
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass


if __name__ == "__main__":
    main()
