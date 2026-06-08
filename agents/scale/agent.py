#!/usr/bin/env python3
"""Scale sensor agent — BLE via bleak (BlueZ D-Bus), publishes to MQTT.

Uses bleak instead of btmon+bluetoothctl because btmon needs raw HCI monitor
sockets which don't work in k3s containers (EAFNOSUPPORT even with privileged:true).
bleak communicates with host bluetoothd over D-Bus, which works fine.
"""

import asyncio
import json
import os
import time

from bleak import BleakScanner
import paho.mqtt.client as mqtt

MAC = os.getenv("SCALE_MAC", "50:FB:19:29:54:37")
SCAN_SEC = int(os.getenv("SCAN_SEC", "5"))
DEDUP_SEC = int(os.getenv("SCALE_DEDUP_SEC", "30"))
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto.iot.svc.cluster.local")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "pi/scale")


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


def decode_scale_data(data):
    """Parse EasyTouch scale manufacturer data."""
    b = bytes(data)
    if len(b) < 7:
        return None
    w = ((b[0] << 8) | b[1]) / 100.0
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
    results = []

    def callback(d, ad):
        r = detection_callback(d, ad)
        if r:
            results.append(r)

    scanner = BleakScanner(detection_callback=callback)
    await scanner.start()
    await asyncio.sleep(SCAN_SEC)
    await scanner.stop()
    return results


def flatten(xss):
    return [x for xs in xss for x in xs]


async def main_async():
    pub = Publisher()
    seen_weights = {}

    print(json.dumps({"event": "startup", "msg": "scale agent starting"}), flush=True)

    while True:
        try:
            results = await scan()
        except Exception as e:
            print(json.dumps({"event": "error", "msg": f"scan failed: {e}"}), flush=True)
            await asyncio.sleep(1)
            continue

        for kind, weight, ts in results:
            if kind == "STANDING":
                print(json.dumps({"event": "debug", "msg": f"standing weight {weight:.2f} kg (not locked)"}), flush=True)
                continue

            if kind == "WEIGHT":
                last = seen_weights.get(round(weight, 2))
                if last and ts - last < DEDUP_SEC:
                    continue
                seen_weights[round(weight, 2)] = ts

                payload = {
                    "event": "reading",
                    "sensor": "scale",
                    "weight_kg": round(weight, 2),
                    "status": "locked",
                }
                pub.publish(payload)


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
