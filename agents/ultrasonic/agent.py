#!/usr/bin/env python3
"""Ultrasonic sensor agent — reads HC-SR04 via libgpiod v2, publishes to MQTT.

Pulse-echo measurement with busy-wait timeouts (1s per edge).
Takes SAMPLES readings and reports median distance (cm) to avoid outliers.
"""

import json
import os
import statistics
import time

# gpiod v2 API via debian:trixie-slim system package.
# NOT using pip install gpiod (needs gcc/libgpiod-dev for CFFI compilation).
# NOT using python3-libgpiod on python:3-slim (ABI mismatch with Python 3.14).
import gpiod
from gpiod.line import Direction, Value
import paho.mqtt.client as mqtt

TRIG = int(os.getenv("US_TRIG_PIN", "17"))
ECHO = int(os.getenv("US_ECHO_PIN", "27"))
INTERVAL = float(os.getenv("US_INTERVAL", "1.0"))
SAMPLES = int(os.getenv("US_SAMPLES", "3"))
GPIO_CHIP = os.getenv("US_GPIO_CHIP", "/dev/gpiochip0")
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto.iot.svc.cluster.local")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "pi/ultrasonic")


def log_error(msg):
    print(json.dumps({"event": "error", "msg": msg}), flush=True)


def measure(req):
    req.set_value(TRIG, Value.ACTIVE)
    time.sleep(0.00001)
    req.set_value(TRIG, Value.INACTIVE)

    t0 = time.time()
    while req.get_value(ECHO) == Value.INACTIVE:
        if time.time() - t0 > 1:
            return None
    t1 = time.time()

    while req.get_value(ECHO) == Value.ACTIVE:
        if time.time() - t1 > 1:
            return None
    t2 = time.time()

    return (t2 - t1) * 34300 / 2


def read_median(req):
    samples = []
    for _ in range(SAMPLES):
        d = measure(req)
        if d is not None:
            samples.append(d)
        time.sleep(0.05)
    if not samples:
        return None
    return round(statistics.median(samples), 1)


def setup_gpio():
    config = {
        TRIG: gpiod.LineSettings(direction=Direction.OUTPUT, output_value=Value.INACTIVE),
        ECHO: gpiod.LineSettings(direction=Direction.INPUT),
    }
    return gpiod.request_lines(GPIO_CHIP, config=config)


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
            log_error(f"mqtt connect failed: {e}")

    def publish(self, payload):
        if self.connected:
            try:
                self.client.publish(MQTT_TOPIC, json.dumps(payload), qos=1)
            except Exception as e:
                log_error(f"mqtt publish failed: {e}")
        print(json.dumps(payload), flush=True)


def main():
    pub = Publisher()
    while True:
        req = None
        try:
            req = setup_gpio()
            log_error("gpio initialised")

            while True:
                d = read_median(req)
                if d is None:
                    log_error("measurement timeout")
                else:
                    payload = {
                        "event": "reading",
                        "sensor": "ultrasonic",
                        "distance_cm": d,
                    }
                    pub.publish(payload)
                time.sleep(INTERVAL)

        except Exception as e:
            log_error(f"gpio error: {e}")
            if req is not None:
                try:
                    req.release()
                except Exception:
                    pass
            time.sleep(3)


if __name__ == "__main__":
    main()
