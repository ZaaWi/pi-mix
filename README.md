# pi-mix

Sensor data pipeline on Raspberry Pi 5 (k3s). Arduino (DHT11 + LDR) over UART, ultrasonic (gpiod), and BLE scale → JSON-RPC serial bridge → agent pods → Mosquitto MQTT.

## Structure

- `agents/` — one pod per sensor (dht11, ldr, ultrasonic, scale) plus serial-bridge
- `firmware/` — Arduino .ino sketches
- `k8s/` — Kubernetes manifests (ArgoCD-managed)
- `.github/workflows/` — CI to build & push images to Docker Hub

## Deploy

Images build via GitHub Actions. ArgoCD syncs `k8s/` to the cluster automatically.
