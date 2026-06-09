#!/bin/sh
set -e

FIRMWARE_DIR="${FIRMWARE_DIR:-dht11-ldr}"
BRIDGE_HOST="${BRIDGE_HOST:-serial-bridge}"
BRIDGE_PORT="${BRIDGE_PORT:-9600}"

cd "/firmware/${FIRMWARE_DIR}"
arduino-cli compile --fqbn arduino:avr:uno --output-dir /tmp/build .

HEX=$(base64 -w0 /tmp/build/*.ino.hex)

RESP=$(printf '{"jsonrpc":"2.0","id":1,"method":"flash_firmware","params":{"hex":"%s","encoding":"base64"}}\n' "$HEX" \
  | nc -w 60 "${BRIDGE_HOST}" "${BRIDGE_PORT}")
echo "$RESP"
if ! echo "$RESP" | grep -qE '"result"|"status":\s*"ok"'; then
  echo "ERROR: flash failed" >&2
  exit 1
fi
