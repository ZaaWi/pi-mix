#!/bin/sh
set -e

FIRMWARE_DIR="${FIRMWARE_DIR:-dht11-ldr}"
BRIDGE_HOST="${BRIDGE_HOST:-serial-bridge}"
BRIDGE_PORT="${BRIDGE_PORT:-9600}"

case "${BRIDGE_CONTROL_TOKEN:-}" in
  ""|*[!0-9a-fA-F]*) echo "BRIDGE_CONTROL_TOKEN must be a hex control token" >&2; exit 1;;
esac

cd "/firmware/${FIRMWARE_DIR}"
arduino-cli compile --fqbn arduino:avr:uno --output-dir /tmp/build .

HEX=$(base64 -w0 /tmp/build/*.ino.hex)

RESP=$(printf '{"jsonrpc":"2.0","id":1,"method":"flash_firmware","params":{"hex":"%s","encoding":"base64","control_token":"%s"}}\n' "$HEX" "$BRIDGE_CONTROL_TOKEN" \
  | nc -w 60 "${BRIDGE_HOST}" "${BRIDGE_PORT}")
echo "$RESP"
if ! echo "$RESP" | grep -qE '"result"|"status":\s*"ok"'; then
  echo "ERROR: flash failed" >&2
  exit 1
fi
