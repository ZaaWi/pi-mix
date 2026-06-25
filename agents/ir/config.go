package main

import (
	"fmt"
	"os"
)

type Config struct {
	GPIOChip   string
	GPIOOffset int
	MQTTBroker string
	MQTTTopic  string
	ActionFile string
	BridgeHost string
	BridgePort string
}

func LoadConfig() Config {
	return Config{
		GPIOChip:   env("IR_GPIO_CHIP", "gpiochip0"),
		GPIOOffset: envInt("IR_GPIO_OFFSET", 17),
		MQTTBroker: env("MQTT_BROKER", "tcp://mosquitto.iot.svc.cluster.local:1883"),
		MQTTTopic:  env("MQTT_TOPIC", "pi/ir"),
		ActionFile: env("IR_ACTION_FILE", "/etc/ir-actions.json"),
		BridgeHost: env("BRIDGE_HOST", "serial-bridge.iot.svc.cluster.local"),
		BridgePort: env("BRIDGE_PORT", "9600"),
	}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return d
}
