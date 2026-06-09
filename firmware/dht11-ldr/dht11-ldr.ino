#include <DHT.h>

#define DHTPIN 2
#define DHTTYPE DHT11
#define LDR_PIN A0
#define LED_PIN 3

DHT dht(DHTPIN, DHTTYPE);

void setup() {
  Serial.begin(9600);
  dht.begin();
  pinMode(LED_PIN, OUTPUT);
}

void loop() {
  if (Serial.available() > 0) {
    handleCommand();
  }
}

void handleCommand() {
  String cmd = Serial.readStringUntil('\n');
  cmd.trim();

  if (cmd == "PING" || cmd == "DHT:PING") {
    Serial.println("PONG");
  }
  else if (cmd == "LDR:PING") {
    Serial.println("PONG");
  }
  else if (cmd == "DHT:READ") {
    readDHT();
  }
  else if (cmd == "LDR:READ") {
    readLDR();
  }
  else if (cmd == "LED:1") {
    digitalWrite(LED_PIN, HIGH);
    Serial.println("LED:OK");
  }
  else if (cmd == "LED:0") {
    digitalWrite(LED_PIN, LOW);
    Serial.println("LED:OK");
  }
  else if (cmd == "READ") {
    readDHT();
    Serial.print(",");
    readLDR();
  }
  else {
    Serial.print("ERR:Unknown command ");
    Serial.println(cmd);
  }
}

void readDHT() {
  float h = dht.readHumidity();
  float t = dht.readTemperature();

  if (isnan(h) || isnan(t)) {
    Serial.println("ERR:Failed to read DHT");
    return;
  }
  if (t < -10 || t > 60) {
    Serial.println("ERR:Temperature out of range");
    return;
  }
  if (h < 0 || h > 100) {
    Serial.println("ERR:Humidity out of range");
    return;
  }
  Serial.print("T:");
  Serial.print(t, 1);
  Serial.print(",H:");
  Serial.println(h, 0);
}

void readLDR() {
  int ldr = analogRead(LDR_PIN);
  Serial.print("L:");
  Serial.println(ldr);
}
