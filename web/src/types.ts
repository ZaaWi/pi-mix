export interface SensorData {
  dht11?: {
    temp_c: number;
    humidity_pct: number;
  };
  ldr?: {
    analog: number;
  };
  scale?: {
    weight_kg: number;
    status: string;
  };
  ultrasonic?: {
    distance_cm: number;
  };
}
