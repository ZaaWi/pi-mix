export interface Message {
  title: string;
  message: string;
  desc: string;
  color?: string;
  timestamp: number;
}

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
  ir?: {
    code: string;
    protocol: string;
    bits: number;
  };
}
