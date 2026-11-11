export interface Message {
  title: string;
  message: string;
  desc: string;
  color?: string;
  timestamp: number;
}

export interface CudyClient {
  hostname?: string;
  conn_type?: string;
  ip?: string;
  mac?: string;
  up_kbps?: number;
  down_kbps?: number;
  signal?: number | null;
  online_seconds?: number | null;
  internet?: boolean;
}

export interface CudyCellular {
  ts?: number;
  connected?: boolean;
  network_type?: string;
  public_ip?: string;
  ip_address?: string;
  mode?: string;
  mcc?: string;
  mnc?: string;
  cell_id?: string;
  pcid?: number;
  band?: number;
  ul_bandwidth_mhz?: number;
  dl_bandwidth_mhz?: number;
  rsrp?: number;
  rsrq?: number;
  sinr?: number;
  rssi?: number;
  pcc?: string;
}

export interface CudyUsage {
  ts?: number;
  current_bytes?: number;
  monthly_bytes?: number;
  total_bytes?: number;
}

export interface CudyUsageConfig {
  ts?: number;
  sim_slot?: string;
  monthly_stats?: string;
  start_date?: string;
  total_used?: string;
  monthly_used?: string;
  total_limit?: string;
  monthly_limit?: string;
  total_allowance?: string;
  monthly_allowance?: string;
  total_allowance_unit?: string;
  monthly_allowance_unit?: string;
  alert_pct?: string;
}

export interface CudyInterfaceStats {
  txbytes?: number;
  rxbytes?: number;
  txpkts?: number;
  rxpkts?: number;
  txerrs?: number;
  txdrop?: number;
  rxerrs?: number;
  rxdrop?: number;
}

export interface CudyClientsState {
  ts?: number;
  clients?: CudyClient[];
  count?: number;
  total_clients?: number;
  truncated?: boolean;
}

export interface CudyState {
  cellular?: CudyCellular;
  usage?: CudyUsage;
  usage_config?: CudyUsageConfig;
  interfaces?: { ts?: number; [name: string]: CudyInterfaceStats | number | undefined };
  clients?: CudyClientsState;
  mesh?: Record<string, unknown>;
  mesh_nodes?: Record<string, unknown>;
  wan?: Record<string, unknown>;
  lan?: Record<string, unknown>;
  wireless?: Record<string, unknown>;
  system?: Record<string, unknown>;
  [area: string]: unknown;
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
  cudy?: CudyState;
}
