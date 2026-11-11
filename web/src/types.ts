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
  cell_id_num?: number;
  pcid?: number;
  band?: number;
  ul_bandwidth_mhz?: number;
  dl_bandwidth_mhz?: number;
  rsrp?: number;
  rsrq?: number;
  sinr?: number;
  rssi?: number;
  pcc?: string;
  scc?: string;
  scc_list?: string[];
  scc_count?: number;
  ca_count?: number;
  connected_seconds?: number | null;
  session_traffic_bytes?: number;
  upload_bytes?: number;
  download_bytes?: number;
}

export interface CudyUsage {
  ts?: number;
  current_bytes?: number;
  monthly_bytes?: number;
  total_bytes?: number;
  upload_bytes?: number;
  download_bytes?: number;
}

export interface CudyUsageConfig {
  ts?: number;
  sim_slot?: string;
  monthly_stats?: string;
  monthly_stats_enabled?: boolean;
  start_date?: string;
  total_used?: string;
  monthly_used?: string;
  total_limit?: string;
  monthly_limit?: string;
  total_used_bytes?: number | null;
  monthly_used_bytes?: number | null;
  total_limit_bytes?: number | null;
  monthly_limit_bytes?: number | null;
  total_allowance?: string;
  monthly_allowance?: string;
  total_allowance_bytes?: number | null;
  monthly_allowance_bytes?: number | null;
  total_allowance_unit?: string;
  monthly_allowance_unit?: string;
  alert_pct?: string;
}

/** A flattened cudy/events/<kind> payload persisted by the API. */
export interface CudyEvent {
  id?: string;
  ts?: number;
  kind?: string;
  derived?: boolean;
  [field: string]: unknown;
}

export interface CudyClientSample {
  ts: number;
  clients: CudyClient[];
}

export interface CudyClientHistory {
  window_seconds?: number;
  samples?: CudyClientSample[];
}

export interface CudyBurnSummary {
  available?: boolean;
  window_seconds?: number;
  start?: number;
  end?: number;
  source?: string;
  samples?: number;
  start_ts?: number;
  end_ts?: number;
  start_bytes?: number;
  end_bytes?: number;
  delta_bytes?: number;
  reset?: boolean;
  bytes_per_day?: number;
  projected_30d_bytes?: number;
}

export interface CudyDriftSummary {
  available?: boolean;
  router_total_bytes?: number;
  telemetry_total_bytes?: number;
  delta_bytes?: number;
  pct?: number;
}

export interface CudyUsageSummary {
  ts?: number;
  usage?: CudyUsage;
  config?: CudyUsageConfig;
  enforced?: boolean;
  burn?: CudyBurnSummary;
  cycle?: CudyBurnSummary;
  drift?: CudyDriftSummary;
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
