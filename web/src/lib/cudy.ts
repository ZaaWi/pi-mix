/** Shared formatting and decoding helpers for the Cudy router tab. */

export const KB = 1024;
export const MB = KB * 1024;
export const GB = MB * 1024;
export const TB = GB * 1024;

export const fmtBytes = (value?: number | null): string => {
  if (value === undefined || value === null || !Number.isFinite(value)) return '--';
  const abs = Math.abs(value);
  if (abs >= TB) return `${(value / TB).toFixed(2)} TB`;
  if (abs >= GB) return `${(value / GB).toFixed(2)} GB`;
  if (abs >= MB) return `${(value / MB).toFixed(1)} MB`;
  if (abs >= KB) return `${(value / KB).toFixed(1)} KB`;
  return `${value.toFixed(0)} B`;
};

export const fmtRate = (value?: number | null): string => {
  if (value === undefined || value === null || !Number.isFinite(value)) return '--';
  return `${fmtBytes(value)}/s`;
};

export const fmtKbps = (value?: number | null): string => {
  if (value === undefined || value === null || !Number.isFinite(value)) return '--';
  if (value >= 1000) return `${(value / 1000).toFixed(2)} Mbps`;
  return `${value.toFixed(1)} kbps`;
};

export const fmtCount = (value?: number | null): string => {
  if (value === undefined || value === null || !Number.isFinite(value)) return '--';
  return value.toLocaleString();
};

export const fmtDuration = (seconds?: number | null): string => {
  if (!seconds || seconds <= 0) return '--';
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
};

export const fmtAgo = (ts?: number | null): string => {
  if (!ts) return '--';
  const seconds = Math.max(0, Math.floor(Date.now() / 1000) - ts);
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
};

export const clamp = (value: number, min: number, max: number): number =>
  Math.min(max, Math.max(min, value));

export interface SignalSpec {
  label: string;
  unit: string;
  min: number;
  max: number;
  /** Higher is better; values above these boundaries get the label. */
  excellent: number;
  good: number;
  fair: number;
}

export const RSRP_SPEC: SignalSpec = {
  label: 'RSRP', unit: 'dBm', min: -120, max: -60, excellent: -80, good: -90, fair: -100,
};
export const RSRQ_SPEC: SignalSpec = {
  label: 'RSRQ', unit: 'dB', min: -25, max: 0, excellent: -10, good: -15, fair: -20,
};
export const SINR_SPEC: SignalSpec = {
  label: 'SINR', unit: 'dB', min: -10, max: 30, excellent: 20, good: 13, fair: 0,
};

export interface SignalState {
  label: string;
  color: string;
  pct: number;
}

export const signalState = (value: number | null | undefined, spec: SignalSpec): SignalState => {
  if (value === undefined || value === null || !Number.isFinite(value)) {
    return { label: 'No data', color: '#64748b', pct: 0 };
  }
  let label = 'Poor';
  let color = '#f87171';
  if (value >= spec.excellent) {
    label = 'Excellent';
    color = '#34d399';
  } else if (value >= spec.good) {
    label = 'Good';
    color = '#38bdf8';
  } else if (value >= spec.fair) {
    label = 'Fair';
    color = '#fbbf24';
  }
  return { label, color, pct: clamp(((value - spec.min) / (spec.max - spec.min)) * 100, 0, 100) };
};

export interface DecodedCell {
  enb: number;
  sector: number;
}

/** Decode an LTE ECI (hex or numeric) into eNB and sector. */
export const decodeCellId = (cellId?: string, cellIdNum?: number): DecodedCell | null => {
  let value = cellIdNum;
  if ((value === undefined || value === null || !Number.isFinite(value)) && cellId) {
    const raw = cellId.trim();
    value = /[a-f]/i.test(raw) ? parseInt(raw, 16) : parseInt(raw, 10);
  }
  if (value === undefined || value === null || !Number.isFinite(value) || value <= 0) return null;
  return { enb: Math.floor(value / 256), sector: value % 256 };
};

/** Extract the MHz value from a Cudy carrier string ("LTE BAND 7 / 20MHz"). */
export const carrierMhz = (text?: string | null): number | null => {
  if (!text) return null;
  const match = /(\d+(?:\.\d+)?)\s*MHz/i.exec(text);
  return match ? Number(match[1]) : null;
};

/** Local-time start of the current billing cycle for a router start day (1-31). */
export const cycleStartTs = (startDay: number, now = new Date()): number => {
  const day = Number.isFinite(startDay) ? clamp(Math.floor(startDay), 1, 31) : 1;
  const make = (year: number, month: number) => {
    const lastDay = new Date(year, month + 1, 0).getDate();
    return new Date(year, month, Math.min(day, lastDay), 0, 0, 0, 0);
  };
  let start = make(now.getFullYear(), now.getMonth());
  if (start.getTime() > now.getTime()) {
    start = make(now.getFullYear(), now.getMonth() - 1);
  }
  return Math.floor(start.getTime() / 1000);
};

/** Pearson correlation for two equal-length series. */
export const correlation = (xs: number[], ys: number[]): number | null => {
  const n = Math.min(xs.length, ys.length);
  if (n < 3) return null;
  let sx = 0;
  let sy = 0;
  for (let i = 0; i < n; i += 1) {
    sx += xs[i];
    sy += ys[i];
  }
  const mx = sx / n;
  const my = sy / n;
  let num = 0;
  let dx = 0;
  let dy = 0;
  for (let i = 0; i < n; i += 1) {
    const a = xs[i] - mx;
    const b = ys[i] - my;
    num += a * b;
    dx += a * a;
    dy += b * b;
  }
  if (dx === 0 || dy === 0) return null;
  return num / Math.sqrt(dx * dy);
};

export const readNumber = (value: unknown): number | null => {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  return null;
};
