import React, { useEffect, useMemo, useState } from 'react';
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { Activity } from 'lucide-react';
import { correlation } from '../lib/cudy';

interface SeriesPoint {
  ts: number;
  value: number;
}

interface CorrelationPoint {
  ts: number;
  time: string;
  sinr: number | null;
  rsrp: number | null;
  mbps: number;
}

const RANGES = [
  { label: 'Last 24 Hours', seconds: 24 * 3600, step: '15m' },
  { label: 'Last 7 Days', seconds: 7 * 24 * 3600, step: '2h' },
  { label: 'Last 30 Days', seconds: 30 * 24 * 3600, step: '12h' },
];

const formatTime = (ts: number, rangeSeconds: number): string => {
  const date = new Date(ts * 1000);
  const time = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  if (rangeSeconds > 24 * 3600) {
    return `${date.toLocaleDateString([], { month: 'short', day: 'numeric' })} ${time}`;
  }
  return time;
};

async function fetchSeries(metric: string, agg: string, start: number, end: number, step: string, signal: AbortSignal): Promise<SeriesPoint[]> {
  const res = await fetch(`/api/history?query={__name__="${metric}"}&start=${start}&end=${end}&step=${step}&agg=${agg}`, { signal });
  if (!res.ok) throw new Error(res.status === 503 ? 'History storage is unavailable' : 'History request failed');
  const json = await res.json();
  if (json.status !== 'success') throw new Error('History request failed');
  const values = json.data?.result?.[0]?.values;
  if (!Array.isArray(values)) return [];
  return values.map((row: unknown[]) => ({ ts: Number(row[0]), value: Number(row[1]) }));
}

function nearest(points: SeriesPoint[], ts: number, tolerance: number): number | null {
  if (points.length === 0) return null;
  let lo = 0;
  let hi = points.length - 1;
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (points[mid].ts < ts) lo = mid + 1;
    else hi = mid;
  }
  const candidates = [points[lo], points[Math.max(0, lo - 1)]];
  let best: SeriesPoint | null = null;
  let bestDelta = Infinity;
  for (const point of candidates) {
    const delta = Math.abs(point.ts - ts);
    if (delta < bestDelta) {
      best = point;
      bestDelta = delta;
    }
  }
  return best && bestDelta <= tolerance ? best.value : null;
}

export const CudyCorrelation: React.FC = () => {
  const [range, setRange] = useState(RANGES[0]);
  const [points, setPoints] = useState<CorrelationPoint[]>([]);
  const [mode, setMode] = useState<'overlay' | 'scatter'>('overlay');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      try {
        const end = Math.floor(Date.now() / 60000) * 60;
        const start = end - range.seconds;
        const [sinr, rsrp, rx] = await Promise.all([
          fetchSeries('sensor_cudy_sinr', 'avg', start, end, range.step, controller.signal),
          fetchSeries('sensor_cudy_rsrp', 'avg', start, end, range.step, controller.signal),
          fetchSeries('sensor_cudy_cellular_rx_bytes', 'last', start, end, range.step, controller.signal),
        ]);
        if (controller.signal.aborted) return;

        // Convert the cumulative byte counter into Mbps.
        const throughput: SeriesPoint[] = [];
        for (let i = 1; i < rx.length; i += 1) {
          const dt = rx[i].ts - rx[i - 1].ts;
          const dv = rx[i].value - rx[i - 1].value;
          if (dt <= 0 || dv < 0) continue;
          throughput.push({ ts: rx[i].ts, value: (dv / dt) * 8 / 1_000_000 });
        }
        const rsrpByTs = new Map(rsrp.map((point) => [point.ts, point.value]));
        const tolerance = Math.max(900, Math.round(range.seconds / Math.max(12, sinr.length)) * 2);
        const merged: CorrelationPoint[] = [];
        for (const point of sinr) {
          const mbps = nearest(throughput, point.ts, tolerance);
          if (mbps === null) continue;
          merged.push({
            ts: point.ts,
            time: formatTime(point.ts, range.seconds),
            sinr: point.value,
            rsrp: rsrpByTs.get(point.ts) ?? null,
            mbps,
          });
        }
        setPoints(merged);
        setError('');
      } catch (err) {
        if (!controller.signal.aborted) {
          setError(err instanceof Error ? err.message : 'History request failed');
          setPoints([]);
        }
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    };
    load();
    const interval = setInterval(load, 60000);
    return () => {
      controller.abort();
      clearInterval(interval);
    };
  }, [range]);

  const stats = useMemo(() => {
    if (points.length === 0) return null;
    const sinrValues = points.map((point) => point.sinr ?? 0);
    const mbpsValues = points.map((point) => point.mbps);
    const peak = Math.max(...mbpsValues);
    const mean = mbpsValues.reduce((sum, value) => sum + value, 0) / mbpsValues.length;
    return {
      r: correlation(sinrValues, mbpsValues),
      peak,
      mean,
      count: points.length,
    };
  }, [points]);

  return (
    <section className="glass glass-panel cudy-section">
      <div className="cudy-section-head">
        <div className="cudy-section-title">
          <Activity color="#a78bfa" />
          RF vs throughput
        </div>
        <div className="cudy-head-tools">
          <div className="segmented">
            <button
              type="button"
              className={`segmented-btn ${mode === 'overlay' ? 'active' : ''}`}
              onClick={() => setMode('overlay')}
            >
              Overlay
            </button>
            <button
              type="button"
              className={`segmented-btn ${mode === 'scatter' ? 'active' : ''}`}
              onClick={() => setMode('scatter')}
            >
              Scatter
            </button>
          </div>
          <select
            value={range.seconds}
            onChange={(event) => setRange(RANGES.find((item) => item.seconds === Number(event.target.value)) || RANGES[0])}
            className="glass-select"
          >
            {RANGES.map((item) => (
              <option key={item.seconds} value={item.seconds}>{item.label}</option>
            ))}
          </select>
        </div>
      </div>

      {stats && (
        <div className="correlation-stats">
          <span>peak <strong>{stats.peak.toFixed(1)} Mbps</strong></span>
          <span>mean <strong>{stats.mean.toFixed(2)} Mbps</strong></span>
          <span>
            SINR↔throughput r = <strong>{stats.r === null ? '--' : stats.r.toFixed(2)}</strong>
          </span>
          <span className="cudy-muted">{stats.count} aligned samples · demand-driven, not causal</span>
        </div>
      )}

      <div style={{ width: '100%', minHeight: 300 }}>
        {error ? (
          <div className="cudy-empty">{error}</div>
        ) : loading && points.length === 0 ? (
          <div className="cudy-empty">Loading RF and throughput history…</div>
        ) : points.length === 0 ? (
          <div className="cudy-empty">No aligned RF/throughput samples for this range.</div>
        ) : mode === 'overlay' ? (
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={points} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
              <XAxis dataKey="time" stroke="#94a3b8" tick={{ fontSize: 11 }} minTickGap={24} />
              <YAxis yAxisId="signal" stroke="#94a3b8" tick={{ fontSize: 11 }} width={48} domain={['auto', 'auto']} label={{ value: 'dB', angle: -90, position: 'insideLeft', fill: '#94a3b8', fontSize: 11 }} />
              <YAxis yAxisId="rate" orientation="right" stroke="#94a3b8" tick={{ fontSize: 11 }} width={56} label={{ value: 'Mbps', angle: 90, position: 'insideRight', fill: '#94a3b8', fontSize: 11 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px', color: '#f8fafc' }}
                formatter={(value: unknown, name: unknown) => {
                  const numeric = Number(value);
                  const label = String(name);
                  if (label === 'Throughput') return [`${numeric.toFixed(2)} Mbps`, label];
                  return [`${numeric.toFixed(1)} dB`, label];
                }}
              />
              <Legend wrapperStyle={{ fontSize: '0.78rem' }} />
              <Area yAxisId="rate" type="monotone" dataKey="mbps" name="Throughput" stroke="#38bdf8" fill="#38bdf8" fillOpacity={0.15} strokeWidth={2} dot={false} isAnimationActive={false} />
              <Line yAxisId="signal" type="monotone" dataKey="sinr" name="SINR" stroke="#a78bfa" strokeWidth={2} dot={false} isAnimationActive={false} />
              <Line yAxisId="signal" type="monotone" dataKey="rsrp" name="RSRP" stroke="#34d399" strokeWidth={2} dot={false} isAnimationActive={false} />
            </ComposedChart>
          </ResponsiveContainer>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <ScatterChart margin={{ top: 8, right: 12, left: 0, bottom: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
              <XAxis type="number" dataKey="sinr" name="SINR" unit=" dB" stroke="#94a3b8" tick={{ fontSize: 11 }} domain={['auto', 'auto']} />
              <YAxis type="number" dataKey="mbps" name="Throughput" unit=" Mbps" stroke="#94a3b8" tick={{ fontSize: 11 }} width={72} domain={[0, 'auto']} />
              <Tooltip
                cursor={{ strokeDasharray: '3 3' }}
                contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px', color: '#f8fafc' }}
                formatter={(value: unknown, name: unknown) => [`${Number(value).toFixed(2)}${String(name) === 'SINR' ? ' dB' : ' Mbps'}`, String(name)]}
              />
              <Scatter data={points} fill="#38bdf8" fillOpacity={0.7} isAnimationActive={false} />
            </ScatterChart>
          </ResponsiveContainer>
        )}
      </div>
    </section>
  );
};
