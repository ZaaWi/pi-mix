import React, { useEffect, useState } from 'react';
import { Activity } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

export interface MetricConfig {
  label: string;
  color: string;
  unit?: string;
  /** Bucket aggregation for 15m/1h series. Counters should use "last". */
  agg?: 'avg' | 'last' | 'max' | 'min';
  /** Derive a per-second rate from consecutive counter values. */
  rate?: boolean;
  /** Draw as a step line (band, connected, ...). */
  step?: boolean;
  /** Optional <optgroup> name for the metric selector. */
  group?: string;
  /** Format values for the tooltip and Y axis (bytes, rates, ...). */
  format?: (value: number) => string;
}

interface HistoryChartsProps {
  metrics?: Record<string, MetricConfig>;
  defaultMetric?: string;
  title?: string;
}

const DEFAULT_METRICS: Record<string, MetricConfig> = {
  'sensor_temp_c': { label: 'Temperature (°C)', color: '#38bdf8', unit: '°C' },
  'sensor_humidity_pct': { label: 'Humidity (%)', color: '#60a5fa', unit: '%' },
  'sensor_weight_kg': { label: 'Weight (kg)', color: '#f472b6', unit: 'kg' },
  'sensor_analog': { label: 'Luminosity', color: '#fcd34d' }
};

const TIME_RANGES = [
  { label: 'Last 15 Min', seconds: 15 * 60, step: '15s' },
  { label: 'Last 1 Hour', seconds: 60 * 60, step: '1m' },
  { label: 'Last 12 Hours', seconds: 12 * 60 * 60, step: '5m' },
  { label: 'Last 24 Hours', seconds: 24 * 60 * 60, step: '15m' },
  { label: 'Last 2 Days', seconds: 2 * 24 * 60 * 60, step: '30m' },
  { label: 'Last 7 Days', seconds: 7 * 24 * 60 * 60, step: '2h' },
  { label: 'Last 30 Days', seconds: 30 * 24 * 60 * 60, step: '12h' },
];

function formatTime(ts: number, rangeSeconds: number) {
  const date = new Date(ts * 1000);
  const timeStr = date.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'});
  if (rangeSeconds > 24 * 60 * 60) {
    const dateStr = date.toLocaleDateString([], {month: 'short', day: 'numeric'});
    return `${dateStr} ${timeStr}`;
  }
  return timeStr;
}

export const HistoryCharts: React.FC<HistoryChartsProps> = ({
  metrics = DEFAULT_METRICS,
  defaultMetric,
  title = 'Historical Analysis',
}) => {
  const firstMetric = defaultMetric && metrics[defaultMetric] ? defaultMetric : Object.keys(metrics)[0];
  const [activeMetric, setActiveMetric] = useState(firstMetric);
  const [timeRange, setTimeRange] = useState(TIME_RANGES[0]);
  const [historyData, setHistoryData] = useState<{ time: string; value: number }[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const metricConfig = metrics[activeMetric] || Object.values(metrics)[0];
  const agg = metricConfig?.agg || 'avg';
  const rate = !!metricConfig?.rate;

  useEffect(() => {
    if (!metrics[activeMetric]) {
      setActiveMetric(Object.keys(metrics)[0]);
    }
  }, [metrics, activeMetric]);

  useEffect(() => {
    const controller = new AbortController();
    let inFlight = false;
    const fetchHistory = async () => {
      if(inFlight) return;
      inFlight = true;
      setLoading(true);
      try {
        const end = Math.floor(Date.now() / 15000) * 15;
        const start = end - timeRange.seconds;

        const res = await fetch(`/api/history?query={__name__="${activeMetric}"}&start=${start}&end=${end}&step=${timeRange.step}&agg=${agg}`, {signal: controller.signal});
        if(!res.ok) throw new Error(res.status === 503 ? "History storage is unavailable" : "History request failed");
        const json = await res.json();
        if(json.status !== "success") throw new Error("History request failed");
        setError('');

        if (json.status === "success" && json.data && json.data.result) {
          const result = json.data.result[0];
          if (result && result.values) {
            const raw = result.values.map((val: any[]) => ({
              ts: Number(val[0]),
              value: parseFloat(val[1]),
            }));
            const formatted = rate
              ? raw.slice(1).map((p: {ts: number; value: number}, i: number) => {
                  const dt = p.ts - raw[i].ts;
                  const dv = p.value - raw[i].value;
                  return {
                    time: formatTime(p.ts, timeRange.seconds),
                    // A negative delta means the counter reset; clamp to zero.
                    value: dt > 0 && dv >= 0 ? dv / dt : 0,
                  };
                })
              : raw.map((p: {ts: number; value: number}) => ({
                  time: formatTime(p.ts, timeRange.seconds),
                  value: p.value,
                }));
            setHistoryData(formatted);
          } else {
            setHistoryData([]);
          }
        }
      } catch (err) {
        if(!controller.signal.aborted) {setError(err instanceof Error ? err.message : "History request failed");setHistoryData([]);}
      } finally {
        inFlight = false;
        if(!controller.signal.aborted) setLoading(false);
      }
    };

    fetchHistory();
    const interval = setInterval(fetchHistory, 15000);
    return () => {controller.abort();clearInterval(interval);};
  }, [activeMetric, timeRange, agg, rate]);

  const lineType = metricConfig?.step ? 'stepAfter' : 'monotone';

  // Group metrics into <optgroup> sections while preserving declaration order.
  const metricGroups = (() => {
    const order: string[] = [];
    const groups = new Map<string, [string, MetricConfig][]>();
    for (const entry of Object.entries(metrics)) {
      const name = entry[1].group || '';
      if (!groups.has(name)) {
        groups.set(name, []);
        order.push(name);
      }
      groups.get(name)!.push(entry);
    }
    return order.map(name => ({ name, items: groups.get(name)! }));
  })();

  const formatValue = (value: number | string) =>
    metricConfig?.format
      ? metricConfig.format(Number(value))
      : `${Number(value).toFixed(2)}${metricConfig?.unit ? ' ' + metricConfig.unit : ''}`;

  return (
    <div className="chart-container glass glass-panel" style={{ display: 'flex', flexDirection: 'column' }}>
      <div className="sensor-header" style={{ marginBottom: '20px', display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: '16px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <Activity color="#a78bfa" />
          <span>{title}</span>
        </div>

        <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
          <select
            value={activeMetric}
            onChange={(e) => setActiveMetric(e.target.value)}
            className="glass-select"
          >
            {metricGroups.map(group =>
              group.name ? (
                <optgroup key={group.name} label={group.name}>
                  {group.items.map(([key, config]) => (
                    <option key={key} value={key}>{config.label}</option>
                  ))}
                </optgroup>
              ) : (
                group.items.map(([key, config]) => (
                  <option key={key} value={key}>{config.label}</option>
                ))
              )
            )}
          </select>

          <select
            value={timeRange.seconds}
            onChange={(e) => setTimeRange(TIME_RANGES.find(t => t.seconds === Number(e.target.value)) || TIME_RANGES[0])}
            className="glass-select"
          >
            {TIME_RANGES.map(t => (
              <option key={t.seconds} value={t.seconds}>{t.label}</option>
            ))}
          </select>
        </div>
      </div>

      <div style={{ flex: 1, width: '100%', minHeight: '300px' }}>
        {error ? (<div role="alert" style={{color: "#fca5a5", padding: "1rem"}}>{error}</div>) : loading && historyData.length === 0 ? (
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#64748b' }}>
            Loading historical data...
          </div>
        ) : historyData.length > 0 ? (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={historyData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
              <XAxis dataKey="time" stroke="#94a3b8" />
              <YAxis stroke="#94a3b8" domain={rate ? [0, 'auto'] : ['auto', 'auto']} tickFormatter={metricConfig?.format} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px', color: '#f8fafc' }}
                itemStyle={{ color: metricConfig?.color }}
                formatter={(value: unknown) => [
                  formatValue(Number(value)),
                  metricConfig?.label || activeMetric,
                ]}
              />
              <Line type={lineType} dataKey="value" name={metricConfig?.label} stroke={metricConfig?.color} strokeWidth={3} dot={false} activeDot={{ r: 8 }} />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#64748b' }}>
            No data found for this time range.
          </div>
        )}
      </div>
    </div>
  );
};
