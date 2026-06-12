import React, { useEffect, useState } from 'react';
import { Activity } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const METRICS: Record<string, { label: string; color: string }> = {
  'sensor_temp_c': { label: 'Temperature (°C)', color: '#38bdf8' },
  'sensor_humidity_pct': { label: 'Humidity (%)', color: '#60a5fa' },
  'sensor_weight_kg': { label: 'Weight (kg)', color: '#f472b6' },
  'sensor_distance_cm': { label: 'Distance (cm)', color: '#a3e635' },
  'sensor_analog': { label: 'Luminosity', color: '#fcd34d' }
};

const TIME_RANGES = [
  { label: 'Last 15 Min', seconds: 15 * 60, step: '15s' },
  { label: 'Last 1 Hour', seconds: 60 * 60, step: '1m' },
  { label: 'Last 12 Hours', seconds: 12 * 60 * 60, step: '5m' },
  { label: 'Last 24 Hours', seconds: 24 * 60 * 60, step: '15m' },
];

export const HistoryCharts: React.FC = () => {
  const [activeMetric, setActiveMetric] = useState('sensor_temp_c');
  const [timeRange, setTimeRange] = useState(TIME_RANGES[0]);
  const [historyData, setHistoryData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const fetchHistory = async () => {
      setLoading(true);
      try {
        const end = Math.floor(Date.now() / 1000);
        const start = end - timeRange.seconds;
        
        const res = await fetch(`/api/history?query={__name__="${activeMetric}"}&start=${start}&end=${end}&step=${timeRange.step}`);
        const json = await res.json();
        
        if (json.status === "success" && json.data && json.data.result) {
          const result = json.data.result[0];
          if (result && result.values) {
            const formatted = result.values.map((val: any[]) => ({
               time: new Date(val[0] * 1000).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}),
               value: parseFloat(val[1])
            }));
            setHistoryData(formatted);
          } else {
            setHistoryData([]);
          }
        }
      } catch (err) {
        console.error("History fetch error:", err);
      } finally {
        setLoading(false);
      }
    };
    
    fetchHistory();
    const interval = setInterval(fetchHistory, 15000);
    return () => clearInterval(interval);
  }, [activeMetric, timeRange]);

  const metricConfig = METRICS[activeMetric];

  return (
    <div className="chart-container glass glass-panel" style={{ display: 'flex', flexDirection: 'column' }}>
      <div className="sensor-header" style={{ marginBottom: '20px', display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: '16px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <Activity color="#a78bfa" /> 
          <span>Historical Analysis</span>
        </div>
        
        <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
          <select 
            value={activeMetric}
            onChange={(e) => setActiveMetric(e.target.value)}
            className="glass-select"
          >
            {Object.entries(METRICS).map(([key, config]) => (
              <option key={key} value={key}>{config.label}</option>
            ))}
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
        {loading && historyData.length === 0 ? (
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#64748b' }}>
            Loading historical data...
          </div>
        ) : historyData.length > 0 ? (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={historyData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
              <XAxis dataKey="time" stroke="#94a3b8" />
              <YAxis stroke="#94a3b8" domain={['auto', 'auto']} />
              <Tooltip 
                contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px', color: '#f8fafc' }}
                itemStyle={{ color: metricConfig.color }}
              />
              <Line type="monotone" dataKey="value" name={metricConfig.label} stroke={metricConfig.color} strokeWidth={3} dot={false} activeDot={{ r: 8 }} />
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
