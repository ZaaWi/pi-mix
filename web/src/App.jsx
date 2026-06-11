import { useEffect, useState } from 'react';
import { Thermometer, Droplets, Sun, Activity } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import './index.css';

function App() {
  const [state, setState] = useState({});
  const [pulses, setPulses] = useState({});
  const [isConnected, setIsConnected] = useState(false);
  const [historyData, setHistoryData] = useState([]);

  // Fetch initial state
  useEffect(() => {
    fetch('/api/state')
      .then(res => res.json())
      .then(data => setState(data))
      .catch(console.error);
  }, []);

  // Fetch historical data from VictoriaMetrics
  useEffect(() => {
    const fetchHistory = async () => {
      try {
        const end = Math.floor(Date.now() / 1000);
        const start = end - 1800; // Last 30 minutes
        const res = await fetch(`/api/history?query={sensor="dht11"}&start=${start}&end=${end}&step=15s`);
        const json = await res.json();
        
        if (json.status === "success" && json.data && json.data.result) {
          const tempResult = json.data.result.find(r => r.metric.__name__ === 'sensor_temp_c');
          
          if (tempResult && tempResult.values) {
            const formatted = tempResult.values.map(val => ({
               time: new Date(val[0] * 1000).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'}),
               temp_c: parseFloat(val[1])
            }));
            setHistoryData(formatted);
          }
        }
      } catch (err) {
        console.error("History fetch error:", err);
      }
    };
    
    fetchHistory();
    const interval = setInterval(fetchHistory, 10000);
    return () => clearInterval(interval);
  }, []);

  // Connect to live SSE stream
  useEffect(() => {
    const sse = new EventSource('/api/stream');
    
    sse.onopen = () => setIsConnected(true);
    sse.onerror = () => setIsConnected(false);

    sse.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      setState(prev => ({
        ...prev,
        [msg.sensor]: msg.data
      }));

      setPulses(prev => ({ ...prev, [msg.sensor]: true }));
      setTimeout(() => {
        setPulses(prev => ({ ...prev, [msg.sensor]: false }));
      }, 1000);
    };

    return () => sse.close();
  }, []);

  const dht = state.dht11 || {};
  const ldr = state.ldr || {};
  
  return (
    <div className="dashboard-container">
      <header className="header">
        <h1>pi-mix telemetry</h1>
        <p>Live edge-to-cloud sensor metrics</p>
        
        <div className={`status-badge ${isConnected ? 'status-live' : 'status-offline'}`}>
          <div className="status-dot"></div>
          {isConnected ? 'STREAMING LIVE' : 'RECONNECTING...'}
        </div>
      </header>

      <div className="grid">
        <div className="glass glass-panel sensor-card">
          <div className="sensor-header">
            <Thermometer color="#38bdf8" /> Temperature
          </div>
          <div className={`sensor-value ${pulses.dht11 ? 'pulse' : ''}`}>
            {dht.temp_c ? dht.temp_c.toFixed(1) : '--'} <span className="sensor-unit">°C</span>
          </div>
        </div>

        <div className="glass glass-panel sensor-card">
          <div className="sensor-header">
            <Droplets color="#38bdf8" /> Humidity
          </div>
          <div className={`sensor-value ${pulses.dht11 ? 'pulse' : ''}`}>
            {dht.humidity_pct ? dht.humidity_pct : '--'} <span className="sensor-unit">%</span>
          </div>
        </div>

        <div className="glass glass-panel sensor-card">
          <div className="sensor-header">
            <Sun color="#fcd34d" /> Luminosity
          </div>
          <div className={`sensor-value ${pulses.ldr ? 'pulse' : ''}`}>
            {ldr.analog ? ldr.analog : '--'} <span className="sensor-unit">analog</span>
          </div>
        </div>
      </div>
      
      <div className="chart-container glass glass-panel">
        <div className="sensor-header" style={{ marginBottom: '20px' }}>
          <Activity color="#a78bfa" /> Temperature History (Last 30 Min)
        </div>
        <div style={{ height: '300px', width: '100%' }}>
          {historyData.length > 0 ? (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={historyData}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                <XAxis dataKey="time" stroke="#94a3b8" />
                <YAxis stroke="#94a3b8" domain={['dataMin - 1', 'dataMax + 1']} />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px', color: '#f8fafc' }}
                  itemStyle={{ color: '#38bdf8' }}
                />
                <Line type="monotone" dataKey="temp_c" stroke="#38bdf8" strokeWidth={3} dot={false} activeDot={{ r: 8 }} />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#64748b' }}>
              Waiting for VictoriaMetrics data...
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
