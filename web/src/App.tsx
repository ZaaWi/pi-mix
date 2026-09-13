import { useEffect, useState } from 'react';
import './index.css';
import { LiveMetricsGrid } from './components/LiveMetricsGrid';
import { HistoryCharts } from './components/HistoryCharts';
import { MessagesPanel } from './components/MessagesPanel';
import { SensorData } from './types';

function App() {
  const [state, setState] = useState<SensorData>({});
  const [pulses, setPulses] = useState<Record<string, boolean>>({});
  const [isConnected, setIsConnected] = useState(false);
  const [fresh, setFresh] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    const refresh = () => fetch('/api/status', {signal: controller.signal})
      .then(r => {if(!r.ok) throw new Error('status unavailable');return r.json();})
      .then(s => setFresh(s.mqtt_connected && !s.sensors?.dht11?.stale && !s.sensors?.ldr?.stale))
      .catch(() => {if(!controller.signal.aborted) setFresh(false);});
    refresh(); const interval = setInterval(refresh, 5000);
    return () => {controller.abort();clearInterval(interval);};
  }, []);

  useEffect(() => {
    fetch('/api/state')
      .then(res => res.json())
      .then(data => setState(data))
      .catch(console.error);
  }, []);

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

  return (
    <div className="dashboard-container">
      <header className="header">
        <h1>pi-mix telemetry</h1>
        <p>Live edge-to-cloud sensor metrics</p>
        
        <div className={`status-badge ${isConnected && fresh ? 'status-live' : 'status-offline'}`}>
          <div className="status-dot"></div>
          {!isConnected ? 'RECONNECTING...' : fresh ? 'STREAMING LIVE' : 'SENSOR DATA STALE'}
        </div>
      </header>

      <LiveMetricsGrid state={state} pulses={pulses} />
      <HistoryCharts />
      <MessagesPanel />
    </div>
  );
}

export default App;
