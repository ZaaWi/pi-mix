import { useEffect, useState } from 'react';
import { Radio, Router } from 'lucide-react';
import './index.css';
import { LiveMetricsGrid } from './components/LiveMetricsGrid';
import { HistoryCharts } from './components/HistoryCharts';
import { MessagesPanel } from './components/MessagesPanel';
import { CudyTab } from './components/CudyTab';
import { CudyEvent, SensorData } from './types';

type Tab = 'sensors' | 'cudy';

interface StatusResponse {
  mqtt_connected?: boolean;
  sensors?: Record<string, { last_seen?: number; stale?: boolean }>;
}

interface StreamMessage {
  sensor: string;
  data: unknown;
}

function App() {
  const [state, setState] = useState<SensorData>({});
  const [pulses, setPulses] = useState<Record<string, boolean>>({});
  const [isConnected, setIsConnected] = useState(false);
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [tab, setTab] = useState<Tab>('sensors');
  const [cudyEvents, setCudyEvents] = useState<CudyEvent[]>([]);

  useEffect(() => {
    const controller = new AbortController();
    const refresh = () => fetch('/api/status', {signal: controller.signal})
      .then(r => {if(!r.ok) throw new Error('status unavailable');return r.json();})
      .then(s => setStatus(s))
      .catch(() => {if(!controller.signal.aborted) setStatus(null);});
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
      const msg: StreamMessage = JSON.parse(e.data);
      if (msg.sensor === 'cudy') {
        const { area, state: areaState } = msg.data as { area: string; state: SensorData['cudy'] };
        setState(prev => ({
          ...prev,
          cudy: { ...(prev.cudy || {}), [area]: areaState },
        }));
        return;
      }
      if (msg.sensor === 'cudy_event') {
        const event = msg.data as CudyEvent;
        setCudyEvents(prev => [event, ...prev].slice(0, 200));
        return;
      }
      setState(prev => ({
        ...prev,
        [msg.sensor]: msg.data,
      }));

      setPulses(prev => ({ ...prev, [msg.sensor]: true }));
      setTimeout(() => {
        setPulses(prev => ({ ...prev, [msg.sensor]: false }));
      }, 1000);
    };

    return () => sse.close();
  }, []);

  const sensorsFresh = !!status?.mqtt_connected
    && !status.sensors?.dht11?.stale
    && !status.sensors?.ldr?.stale;
  const cudyFresh = !!status?.mqtt_connected && !status.sensors?.cudy?.stale;
  const fresh = tab === 'cudy' ? cudyFresh : sensorsFresh;
  const staleLabel = tab === 'cudy' ? 'ROUTER DATA STALE' : 'SENSOR DATA STALE';

  return (
    <div className="dashboard-container">
      <header className="header">
        <h1>pi-mix telemetry</h1>
        <p>{tab === 'cudy' ? 'Cudy LTE router telemetry' : 'Live edge-to-cloud sensor metrics'}</p>

        <nav className="tabs" aria-label="Dashboard sections">
          <button
            type="button"
            className={`tab ${tab === 'sensors' ? 'tab-active' : ''}`}
            aria-pressed={tab === 'sensors'}
            onClick={() => setTab('sensors')}
          >
            <Radio size={16} /> Sensors
          </button>
          <button
            type="button"
            className={`tab ${tab === 'cudy' ? 'tab-active' : ''}`}
            aria-pressed={tab === 'cudy'}
            onClick={() => setTab('cudy')}
          >
            <Router size={16} /> Cudy Router
          </button>
        </nav>

        <div className={`status-badge ${isConnected && fresh ? 'status-live' : 'status-offline'}`}>
          <div className="status-dot"></div>
          {!isConnected ? 'RECONNECTING...' : fresh ? 'STREAMING LIVE' : staleLabel}
        </div>
      </header>

      {tab === 'sensors' ? (
        <>
          <LiveMetricsGrid state={state} pulses={pulses} />
          <HistoryCharts />
          <MessagesPanel />
        </>
      ) : (
        <CudyTab cudy={state.cudy} liveEvents={cudyEvents} />
      )}
    </div>
  );
}

export default App;
