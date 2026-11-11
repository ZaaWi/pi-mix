import React, { useMemo } from 'react';
import {
  ArrowLeftRight,
  Globe,
  Layers,
  Radio,
  RefreshCw,
  UserMinus,
  UserPlus,
  Wifi,
  WifiOff,
} from 'lucide-react';
import { CudyCellular, CudyEvent } from '../types';
import { fmtAgo, fmtDuration } from '../lib/cudy';

interface CudyTimelineProps {
  events: CudyEvent[];
  cellular?: CudyCellular;
}

const eventTs = (event: CudyEvent): number => (typeof event.ts === 'number' ? event.ts : 0);

const text = (value: unknown): string => {
  if (typeof value === 'string') return value;
  if (typeof value === 'number') return String(value);
  return '';
};

interface EventDisplay {
  icon: React.ReactNode;
  label: string;
  detail: string;
  tone: string;
}

const describe = (event: CudyEvent): EventDisplay => {
  switch (event.kind) {
    case 'connectivity': {
      const up = event.state === 'up';
      return {
        icon: up ? <Wifi size={16} /> : <WifiOff size={16} />,
        label: up ? 'Link up' : 'Link down',
        detail: [text(event.public_ip), text(event.cell_id)].filter(Boolean).join(' · '),
        tone: up ? 'good' : 'bad',
      };
    }
    case 'public_ip':
      return {
        icon: <Globe size={16} />,
        label: 'Public IP changed',
        detail: [text(event.previous), text(event.current)].filter(Boolean).join(' → '),
        tone: 'info',
      };
    case 'cell_change':
      return {
        icon: <Radio size={16} />,
        label: 'Serving cell changed',
        detail: [
          text(event.cell_id),
          event.pcid !== undefined ? `PCI ${text(event.pcid)}` : '',
          event.band !== undefined ? `Band ${text(event.band)}` : '',
          text(event.previous_cell_id) ? `from ${text(event.previous_cell_id)}` : '',
        ].filter(Boolean).join(' · '),
        tone: 'info',
      };
    case 'ca_change':
      return {
        icon: <Layers size={16} />,
        label: 'Carrier aggregation changed',
        detail: [
          event.ca_count !== undefined ? `${text(event.ca_count)} carriers` : '',
          event.scc_count !== undefined ? `${text(event.scc_count)} SCC` : '',
          event.previous_ca_count !== undefined ? `from ${text(event.previous_ca_count)}` : '',
        ].filter(Boolean).join(' · '),
        tone: 'info',
      };
    case 'client_join':
      return {
        icon: <UserPlus size={16} />,
        label: 'Client joined',
        detail: [text(event.hostname), text(event.mac)].filter(Boolean).join(' · '),
        tone: 'good',
      };
    case 'client_leave':
      return {
        icon: <UserMinus size={16} />,
        label: 'Client left',
        detail: [text(event.hostname), text(event.mac)].filter(Boolean).join(' · '),
        tone: 'muted',
      };
    case 'wan_failover':
      return {
        icon: <ArrowLeftRight size={16} />,
        label: 'WAN failover',
        detail: [text(event.previous), text(event.current)].filter(Boolean).join(' → '),
        tone: 'warn',
      };
    default:
      return {
        icon: <RefreshCw size={16} />,
        label: event.kind || 'Event',
        detail: '',
        tone: 'muted',
      };
  }
};

export const CudyTimeline: React.FC<CudyTimelineProps> = ({ events, cellular }) => {
  const now = Math.floor(Date.now() / 1000);
  const connectedSeconds = cellular?.connected_seconds ?? 0;

  const stats = useMemo(() => {
    const reconnects = (windowSeconds: number) =>
      events.filter((event) => event.kind === 'connectivity' && event.state === 'up' && eventTs(event) >= now - windowSeconds).length;
    const lastReconnectEvent = events
      .filter((event) => event.kind === 'connectivity' && event.state === 'up')
      .sort((a, b) => eventTs(b) - eventTs(a))[0];
    const lastIPEvent = events
      .filter((event) => event.kind === 'public_ip')
      .sort((a, b) => eventTs(b) - eventTs(a))[0];
    const lastCellEvent = events
      .filter((event) => event.kind === 'cell_change')
      .sort((a, b) => eventTs(b) - eventTs(a))[0];

    const lastReconnect = lastReconnectEvent ? eventTs(lastReconnectEvent) : connectedSeconds > 0 ? now - connectedSeconds : 0;
    const ipSince = lastIPEvent ? eventTs(lastIPEvent) : connectedSeconds > 0 ? now - connectedSeconds : 0;
    const cellSince = lastCellEvent ? eventTs(lastCellEvent) : connectedSeconds > 0 ? now - connectedSeconds : 0;

    return {
      reconnects24h: reconnects(24 * 3600),
      reconnects7d: reconnects(7 * 24 * 3600),
      reconnects30d: reconnects(30 * 24 * 3600),
      lastReconnect,
      ipSince,
      cellSince,
      hasCellEvent: !!lastCellEvent,
    };
  }, [events, now, connectedSeconds]);

  const recent = useMemo(
    () => [...events].sort((a, b) => eventTs(b) - eventTs(a)).slice(0, 40),
    [events],
  );

  return (
    <section className="glass glass-panel cudy-section">
      <div className="cudy-section-head">
        <div className="cudy-section-title">
          <RefreshCw color="#22d3ee" />
          Link timeline
        </div>
        <span className="cudy-muted">{events.length} persisted events (90d retention)</span>
      </div>

      <div className="timeline-stats">
        <div className="timeline-stat">
          <span className="usage-label">Reconnects 24h</span>
          <span className="usage-value">{stats.reconnects24h}</span>
        </div>
        <div className="timeline-stat">
          <span className="usage-label">Reconnects 7d</span>
          <span className="usage-value">{stats.reconnects7d}</span>
        </div>
        <div className="timeline-stat">
          <span className="usage-label">Reconnects 30d</span>
          <span className="usage-value">{stats.reconnects30d}</span>
        </div>
        <div className="timeline-stat">
          <span className="usage-label">Last reconnect</span>
          <span className="usage-value">{stats.lastReconnect ? fmtAgo(stats.lastReconnect) : '--'}</span>
        </div>
        <div className="timeline-stat">
          <span className="usage-label">Public IP age</span>
          <span className="usage-value">{stats.ipSince ? fmtDuration(now - stats.ipSince) : '--'}</span>
        </div>
        <div className="timeline-stat">
          <span className="usage-label">Time on cell</span>
          <span className="usage-value">{stats.cellSince ? fmtDuration(now - stats.cellSince) : '--'}</span>
          {!stats.hasCellEvent && <span className="usage-hint">estimated from connection time</span>}
        </div>
      </div>

      {recent.length > 0 ? (
        <ul className="event-list">
          {recent.map((event, index) => {
            const display = describe(event);
            return (
              <li key={event.id || `${eventTs(event)}-${index}`} className={`event-item event-${display.tone}`}>
                <span className="event-icon">{display.icon}</span>
                <div className="event-body">
                  <span className="event-label">{display.label}</span>
                  {display.detail && <span className="event-detail">{display.detail}</span>}
                </div>
                <span className="event-time" title={eventTs(event) ? new Date(eventTs(event) * 1000).toLocaleString() : ''}>
                  {fmtAgo(eventTs(event))}
                </span>
              </li>
            );
          })}
        </ul>
      ) : (
        <div className="cudy-empty">No persisted events yet. Link, IP, cell and client transitions will appear here.</div>
      )}
    </section>
  );
};
