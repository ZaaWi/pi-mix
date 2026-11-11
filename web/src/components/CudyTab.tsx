import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity,
  Database,
  Gauge,
  Globe,
  MonitorSmartphone,
  Router,
  Signal,
  Users,
} from 'lucide-react';
import { SensorCard } from './SensorCard';
import { HistoryCharts, MetricConfig } from './HistoryCharts';
import { CudyClient, CudyInterfaceStats, CudyState } from '../types';

interface CudyTabProps {
  cudy?: CudyState;
}

const KB = 1024;
const MB = KB * 1024;
const GB = MB * 1024;
const TB = GB * 1024;

const fmtBytes = (value?: number | null): string => {
  if (value === undefined || value === null || !Number.isFinite(value)) return '--';
  const abs = Math.abs(value);
  if (abs >= TB) return `${(value / TB).toFixed(2)} TB`;
  if (abs >= GB) return `${(value / GB).toFixed(2)} GB`;
  if (abs >= MB) return `${(value / MB).toFixed(1)} MB`;
  if (abs >= KB) return `${(value / KB).toFixed(1)} KB`;
  return `${value.toFixed(0)} B`;
};

const fmtRate = (value?: number | null): string => {
  if (value === undefined || value === null || !Number.isFinite(value)) return '--';
  return `${fmtBytes(value)}/s`;
};

const fmtKbps = (value?: number | null): string => {
  if (value === undefined || value === null || !Number.isFinite(value)) return '--';
  if (value >= 1000) return `${(value / 1000).toFixed(2)} Mbps`;
  return `${value.toFixed(1)} kbps`;
};

const fmtCount = (value?: number | null): string => {
  if (value === undefined || value === null || !Number.isFinite(value)) return '--';
  return value.toLocaleString();
};

const fmtDuration = (seconds?: number | null): string => {
  if (!seconds || seconds <= 0) return '--';
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
};

// Metric groups shown in the shared history chart. Counter series use
// agg="last" + rate so the chart plots bytes/s instead of a bucket average.
const CUDY_METRICS: Record<string, MetricConfig> = {
  'sensor_cudy_rsrp': { label: 'RSRP', color: '#38bdf8', unit: 'dBm', group: 'Signal' },
  'sensor_cudy_rsrq': { label: 'RSRQ', color: '#818cf8', unit: 'dB', group: 'Signal' },
  'sensor_cudy_sinr': { label: 'SINR', color: '#a78bfa', unit: 'dB', group: 'Signal' },
  'sensor_cudy_rssi': { label: 'RSSI', color: '#c084fc', unit: 'dBm', group: 'Signal' },

  'sensor_cudy_band': { label: 'Band', color: '#fbbf24', agg: 'max', step: true, group: 'Link' },
  'sensor_cudy_connected': { label: 'Connected (1/0)', color: '#34d399', agg: 'max', step: true, group: 'Link' },
  'sensor_cudy_dl_bandwidth': { label: 'Download bandwidth', color: '#22d3ee', unit: 'MHz', agg: 'max', group: 'Link' },
  'sensor_cudy_ul_bandwidth': { label: 'Upload bandwidth', color: '#2dd4bf', unit: 'MHz', agg: 'max', group: 'Link' },

  'sensor_cudy_clients_count': { label: 'Connected clients', color: '#f472b6', agg: 'max', group: 'Clients' },

  'sensor_cudy_cellular_rx_bytes': { label: 'Cellular download', color: '#38bdf8', agg: 'last', rate: true, unit: 'B/s', format: fmtRate, group: 'Traffic' },
  'sensor_cudy_cellular_tx_bytes': { label: 'Cellular upload', color: '#818cf8', agg: 'last', rate: true, unit: 'B/s', format: fmtRate, group: 'Traffic' },
  'sensor_cudy_lan_rx_bytes': { label: 'LAN download', color: '#34d399', agg: 'last', rate: true, unit: 'B/s', format: fmtRate, group: 'Traffic' },
  'sensor_cudy_lan_tx_bytes': { label: 'LAN upload', color: '#fbbf24', agg: 'last', rate: true, unit: 'B/s', format: fmtRate, group: 'Traffic' },
  'sensor_cudy_wn001_rx_bytes': { label: 'WAN download', color: '#22d3ee', agg: 'last', rate: true, unit: 'B/s', format: fmtRate, group: 'Traffic' },
  'sensor_cudy_wn001_tx_bytes': { label: 'WAN upload', color: '#f472b6', agg: 'last', rate: true, unit: 'B/s', format: fmtRate, group: 'Traffic' },

  'sensor_cudy_current_bytes': { label: 'Session traffic', color: '#fbbf24', agg: 'last', unit: 'B', format: fmtBytes, group: 'Usage' },
  'sensor_cudy_monthly_bytes': { label: 'Monthly traffic', color: '#38bdf8', agg: 'last', unit: 'B', format: fmtBytes, group: 'Usage' },
  'sensor_cudy_total_bytes': { label: 'Total traffic', color: '#a78bfa', agg: 'last', unit: 'B', format: fmtBytes, group: 'Usage' },
};

const interfaceLabel = (name: string): string => {
  if (name === '~${WN001}') return 'WAN';
  if (name === '1') return 'WAN (alt)';
  return name;
};

const clientType = (client: CudyClient): string => {
  if (client.conn_type) return client.conn_type;
  return client.internet ? 'Online' : 'Unknown';
};

export const CudyTab: React.FC<CudyTabProps> = ({ cudy }) => {
  const cellular = cudy?.cellular || {};
  const usage = cudy?.usage || {};
  const usageConfig = cudy?.usage_config || {};
  const clientsState = cudy?.clients;
  const clients = useMemo(() => clientsState?.clients || [], [clientsState]);
  const system = cudy?.system as { firmware?: string; local_time?: string; uptime_seconds?: number } | undefined;
  const mesh = cudy?.mesh as { device_name?: string; mesh_units?: string; role?: string } | undefined;
  const meshNodes = cudy?.mesh_nodes as { nodes?: unknown[]; count?: number } | undefined;
  const lan = cudy?.lan as { ip?: string; mac?: string } | undefined;
  const wireless = cudy?.wireless as { ssid?: string; channel?: string } | undefined;

  const interfaces = cudy?.interfaces;
  const ifaceTs = typeof interfaces?.ts === 'number' ? interfaces.ts : 0;
  const ifaceEntries = useMemo(() => {
    const entries: [string, CudyInterfaceStats][] = [];
    if (interfaces) {
      for (const [name, stats] of Object.entries(interfaces)) {
        if (name === 'ts' || typeof stats !== 'object' || stats === null) continue;
        entries.push([name, stats as CudyInterfaceStats]);
      }
    }
    return entries.sort((a, b) => interfaceLabel(a[0]).localeCompare(interfaceLabel(b[0])));
  }, [interfaces]);

  // Live throughput derived from consecutive retained interface snapshots.
  const prevSnapshot = useRef<{ ts: number; stats: Record<string, CudyInterfaceStats> } | null>(null);
  const [ifaceRates, setIfaceRates] = useState<Record<string, { rx: number; tx: number }>>({});

  useEffect(() => {
    if (!ifaceTs || ifaceEntries.length === 0) return;
    const prev = prevSnapshot.current;
    if (prev && ifaceTs > prev.ts) {
      const dt = ifaceTs - prev.ts;
      const next: Record<string, { rx: number; tx: number }> = {};
      for (const [name, stats] of ifaceEntries) {
        const before = prev.stats[name];
        if (!before) continue;
        const rx = (stats.rxbytes ?? 0) - (before.rxbytes ?? 0);
        const tx = (stats.txbytes ?? 0) - (before.txbytes ?? 0);
        // A negative delta means the counter reset; report zero instead.
        next[name] = { rx: rx >= 0 ? rx / dt : 0, tx: tx >= 0 ? tx / dt : 0 };
      }
      setIfaceRates(next);
    }
    const snapshot: Record<string, CudyInterfaceStats> = {};
    for (const [name, stats] of ifaceEntries) snapshot[name] = stats;
    prevSnapshot.current = { ts: ifaceTs, stats: snapshot };
  }, [ifaceTs, ifaceEntries]);

  const wired = clients.filter(c => (c.conn_type || '').toLowerCase().includes('wired')).length;
  const wirelessClients = clients.length - wired;
  const clientTotal = clientsState?.total_clients ?? clientsState?.count ?? clients.length;
  const monthlyLimit = Number(usageConfig.monthly_limit || 0);
  const monthlyAllowance = usageConfig.monthly_allowance
    ? `${usageConfig.monthly_allowance} ${usageConfig.monthly_allowance_unit || ''}`.trim()
    : '--';

  return (
    <div className="cudy-tab">
      <div className="grid">
        <SensorCard
          title="4G Link"
          icon={<Signal color={cellular.connected ? '#34d399' : '#f87171'} />}
          value={cellular.connected ? 'Online' : 'Offline'}
          unit=""
          isPulse={false}
          subText={[cellular.network_type, cellular.band ? `Band ${cellular.band}` : ''].filter(Boolean).join(' · ') || 'Waiting for router state'}
        />
        <SensorCard
          title="Signal"
          icon={<Activity color="#38bdf8" />}
          value={cellular.rsrp ?? '--'}
          unit="dBm"
          isPulse={false}
          subText={`RSRQ ${cellular.rsrq ?? '--'} dB · SINR ${cellular.sinr ?? '--'} dB`}
        />
        <SensorCard
          title="Clients"
          icon={<Users color="#f472b6" />}
          value={fmtCount(clientTotal)}
          unit=""
          isPulse={false}
          subText={`${wired} wired · ${wirelessClients} Wi-Fi`}
        />
        <SensorCard
          title="Public IP"
          icon={<Globe color="#22d3ee" />}
          value={cellular.public_ip || '--'}
          unit=""
          isPulse={false}
          compact
          subText={lan?.ip ? `LAN ${lan.ip}` : `Mode ${cellular.mode ?? '--'}`}
        />
        <SensorCard
          title="Bandwidth"
          icon={<Gauge color="#fbbf24" />}
          value={cellular.dl_bandwidth_mhz ? `${cellular.dl_bandwidth_mhz} / ${cellular.ul_bandwidth_mhz ?? '--'}` : '--'}
          unit="MHz"
          isPulse={false}
          subText={`Mode ${cellular.mode ?? '--'} · PCC ${cellular.pcc ?? '--'}`}
        />
        <SensorCard
          title="Uptime"
          icon={<Router color="#a78bfa" />}
          value={fmtDuration(system?.uptime_seconds)}
          unit=""
          isPulse={false}
          subText={system?.firmware ? `Firmware ${system.firmware}` : 'Waiting for system state'}
        />
      </div>

      <section className="glass glass-panel cudy-section">
        <div className="cudy-section-head">
          <div className="cudy-section-title">
            <Database color="#38bdf8" />
            Data usage
          </div>
          {usage.ts && <span className="cudy-muted">updated {new Date(usage.ts * 1000).toLocaleTimeString()}</span>}
        </div>
        <div className="usage-grid">
          <div className="usage-item">
            <span className="usage-label">This session</span>
            <span className="usage-value">{fmtBytes(usage.current_bytes)}</span>
          </div>
          <div className="usage-item">
            <span className="usage-label">This month</span>
            <span className="usage-value">{fmtBytes(usage.monthly_bytes)}</span>
          </div>
          <div className="usage-item">
            <span className="usage-label">Total</span>
            <span className="usage-value">{fmtBytes(usage.total_bytes)}</span>
          </div>
          <div className="usage-item">
            <span className="usage-label">Monthly limit</span>
            <span className="usage-value">{monthlyLimit > 0 ? `${monthlyLimit} ${usageConfig.monthly_allowance_unit || ''}`.trim() : 'No limit'}</span>
          </div>
          <div className="usage-item">
            <span className="usage-label">Monthly allowance</span>
            <span className="usage-value">{monthlyAllowance}</span>
          </div>
          <div className="usage-item">
            <span className="usage-label">SIM slot / alert</span>
            <span className="usage-value">
              {usageConfig.sim_slot ?? '--'} {usageConfig.alert_pct ? `· ${usageConfig.alert_pct}%` : ''}
            </span>
          </div>
        </div>
      </section>

      <section className="glass glass-panel cudy-section">
        <div className="cudy-section-head">
          <div className="cudy-section-title">
            <MonitorSmartphone color="#f472b6" />
            Connected clients
            <span className="cudy-count">{clients.length}</span>
          </div>
          {clientsState?.truncated && (
            <span className="cudy-muted">
              showing {clients.length} of {clientTotal} (payload capped)
            </span>
          )}
        </div>
        {clients.length > 0 ? (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Hostname</th>
                  <th>IP</th>
                  <th>MAC</th>
                  <th>Connection</th>
                  <th className="num">Down</th>
                  <th className="num">Up</th>
                  <th className="num">Signal</th>
                  <th className="num">Online</th>
                </tr>
              </thead>
              <tbody>
                {clients.map((client, i) => (
                  <tr key={client.mac || client.ip || i}>
                    <td>{client.hostname || 'Unknown'}</td>
                    <td className="mono">{client.ip || '--'}</td>
                    <td className="mono">{client.mac || '--'}</td>
                    <td>{clientType(client)}</td>
                    <td className="num">{fmtKbps(client.down_kbps)}</td>
                    <td className="num">{fmtKbps(client.up_kbps)}</td>
                    <td className="num">{client.signal !== null && client.signal !== undefined ? `${client.signal}%` : '--'}</td>
                    <td className="num">{fmtDuration(client.online_seconds)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="cudy-empty">No client snapshot yet.</div>
        )}
      </section>

      <section className="glass glass-panel cudy-section">
        <div className="cudy-section-head">
          <div className="cudy-section-title">
            <Router color="#a78bfa" />
            Network interfaces
          </div>
          <span className="cudy-muted">cumulative counters · live rate from consecutive snapshots</span>
        </div>
        {ifaceEntries.length > 0 ? (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Interface</th>
                  <th className="num">RX</th>
                  <th className="num">TX</th>
                  <th className="num">RX pkts</th>
                  <th className="num">TX pkts</th>
                  <th className="num">Errors / drops</th>
                  <th className="num">RX/s</th>
                  <th className="num">TX/s</th>
                </tr>
              </thead>
              <tbody>
                {ifaceEntries.map(([name, stats]) => (
                  <tr key={name}>
                    <td>{interfaceLabel(name)}</td>
                    <td className="num">{fmtBytes(stats.rxbytes)}</td>
                    <td className="num">{fmtBytes(stats.txbytes)}</td>
                    <td className="num">{fmtCount(stats.rxpkts)}</td>
                    <td className="num">{fmtCount(stats.txpkts)}</td>
                    <td className="num">
                      {fmtCount((stats.rxerrs ?? 0) + (stats.txerrs ?? 0) + (stats.rxdrop ?? 0) + (stats.txdrop ?? 0))}
                    </td>
                    <td className="num">{fmtRate(ifaceRates[name]?.rx)}</td>
                    <td className="num">{fmtRate(ifaceRates[name]?.tx)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="cudy-empty">No interface snapshot yet.</div>
        )}
      </section>

      <section className="glass glass-panel cudy-section">
        <div className="cudy-section-head">
          <div className="cudy-section-title">
            <Router color="#34d399" />
            Router details
          </div>
          {system?.local_time && <span className="cudy-muted">router time {system.local_time}</span>}
        </div>
        <dl className="info-grid">
          <div className="info-row"><dt>Device</dt><dd>{mesh?.device_name || '--'}</dd></div>
          <div className="info-row"><dt>Role</dt><dd>{mesh?.role || '--'}</dd></div>
          <div className="info-row"><dt>Mesh units</dt><dd>{mesh?.mesh_units || '--'}</dd></div>
          <div className="info-row"><dt>Mesh nodes</dt><dd>{fmtCount(meshNodes?.count ?? meshNodes?.nodes?.length)}</dd></div>
          <div className="info-row"><dt>SSID</dt><dd>{wireless?.ssid || '--'}</dd></div>
          <div className="info-row"><dt>Wi-Fi channel</dt><dd>{wireless?.channel || '--'}</dd></div>
          <div className="info-row"><dt>LAN IP</dt><dd className="mono">{lan?.ip || '--'}</dd></div>
          <div className="info-row"><dt>LAN MAC</dt><dd className="mono">{lan?.mac || '--'}</dd></div>
          <div className="info-row"><dt>Firmware</dt><dd>{system?.firmware || '--'}</dd></div>
          <div className="info-row"><dt>WAN status</dt><dd>{cellular.network_type || '--'}</dd></div>
        </dl>
      </section>

      <HistoryCharts
        metrics={CUDY_METRICS}
        defaultMetric="sensor_cudy_rsrp"
        title="Cudy Router History"
      />
    </div>
  );
};
