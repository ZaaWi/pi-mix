import React, { useEffect, useMemo, useState } from 'react';
import { Area, AreaChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { Flame, MonitorSmartphone } from 'lucide-react';
import { CudyClient, CudyClientHistory } from '../types';
import { fmtDuration, fmtKbps } from '../lib/cudy';

interface CudyClientsPanelProps {
  clients: CudyClient[];
  totalClients?: number;
  truncated?: boolean;
}

type RateField = 'down_kbps' | 'up_kbps';

interface HostInfo {
  key: string;
  /** Sanitized dataKey: recharts treats dots in a string key as a path. */
  chartKey: string;
  name: string;
  mac?: string;
  ip?: string;
  color: string;
  peak: number;
}

const COLORS = ['#38bdf8', '#a78bfa', '#34d399', '#fbbf24', '#f472b6', '#22d3ee', '#fb7185', '#c084fc'];

const hostKey = (client: CudyClient): string => client.mac || client.ip || client.hostname || 'unknown';
const hostName = (client: CudyClient): string => client.hostname || client.ip || client.mac || 'Unknown';
const chartKey = (key: string): string => `host_${key.replace(/[^a-zA-Z0-9]/g, '_')}`;

const chartTime = (ts: number, windowSeconds: number): string => {
  const date = new Date(ts * 1000);
  const time = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  if (windowSeconds > 24 * 3600) {
    return `${date.toLocaleDateString([], { month: 'short', day: 'numeric' })} ${time}`;
  }
  return time;
};

const rate = (client: CudyClient, field: RateField): number => {
  const value = client[field];
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
};

export const CudyClientsPanel: React.FC<CudyClientsPanelProps> = ({ clients, totalClients, truncated }) => {
  const [history, setHistory] = useState<CudyClientHistory>({});
  const [metric, setMetric] = useState<RateField>('down_kbps');
  const [error, setError] = useState('');

  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      try {
        const res = await fetch('/api/cudy/clients/history', { signal: controller.signal });
        if (!res.ok) throw new Error(res.status === 503 ? 'Client history unavailable' : 'Client history request failed');
        const json: CudyClientHistory = await res.json();
        if (!controller.signal.aborted) {
          setHistory(json);
          setError('');
        }
      } catch (err) {
        if (!controller.signal.aborted) {
          setError(err instanceof Error ? err.message : 'Client history request failed');
        }
      }
    };
    load();
    const interval = setInterval(load, 30000);
    return () => {
      controller.abort();
      clearInterval(interval);
    };
  }, []);

  const samples = useMemo(() => history.samples || [], [history]);
  const windowSeconds = history.window_seconds || 24 * 3600;

  const hosts = useMemo<HostInfo[]>(() => {
    const byKey = new Map<string, HostInfo>();
    for (const client of clients) {
      const key = hostKey(client);
      if (!byKey.has(key)) {
        byKey.set(key, {
          key,
          chartKey: chartKey(key),
          name: hostName(client),
          mac: client.mac,
          ip: client.ip,
          color: COLORS[byKey.size % COLORS.length],
          peak: 0,
        });
      }
    }
    for (const sample of samples) {
      for (const client of sample.clients || []) {
        const key = hostKey(client);
        const host = byKey.get(key);
        if (!host) continue;
        host.peak = Math.max(host.peak, rate(client, metric));
      }
    }
    const ordered = Array.from(byKey.values()).sort((a, b) => b.peak - a.peak);
    ordered.forEach((host, index) => {
      host.color = COLORS[index % COLORS.length];
    });
    return ordered.slice(0, 7);
  }, [clients, samples, metric]);

  const chartData = useMemo(() => {
    const keys = new Set(hosts.map((host) => host.key));
    return samples.map((sample) => {
      const row: Record<string, number | string> = {
        ts: sample.ts,
        time: chartTime(sample.ts, windowSeconds),
      };
      for (const host of hosts) row[host.chartKey] = 0;
      for (const client of sample.clients || []) {
        const key = hostKey(client);
        if (!keys.has(key)) continue;
        const host = hosts.find((item) => item.key === key);
        if (host) row[host.chartKey] = rate(client, metric);
      }
      return row;
    });
  }, [samples, hosts, metric, windowSeconds]);

  const totalDown = clients.reduce((sum, client) => sum + rate(client, 'down_kbps'), 0);
  const totalUp = clients.reduce((sum, client) => sum + rate(client, 'up_kbps'), 0);

  const ranked = useMemo(() => {
    return clients
      .map((client) => {
        const down = rate(client, 'down_kbps');
        const up = rate(client, 'up_kbps');
        const share = totalDown > 0 ? down / totalDown : 0;
        const hog = (share > 0.5 && down > 2000) || (share > 0.25 && down > 10000);
        return { client, down, up, share, hog };
      })
      .sort((a, b) => b.down - a.down);
  }, [clients, totalDown]);

  return (
    <section className="glass glass-panel cudy-section">
      <div className="cudy-section-head">
        <div className="cudy-section-title">
          <MonitorSmartphone color="#f472b6" />
          Connected clients
          <span className="cudy-count">{clients.length}</span>
        </div>
        <div className="cudy-head-tools">
          <div className="segmented">
            <button
              type="button"
              className={`segmented-btn ${metric === 'down_kbps' ? 'active' : ''}`}
              onClick={() => setMetric('down_kbps')}
            >
              Download
            </button>
            <button
              type="button"
              className={`segmented-btn ${metric === 'up_kbps' ? 'active' : ''}`}
              onClick={() => setMetric('up_kbps')}
            >
              Upload
            </button>
          </div>
          {truncated && (
            <span className="cudy-muted">
              showing {clients.length} of {totalClients ?? clients.length} (payload capped)
            </span>
          )}
        </div>
      </div>

      <div className="client-summary">
        <span className="cudy-muted">
          Live total <strong>{fmtKbps(totalDown)}</strong> down · <strong>{fmtKbps(totalUp)}</strong> up
        </span>
        <span className="cudy-muted">per-host history: {samples.length} samples over 24h</span>
      </div>

      {error ? (
        <div className="cudy-empty">{error}</div>
      ) : chartData.length > 1 ? (
        <div className="client-chart">
          <ResponsiveContainer width="100%" height={240}>
            <AreaChart data={chartData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
              <XAxis dataKey="time" stroke="#94a3b8" tick={{ fontSize: 11 }} minTickGap={24} />
              <YAxis stroke="#94a3b8" tick={{ fontSize: 11 }} tickFormatter={(value) => fmtKbps(Number(value))} width={72} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px', color: '#f8fafc' }}
                formatter={(value: unknown) => fmtKbps(Number(value))}
              />
              <Legend wrapperStyle={{ fontSize: '0.78rem' }} />
              {hosts.map((host) => (
                <Area
                  key={host.key}
                  type="monotone"
                  dataKey={host.chartKey}
                  name={host.name}
                  stackId="clients"
                  stroke={host.color}
                  fill={host.color}
                  fillOpacity={0.18}
                  strokeWidth={2}
                  dot={false}
                  isAnimationActive={false}
                />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div className="cudy-empty">Collecting per-host history — the chart appears after two polls.</div>
      )}

      {ranked.length > 0 ? (
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Hostname</th>
                <th>IP</th>
                <th>Connection</th>
                <th className="num">Down</th>
                <th className="num">Up</th>
                <th className="num">Share</th>
                <th className="num">Online</th>
              </tr>
            </thead>
            <tbody>
              {ranked.map(({ client, down, up, share, hog }) => (
                <tr key={hostKey(client)}>
                  <td>
                    <span className="client-name">
                      {hostName(client)}
                      {hog && (
                        <span className="badge badge-hog" title="Sustained hog: large share and high absolute rate">
                          <Flame size={12} /> Hog
                        </span>
                      )}
                    </span>
                  </td>
                  <td className="mono">{client.ip || '--'}</td>
                  <td>{client.conn_type || (client.internet ? 'Online' : 'Unknown')}</td>
                  <td className="num">{fmtKbps(down)}</td>
                  <td className="num">{fmtKbps(up)}</td>
                  <td className="num">{totalDown > 0 ? `${(share * 100).toFixed(0)}%` : '--'}</td>
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
  );
};
