import React, { useEffect, useMemo, useState } from 'react';
import { AlertTriangle, Database } from 'lucide-react';
import { CudyUsage, CudyUsageConfig, CudyUsageSummary } from '../types';
import { cycleStartTs, fmtBytes, fmtDuration, GB, readNumber } from '../lib/cudy';

interface CudyUsagePanelProps {
  usage?: CudyUsage;
  config?: CudyUsageConfig;
}

const CAP_STORAGE_KEY = 'pi-mix.cudy.planCapGb';

const readStoredCap = (): string => {
  try {
    return window.localStorage.getItem(CAP_STORAGE_KEY) || '';
  } catch {
    return '';
  }
};

export const CudyUsagePanel: React.FC<CudyUsagePanelProps> = ({ usage, config }) => {
  const [summary, setSummary] = useState<CudyUsageSummary>({});
  const [capInput, setCapInput] = useState<string>(readStoredCap);
  const [error, setError] = useState('');

  const startDay = useMemo(() => {
    const value = readNumber(config?.start_date);
    return value && value >= 1 && value <= 31 ? value : 1;
  }, [config?.start_date]);
  const cycleStart = useMemo(() => cycleStartTs(startDay), [startDay]);

  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      try {
        const res = await fetch(`/api/cudy/usage?days=7&since=${cycleStart}`, { signal: controller.signal });
        if (!res.ok) throw new Error(res.status === 503 ? 'Usage history unavailable' : 'Usage request failed');
        const json: CudyUsageSummary = await res.json();
        if (!controller.signal.aborted) {
          setSummary(json);
          setError('');
        }
      } catch (err) {
        if (!controller.signal.aborted) {
          setError(err instanceof Error ? err.message : 'Usage request failed');
        }
      }
    };
    load();
    const interval = setInterval(load, 60000);
    return () => {
      controller.abort();
      clearInterval(interval);
    };
  }, [cycleStart]);

  const saveCap = (value: string) => {
    setCapInput(value);
    try {
      if (value.trim() === '') window.localStorage.removeItem(CAP_STORAGE_KEY);
      else window.localStorage.setItem(CAP_STORAGE_KEY, value);
    } catch {
      /* localStorage may be unavailable in private mode */
    }
  };

  const capGb = Number(capInput);
  const capBytes = Number.isFinite(capGb) && capGb > 0 ? capGb * GB : 0;
  const cycle = summary.cycle;
  const burn = summary.burn;
  const drift = summary.drift;

  const currentTotal = readNumber(usage?.total_bytes) ?? 0;
  const sessionBytes = readNumber(usage?.current_bytes) ?? 0;
  const cycleBytes = cycle?.available ? Math.max(0, cycle.delta_bytes ?? 0) : 0;
  const pct = capBytes > 0 ? (cycleBytes / capBytes) * 100 : 0;
  const cycleIncomplete = !!cycle?.available && !!cycle?.start_ts && cycle.start_ts > cycleStart + 3600;
  const perDay = burn?.bytes_per_day ?? 0;
  const projected30d = burn?.projected_30d_bytes ?? 0;

  const alert = pct >= 100 ? 100 : pct >= 90 ? 90 : pct >= 80 ? 80 : 0;
  const remaining = capBytes > 0 ? capBytes - cycleBytes : 0;
  const daysToCap = capBytes > 0 && perDay > 0 ? remaining / perDay : null;

  const monthlyLimitBytes = readNumber(config?.monthly_limit_bytes) ?? readNumber(config?.monthly_limit) ?? 0;
  const allowanceBytes = readNumber(config?.monthly_allowance_bytes);
  const allowanceLabel = allowanceBytes && allowanceBytes > 0
    ? fmtBytes(allowanceBytes)
    : config?.monthly_allowance
      ? `${config.monthly_allowance} ${config.monthly_allowance_unit || ''}`.trim()
      : '--';

  return (
    <section className="glass glass-panel cudy-section">
      <div className="cudy-section-head">
        <div className="cudy-section-title">
          <Database color="#38bdf8" />
          Data usage & burn rate
        </div>
        <span className="cudy-muted">
          {usage?.ts ? `updated ${new Date(usage.ts * 1000).toLocaleTimeString()}` : 'waiting for usage state'}
        </span>
      </div>

      {alert > 0 && (
        <div className={`usage-alert usage-alert-${alert}`}>
          <AlertTriangle size={16} />
          {alert >= 100
            ? 'Plan cap reached for this cycle.'
            : `${alert}% of the dashboard plan cap used this cycle.`}
        </div>
      )}

      <div className="usage-grid">
        <div className="usage-item">
          <span className="usage-label">Since modem connection</span>
          <span className="usage-value">{fmtBytes(sessionBytes)}</span>
          <span className="usage-hint">router “current/monthly” counter (not calendar-scoped)</span>
        </div>
        <div className="usage-item">
          <span className="usage-label">Router total</span>
          <span className="usage-value">{fmtBytes(currentTotal)}</span>
          <span className="usage-hint">lifetime counter</span>
        </div>
        <div className="usage-item">
          <span className="usage-label">This cycle (tracked)</span>
          <span className="usage-value">{cycle?.available ? fmtBytes(cycleBytes) : '--'}</span>
          <span className="usage-hint">
            {cycle?.available
              ? `${cycle.reset ? 'counter reset · ' : ''}since ${new Date((cycle.start_ts || cycleStart) * 1000).toLocaleDateString()}${cycleIncomplete ? ' (history starts late)' : ''}`
              : 'history not available yet'}
          </span>
        </div>
        <div className="usage-item">
          <span className="usage-label">Burn rate (7d)</span>
          <span className="usage-value">{perDay > 0 ? `${fmtBytes(perDay)}/day` : '--'}</span>
          <span className="usage-hint">{burn?.available ? `${burn.samples ?? 0} points · ${burn.source ?? ''}` : 'waiting for history'}</span>
        </div>
        <div className="usage-item">
          <span className="usage-label">Projected 30 days</span>
          <span className="usage-value">{projected30d > 0 ? fmtBytes(projected30d) : '--'}</span>
          <span className="usage-hint">from the same observed delta</span>
        </div>
        <div className="usage-item">
          <span className="usage-label">Router limit</span>
          <span className="usage-value">{monthlyLimitBytes > 0 ? fmtBytes(monthlyLimitBytes) : 'No enforced limit'}</span>
          <span className="usage-hint">
            stats {config?.monthly_stats_enabled ? 'enabled' : 'disabled'} · allowance {allowanceLabel} {!config?.monthly_stats_enabled ? '(inactive)' : ''}
          </span>
        </div>
      </div>

      <div className="plan-cap">
        <label className="plan-cap-label" htmlFor="plan-cap-input">
          Dashboard plan cap
          <input
            id="plan-cap-input"
            type="number"
            min="1"
            step="1"
            inputMode="decimal"
            placeholder="e.g. 1024"
            value={capInput}
            onChange={(event) => saveCap(event.target.value)}
          />
          GB
        </label>
        <span className="cudy-muted">stored in this browser only · router has no enforced quota</span>
      </div>

      {capBytes > 0 && (
        <div className="cap-progress-wrap">
          <div className="cap-progress">
            <div
              className={`cap-progress-fill ${pct >= 100 ? 'over' : pct >= 90 ? 'warn' : ''}`}
              style={{ width: `${Math.min(100, Math.max(0, pct))}%` }}
            />
          </div>
          <div className="cap-progress-meta">
            <span>
              {fmtBytes(cycleBytes)} of {fmtBytes(capBytes)} ({pct.toFixed(1)}%)
            </span>
            <span>
              {daysToCap === null
                ? 'projection unavailable'
                : daysToCap <= 0
                  ? 'cap already reached'
                  : `cap reached in ~${fmtDuration(daysToCap * 86400)} at the 7d burn rate`}
            </span>
          </div>
        </div>
      )}

      {drift?.available && (
        <div className="usage-drift">
          <span className="cudy-muted">
            Counter drift: router total {fmtBytes(drift.router_total_bytes)} vs telemetry {fmtBytes(drift.telemetry_total_bytes)}
            {' '}({drift.pct !== undefined ? `${drift.pct.toFixed(2)}%` : '--'})
          </span>
        </div>
      )}

      {error && <div className="cudy-empty">{error}</div>}
    </section>
  );
};
