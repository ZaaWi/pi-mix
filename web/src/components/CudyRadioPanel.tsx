import React, { useMemo } from 'react';
import { Layers, Radio, Signal } from 'lucide-react';
import { CudyCellular, CudyEvent, CudyInterfaceStats } from '../types';
import {
  RSRP_SPEC,
  RSRQ_SPEC,
  SINR_SPEC,
  SignalSpec,
  carrierMhz,
  clamp,
  decodeCellId,
  fmtDuration,
  signalState,
} from '../lib/cudy';

interface CudyRadioPanelProps {
  cellular?: CudyCellular;
  interfaces?: { ts?: number; [name: string]: CudyInterfaceStats | number | undefined };
  events: CudyEvent[];
}

const SignalGauge: React.FC<{ spec: SignalSpec; value?: number | null }> = ({ spec, value }) => {
  const state = signalState(value, spec);
  return (
    <div className="signal-gauge">
      <div className="signal-gauge-head">
        <span className="usage-label">{spec.label}</span>
        <span className="signal-quality" style={{ color: state.color }}>{state.label}</span>
      </div>
      <div className="signal-value">
        {value ?? '--'} <span>{spec.unit}</span>
      </div>
      <div className="signal-bar">
        <div className="signal-fill" style={{ width: `${state.pct}%`, background: state.color }} />
      </div>
      <div className="signal-scale">
        <span>{spec.min}</span>
        <span>{spec.max}</span>
      </div>
    </div>
  );
};

const normalizeCarrier = (text?: string | null): string => (text || '').replace(/\s+/g, ' ').trim();

export const CudyRadioPanel: React.FC<CudyRadioPanelProps> = ({ cellular, interfaces, events }) => {
  const decoded = decodeCellId(cellular?.cell_id, cellular?.cell_id_num);
  const sccList = useMemo(() => {
    const list = cellular?.scc_list?.length ? cellular.scc_list : cellular?.scc ? [cellular.scc] : [];
    return list.map(normalizeCarrier).filter(Boolean);
  }, [cellular?.scc, cellular?.scc_list]);

  const carriers = useMemo(
    () => [normalizeCarrier(cellular?.pcc), ...sccList].filter(Boolean),
    [cellular?.pcc, sccList],
  );
  const bandwidth = useMemo(
    () => carriers.reduce((sum, carrier) => sum + (carrierMhz(carrier) ?? 0), 0),
    [carriers],
  );
  const caCount = cellular?.ca_count ?? (carriers.length > 0 ? carriers.length : 0);

  const cellSince = useMemo(() => {
    const now = Math.floor(Date.now() / 1000);
    const last = events
      .filter((event) => event.kind === 'cell_change' && typeof event.ts === 'number')
      .sort((a, b) => Number(b.ts) - Number(a.ts))[0];
    if (last?.ts) return Number(last.ts);
    if (cellular?.connected_seconds) return now - cellular.connected_seconds;
    return 0;
  }, [events, cellular?.connected_seconds]);

  const health = useMemo(() => {
    let penalty = 0;
    const rsrp = cellular?.rsrp;
    const rsrq = cellular?.rsrq;
    const sinr = cellular?.sinr;
    if (rsrp !== undefined) penalty += rsrp >= -80 ? 0 : rsrp >= -90 ? 10 : rsrp >= -100 ? 20 : rsrp >= -110 ? 35 : 50;
    if (rsrq !== undefined) penalty += rsrq >= -10 ? 0 : rsrq >= -15 ? 5 : rsrq >= -20 ? 10 : 20;
    if (sinr !== undefined) penalty += sinr >= 20 ? 0 : sinr >= 13 ? 5 : sinr >= 0 ? 15 : 30;
    penalty += caCount >= 2 ? 0 : caCount === 1 ? 10 : 20;

    const cellStats = interfaces?.Cellular;
    if (cellStats && typeof cellStats === 'object') {
      const rxpkts = cellStats.rxpkts ?? 0;
      const drops = cellStats.rxdrop ?? 0;
      if (rxpkts > 0) {
        const pct = (drops / rxpkts) * 100;
        penalty += pct > 0.1 ? 20 : pct > 0.01 ? 10 : 0;
      }
    }

    const score = clamp(100 - penalty, 0, 100);
    const label = score >= 85 ? 'Excellent' : score >= 70 ? 'Good' : score >= 50 ? 'Fair' : 'Poor';
    const color = score >= 85 ? '#34d399' : score >= 70 ? '#38bdf8' : score >= 50 ? '#fbbf24' : '#f87171';
    return { score, label, color };
  }, [cellular?.rsrp, cellular?.rsrq, cellular?.sinr, caCount, interfaces?.Cellular]);

  return (
    <section className="glass glass-panel cudy-section">
      <div className="cudy-section-head">
        <div className="cudy-section-title">
          <Signal color="#38bdf8" />
          Radio signal
        </div>
        <div className="health-badge" style={{ borderColor: health.color, color: health.color }}>
          Link health {health.score}/100 · {health.label}
        </div>
      </div>

      <div className="signal-grid">
        <SignalGauge spec={RSRP_SPEC} value={cellular?.rsrp} />
        <SignalGauge spec={RSRQ_SPEC} value={cellular?.rsrq} />
        <SignalGauge spec={SINR_SPEC} value={cellular?.sinr} />
      </div>

      <div className="cudy-subsection">
        <div className="cudy-section-head">
          <div className="cudy-section-title cudy-subtitle">
            <Radio color="#a78bfa" />
            Serving cell & carrier aggregation
          </div>
          <div className="cell-badges">
            <span className="badge badge-ca"><Layers size={12} /> {caCount > 0 ? `${caCount}CA` : 'CA --'}</span>
            {bandwidth > 0 && <span className="badge">{bandwidth} MHz</span>}
            <span className={`badge ${cellular?.connected ? 'badge-good' : 'badge-bad'}`}>
              {cellular?.connected ? 'Connected' : 'Offline'}
            </span>
          </div>
        </div>

        <dl className="info-grid">
          <div className="info-row"><dt>Serving cell</dt><dd className="mono">{cellular?.cell_id || '--'}</dd></div>
          <div className="info-row">
            <dt>Decoded eNB / sector</dt>
            <dd className="mono">{decoded ? `${decoded.enb} / ${decoded.sector}` : '--'}</dd>
          </div>
          <div className="info-row"><dt>PCI</dt><dd>{cellular?.pcid ?? '--'}</dd></div>
          <div className="info-row"><dt>Band</dt><dd>{cellular?.band ? `LTE Band ${cellular.band}` : '--'}</dd></div>
          <div className="info-row">
            <dt>Bandwidth DL / UL</dt>
            <dd>{cellular?.dl_bandwidth_mhz ? `${cellular.dl_bandwidth_mhz} / ${cellular.ul_bandwidth_mhz ?? '--'} MHz` : '--'}</dd>
          </div>
          <div className="info-row"><dt>MCC / MNC</dt><dd>{cellular?.mcc || '--'} / {cellular?.mnc || '--'}</dd></div>
          <div className="info-row"><dt>Network / mode</dt><dd>{[cellular?.network_type, cellular?.mode].filter(Boolean).join(' · ') || '--'}</dd></div>
          <div className="info-row"><dt>Connected time</dt><dd>{fmtDuration(cellular?.connected_seconds)}</dd></div>
          <div className="info-row">
            <dt>Time on current cell</dt>
            <dd>{cellSince ? fmtDuration(Math.floor(Date.now() / 1000) - cellSince) : '--'}</dd>
          </div>
          <div className="info-row"><dt>PCC</dt><dd className="mono">{normalizeCarrier(cellular?.pcc) || '--'}</dd></div>
          <div className="info-row">
            <dt>SCC</dt>
            <dd className="mono">{sccList.length > 0 ? sccList.join(' · ') : 'None'}</dd>
          </div>
        </dl>
      </div>
    </section>
  );
};
