import React from 'react';

interface SensorCardProps {
  title: string;
  icon: React.ReactNode;
  value: string | number;
  unit: string;
  isPulse: boolean;
  subText?: string;
  compact?: boolean;
}

export const SensorCard: React.FC<SensorCardProps> = ({ title, icon, value, unit, isPulse, subText, compact }) => {
  return (
    <div className="glass glass-panel sensor-card">
      <div className="sensor-header">
        {icon} {title}
      </div>
      <div className={`sensor-value ${isPulse ? 'pulse' : ''} ${compact ? 'compact' : ''}`}>
        {value} <span className="sensor-unit">{unit}</span>
      </div>
      {subText && (
        <div style={{ fontSize: '0.9rem', color: '#94a3b8', marginTop: '-10px' }}>
          {subText}
        </div>
      )}
    </div>
  );
};
