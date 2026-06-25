import React from 'react';
import { Thermometer, Droplets, Sun, Scale, Radio } from 'lucide-react';
import { SensorCard } from './SensorCard';
import { SensorData } from '../types';

interface LiveMetricsGridProps {
  state: SensorData;
  pulses: Record<string, boolean>;
}

export const LiveMetricsGrid: React.FC<LiveMetricsGridProps> = ({ state, pulses }) => {
  const dht = state.dht11 || {};
  const ldr = state.ldr || {};
  const scale = state.scale || {};
  const ir = state.ir || {};

  return (
    <div className="grid">
      <SensorCard 
        title="Temperature" 
        icon={<Thermometer color="#38bdf8" />} 
        value={dht.temp_c !== undefined ? dht.temp_c.toFixed(1) : '--'} 
        unit="°C" 
        isPulse={!!pulses.dht11} 
      />
      <SensorCard 
        title="Humidity" 
        icon={<Droplets color="#38bdf8" />} 
        value={dht.humidity_pct !== undefined ? dht.humidity_pct : '--'} 
        unit="%" 
        isPulse={!!pulses.dht11} 
      />
      <SensorCard 
        title="Luminosity" 
        icon={<Sun color="#fcd34d" />} 
        value={ldr.analog !== undefined ? ldr.analog : '--'} 
        unit="analog" 
        isPulse={!!pulses.ldr} 
      />
      <SensorCard 
        title="Weight" 
        icon={<Scale color="#f472b6" />} 
        value={scale.weight_kg !== undefined ? scale.weight_kg.toFixed(2) : '--'} 
        unit="kg" 
        isPulse={!!pulses.scale} 
        subText={scale.status ? `Status: ${scale.status}` : undefined}
      />
      <SensorCard 
        title="IR Remote" 
        icon={<Radio color="#a78bfa" />} 
        value={ir.code || '--'} 
        unit="" 
        isPulse={!!pulses.ir} 
        subText={ir.protocol ? `Protocol: ${ir.protocol}` : undefined}
      />
    </div>
  );
};
