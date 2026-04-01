import React, { useState } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';

export default function App() {
  const [file, setFile] = useState(null);
  const [zipcode, setZipcode] = useState('');
  const [date, setDate] = useState('');
  const [benchmark, setBenchmark] = useState(1000);
  
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState(null);
  const [error, setError] = useState('');

  const handleFileDrop = (e) => {
    e.preventDefault();
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      setFile(e.dataTransfer.files[0]);
    }
  };

  const handleAnalyze = async () => {
    if (!file) return setError('Please upload a Nest Takeout .zip file');
    
    setLoading(true);
    setError('');
    
    const formData = new FormData();
    formData.append('file', file);
    formData.append('zipcode', zipcode);
    formData.append('intervention_date', date);
    formData.append('benchmark', benchmark);

    try {
      const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
      console.log(`[Diagnostic] Calling API at: ${apiUrl}/api/analyze`);
      
      const res = await fetch(`${apiUrl}/api/analyze`, {
        method: 'POST',
        body: formData,
      });
      
      const data = await res.json();
      console.log("[Diagnostic] Full Data Received:", data);
      if (data.error) throw new Error(data.error);
      setResults(data);
    } catch (err) {
      console.log("[Diagnostic] Full Error Object:", err);
      setError(err.message || 'Failed to analyze data');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="dashboard-container">
      <header>
        <h1>Thermal Efficiency Analytics</h1>
        <p style={{color: 'var(--text-secondary)'}}>Empirical diagnostic modeling of your home's envelope</p>
      </header>

      {!results ? (
        <div className="glass-panel" style={{maxWidth: '800px', margin: '0 auto'}}>
          <div className="upload-grid">
            <div className="input-group">
              <label>Zip Code</label>
              <input className="glass-input" value={zipcode} onChange={e => setZipcode(e.target.value)} />
            </div>
            <div className="input-group">
              <label>Intervention Date</label>
              <input type="date" className="glass-input" value={date} onChange={e => setDate(e.target.value)} />
            </div>
            <div className="input-group">
              <label>Cold Month Bill Benchmark ($)</label>
              <input type="number" className="glass-input" value={benchmark} onChange={e => setBenchmark(e.target.value)} />
            </div>
          </div>

          <div 
            className="file-drop-zone"
            onDragOver={e => e.preventDefault()}
            onDrop={handleFileDrop}
            onClick={() => document.getElementById('fileUpload').click()}
          >
            <input 
              id="fileUpload" type="file" style={{display:'none'}} accept=".zip"
              onChange={(e) => setFile(e.target.files[0])} 
            />
            {file ? <p style={{color: 'var(--accent-blue)', fontWeight: 600}}>{file.name}</p> : 
                    <p style={{color: 'var(--text-secondary)'}}>Drag & Drop Nest Takeout .zip here or click to browse</p>}
          </div>

          {error && <p style={{color: 'var(--accent-red)', marginBottom: '15px'}}>{error}</p>}

          <button className="submit-btn" onClick={handleAnalyze} disabled={loading}>
            {loading ? 'Crunching Telemetry...' : 'Generate Optimization Report'}
          </button>
        </div>
      ) : (
        <Dashboard results={results} reset={() => setResults(null)} />
      )}
    </div>
  );
}

function Dashboard({ results, reset }) {
  const { statistics, financials, schedule_recommendations } = results;
  const paretoData = schedule_recommendations.pareto_curve;

  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      return (
        <div className="glass-panel" style={{padding: '10px 15px', borderRadius: '8px', border: '1px solid var(--accent-blue)'}}>
          <p style={{fontWeight:600}}>{`Setpoint: ${label}°F`}</p>
          <p style={{color: 'var(--accent-red)'}}>{`Furnace: ${payload[0].value.toFixed(1)} hrs`}</p>
          <p style={{color: 'var(--accent-blue)'}}>{`Discomfort: ${payload[1].value.toFixed(1)} degree-hrs`}</p>
        </div>
      );
    }
    return null;
  };

  return (
    <>
      <button onClick={reset} style={{background:'transparent', color:'var(--text-secondary)', border:'none', cursor:'pointer', marginBottom:'20px'}}>← Analyze another home</button>
      
      <div className="stats-grid">
        <div className="glass-panel stat-card">
          <div className="stat-label">Efficiency Change</div>
          <div className={`stat-value ${(statistics?.efficiency_degradation_pct || 0) > 0 ? 'loss' : 'gain'}`}>
            {(statistics?.efficiency_degradation_pct || 0) > 0 ? '+' : ''}{statistics?.efficiency_degradation_pct || 0}%
          </div>
          <p style={{color: 'var(--text-secondary)', fontSize: '0.85rem'}}>
            Welch's p-value: <span style={{color: statistics?.is_significant ? 'var(--accent-blue)' : 'inherit'}}>{statistics?.p_value?.toExponential(2) || 'N/A'}</span>
          </p>
        </div>
        
        <div className="glass-panel stat-card">
          <div className="stat-label">Projected Financial Impact</div>
          <div className="stat-value loss">
            +${financials?.estimated_loss_usd || 0}
          </div>
          <p style={{color: 'var(--text-secondary)', fontSize: '0.85rem'}}>Estimated month bill: ${financials?.projected_post_intervention_usd || 0}</p>
        </div>

        <div className="glass-panel stat-card">
          <div className="stat-label">Optimal Wake Setpoint</div>
          <div className="stat-value gain">
            {schedule_recommendations?.optimal_wake_setpoint ?? 'N/A'}°F
          </div>
          <p style={{color: 'var(--text-secondary)', fontSize: '0.85rem'}}>
            Best Drop Setback: {schedule_recommendations?.recommended_overnight_setback ?? 0}°F (saves {(schedule_recommendations?.expected_daily_savings_pct ?? 0).toFixed(1)}%)
          </p>
        </div>
      </div>

      {Array.isArray(paretoData) && paretoData.length > 0 && (
        <div className="glass-panel chart-container">
          <h3 style={{marginBottom: '20px'}}>Pareto Frontier: Heating Cost vs. Comfort</h3>
          <ResponsiveContainer width="100%" height="85%">
            <LineChart data={paretoData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
              <XAxis dataKey="setpoint" stroke="var(--text-secondary)" />
              <YAxis yAxisId="left" stroke="var(--accent-red)" label={{ value: 'Daily Heat Hrs', angle: -90, position: 'insideLeft', fill: 'var(--accent-red)' }} />
              <YAxis yAxisId="right" orientation="right" stroke="var(--accent-blue)" label={{ value: 'Discomfort DH', angle: 90, position: 'insideRight', fill: 'var(--accent-blue)' }} />
              <Tooltip content={<CustomTooltip />} />
              {typeof schedule_recommendations?.optimal_wake_setpoint === 'number' && (
                <ReferenceLine x={schedule_recommendations.optimal_wake_setpoint} stroke="var(--success-green)" strokeDasharray="3 3" label={{position: 'top', value: 'Optimal', fill: 'var(--success-green)'}} />
              )}
              <Line yAxisId="left" type="monotone" dataKey="daily_heat_hrs" stroke="var(--accent-red)" strokeWidth={3} dot={false} isAnimationActive={false} />
              <Line yAxisId="right" type="monotone" dataKey="discomfort_dh" stroke="var(--accent-blue)" strokeWidth={3} dot={false} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </>
  );
}
