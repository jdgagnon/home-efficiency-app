import io
import json
import zipfile
from datetime import datetime
import pandas as pd
import numpy as np
import scipy.stats as stats
import statsmodels.formula.api as smf

import time
import requests
import pgeocode
import gc
from threading import Lock

# --- SYSTEM CONFIGS ---
WEATHER_CACHE = {}
CACHE_LOCK = Lock()
WAKE_START_HOUR = 7
WAKE_END_HOUR = 20
SETPOINT_GRID = np.arange(64, 76.5, 0.5)

# =============================================================================
# MODULE A: DATA INGESTION
# =============================================================================

def parse_nest_jsonl_from_zip(zip_bytes: bytes) -> pd.DataFrame:
    """
    Parses 'HvacRuntime.jsonl' files directly from a zip file in memory.
    Optimized for memory by only keeping required columns and using float32.
    """
    REQUIRED_COLS = {
        'interval_start', 'heating_time', 'cooling_time', 
        'indoor_temp', 'outdoor_temp', 'heating_target', 'cooling_target'
    }
    
    all_data = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        jsonl_files = [m for m in z.namelist() if m.endswith('HvacRuntime.jsonl')]
        
        if not jsonl_files:
            raise FileNotFoundError("No HvacRuntime.jsonl files found in the provided zip.")
            
        for filepath in jsonl_files:
            with z.open(filepath) as f:
                for line in f:
                    try:
                        decoded_line = line.decode('utf-8').strip()
                        if decoded_line.startswith('"') and decoded_line.endswith('"'):
                            decoded_line = decoded_line[1:-1].replace('\\"', '"').replace('""', '"')
                        
                        raw_json = json.loads(decoded_line)
                        payload = raw_json.get("value", raw_json) if isinstance(raw_json, dict) else json.loads(raw_json)
                        if isinstance(payload, str): 
                            payload = json.loads(payload)
                        
                        # Only keep essential columns to save RAM
                        filtered_payload = {k: payload[k] for k in REQUIRED_COLS if k in payload}
                        all_data.append(filtered_payload)
                    except Exception:
                        continue

    df = pd.DataFrame(all_data)
    del all_data
    gc.collect()
    
    # Find the timestamp column
    ts_col = next((c for c in ['interval_start', 'hourly_start', 'start_time'] if c in df.columns), None)
    
    if ts_col:
        df['ts'] = pd.to_datetime(df[ts_col], unit='s', utc=True, errors='coerce').dt.tz_convert('America/New_York')
        df['date'] = df['ts'].dt.date
        df['hour'] = df['ts'].dt.hour.astype(np.int8)
        # Drop raw timestamp columns
        cols_to_drop = [c for c in ['interval_start', 'hourly_start', 'start_time', 'ts'] if c in df.columns]
        df.drop(columns=cols_to_drop, inplace=True, errors='ignore')
        
    num_cols = ['heating_time', 'cooling_time', 'indoor_temp', 'outdoor_temp', 
                'heating_target', 'cooling_target']
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').astype(np.float32)
            
    if 'heating_time' in df.columns:
        df['heating_hrs'] = (df['heating_time'] / 3600.0).astype(np.float32)
    if 'cooling_time' in df.columns:
        df['cooling_hrs'] = (df['cooling_time'] / 3600.0).astype(np.float32)
        
    temp_cols = ['indoor_temp', 'outdoor_temp', 'heating_target', 'cooling_target']
    for t in temp_cols:
        if t in df.columns:
            df[f'{t}_f'] = ((df[t] * 9/5) + 32).astype(np.float32)
    
    # Drop raw runtime seconds to free space
    df.drop(columns=['heating_time', 'cooling_time', 'indoor_temp', 'outdoor_temp', 
                    'heating_target', 'cooling_target'], inplace=True)
            
    return df

def fetch_weather_by_zip(zipcode: str, start_date: str, end_date: str) -> pd.DataFrame:
    cache_key = f"{zipcode}_{start_date}_{end_date}"
    
    with CACHE_LOCK:
        if cache_key in WEATHER_CACHE:
            print(f"[Diagnostic] Cache HIT for Zip {zipcode}")
            return WEATHER_CACHE[cache_key]

    nomi = pgeocode.Nominatim('us')
    loc = nomi.query_postal_code(zipcode)
    
    if pd.isna(loc.latitude):
        raise ValueError(f"Invalid or unrecognized US Zip Code: {zipcode}")
        
    print(f"[Diagnostic] Switched to Open-Meteo. Fetching weather for: {loc.latitude}, {loc.longitude}")
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": float(loc.latitude),
        "longitude": float(loc.longitude),
        "start_date": start_date,
        "end_date": end_date,
        "daily": ["temperature_2m_mean", "wind_speed_10m_max"],
        "timezone": "America/New_York"
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=15)
            
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", 5 * (attempt + 1)))
                print(f"[Diagnostic] Rate limited (429). Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            data = response.json()
            
            if "daily" not in data:
                raise ValueError("Open-Meteo API returned no daily data.")
                
            daily = data["daily"]
            weather_df = pd.DataFrame({
                "date": pd.to_datetime(daily["time"]).date,
                "avg_out_temp_weather": ((pd.Series(daily["temperature_2m_mean"]) * 9/5) + 32).astype(np.float32),
                "avg_wind_mph": (pd.Series(daily["wind_speed_10m_max"]) * 0.621371).astype(np.float32)
            })
            
            weather_df.dropna(subset=['avg_out_temp_weather'], inplace=True)
            
            print(f"[Diagnostic] Open-Meteo fetch successful. Rows: {len(weather_df)}")
            
            with CACHE_LOCK:
                WEATHER_CACHE[cache_key] = weather_df
                # Keep cache small
                if len(WEATHER_CACHE) > 50:
                    WEATHER_CACHE.pop(next(iter(WEATHER_CACHE)))
                    
            return weather_df
            
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"[Diagnostic] Open-Meteo Error: {str(e)}")
                raise ValueError(f"Failed to fetch weather from Open-Meteo after {max_retries} attempts: {str(e)}")
            time.sleep(2)

# =============================================================================
# MODULE B: DATA PROCESSING & MASTER AGGREGATION
# =============================================================================

def build_daily_master(nest_df: pd.DataFrame, weather_df: pd.DataFrame, intervention_date: str) -> pd.DataFrame:
    nest_df['temp_delta'] = (nest_df['indoor_temp_f'] - nest_df['outdoor_temp_f']).astype(np.float32)
    daily_nest = nest_df.groupby('date').agg(
        avg_heating_target_f=('heating_target_f', 'mean'),
        total_heat_hrs=('heating_hrs', 'sum'),
        avg_delta=('temp_delta', 'mean'),
        avg_out_temp=('outdoor_temp_f', 'mean'),
        in_temp_f=('indoor_temp_f', 'mean')
    ).reset_index()
    
    # Cast aggregated results to float32
    for col in daily_nest.columns:
        if daily_nest[col].dtype == np.float64:
            daily_nest[col] = daily_nest[col].astype(np.float32)

    if not weather_df.empty:
        df_master = pd.merge(daily_nest, weather_df, on='date', how='left')
    else:
        df_master = daily_nest.copy()
        
    df_master = df_master[df_master['total_heat_hrs'] <= 24].copy()
    
    inter_date_obj = datetime.strptime(intervention_date, "%Y-%m-%d").date()
    df_master['period'] = np.where(df_master['date'] >= inter_date_obj, "After Intervention", "Before Intervention")
    df_master['leak_ratio'] = np.where(df_master['avg_delta'] > 0, df_master['total_heat_hrs'] / df_master['avg_delta'], np.nan).astype(np.float32)
    
    gc.collect()
    return df_master

# =============================================================================
# MODULE C: DIAGNOSTICS & STATISTICAL MODELING
# =============================================================================

def evaluate_envelope(df_master: pd.DataFrame) -> dict:
    df_heat = df_master[(df_master['avg_delta'] > 5) & (df_master['total_heat_hrs'] > 0)].copy()
    before = df_heat[df_heat['period'] == 'Before Intervention']['leak_ratio'].dropna()
    after = df_heat[df_heat['period'] == 'After Intervention']['leak_ratio'].dropna()
    
    if len(before) < 3 or len(after) < 3:
        return {"error": "Insufficient data"}
        
    before_mean = before.mean()
    after_mean = after.mean()
    pct_change = (after_mean - before_mean) / before_mean
    t_stat, p_value = stats.ttest_ind(after, before, equal_var=False)
    
    return {
        "before_mean_ratio": round(before_mean, 4),
        "after_mean_ratio": round(after_mean, 4),
        "efficiency_degradation_pct": round(pct_change * 100, 2),
        "p_value": p_value,
        "is_significant": p_value < 0.05
    }

# =============================================================================
# MODULE D: SCHEDULE OPTIMIZATION
# =============================================================================

def optimize_thermostat_schedule(interval_df: pd.DataFrame, daily_df: pd.DataFrame, intervention_date: str) -> dict:
    inter_date_obj = datetime.strptime(intervention_date, "%Y-%m-%d").date()
    
    heat_days = daily_df[(daily_df['avg_delta'] > 5) & (daily_df['total_heat_hrs'] > 0)]['date']
    df = interval_df[interval_df['date'].isin(heat_days)].copy()
    
    if df.empty:
        return {"error": "Not enough data"}
        
    df['period'] = np.where(df['date'] >= inter_date_obj, "After Intervention", "Before Intervention")
    df['out_bin'] = (df['outdoor_temp_f'] / 5).round() * 5
    df['time_zone'] = np.where((df['hour'] >= WAKE_START_HOUR) & (df['hour'] < WAKE_END_HOUR), "Wake",
                      np.where((df['hour'] >= 22) | (df['hour'] < 5), "Deep Night", "Shoulder"))
                      
    hourly_df = df.groupby(['date', 'hour', 'out_bin', 'time_zone', 'period']).agg(
        avg_indoor=('indoor_temp_f', 'mean'), heat_frac=('heating_hrs', 'sum')
    ).reset_index()
    hourly_df = hourly_df[hourly_df['heat_frac'] <= 1.0].copy()

    before_hourly = hourly_df[hourly_df['period'] == 'Before Intervention'].copy()
    before_hourly['delta_t'] = before_hourly['avg_indoor'] - before_hourly['out_bin']
    
    if len(before_hourly) < 10:
        return {"error": "Insufficient hourly data"}
        
    heating_rate_model = smf.ols('heat_frac ~ delta_t', data=before_hourly).fit()

    # Pre-calculate comfort for a range of setpoints to avoid inner-loop overhead
    wake_intervals = df[df['time_zone'] == "Wake"].copy()
    wake_in_temp = wake_intervals['indoor_temp_f'].values
    wake_dates = wake_intervals['date'].values
    unique_dates = np.unique(wake_dates)
    date_map = {d: i for i, d in enumerate(unique_dates)}
    date_indices = np.array([date_map[d] for d in wake_dates])
    
    def compute_comfort(setpoint):
        discomfort = np.maximum(0, wake_in_temp - setpoint) * (5/60.0)
        # Sum by date using numpy for speed and memory
        daily_sums = np.zeros(len(unique_dates), dtype=np.float32)
        np.add.at(daily_sums, date_indices, discomfort)
        return daily_sums.mean()

    wake_hours_per_day = WAKE_END_HOUR - WAKE_START_HOUR
    typical_out_temp = hourly_df['out_bin'].median()

    pareto_results = []
    for sp in SETPOINT_GRID:
        pred_data = pd.DataFrame({'delta_t': [sp - typical_out_temp]})
        hf = max(0, min(1, heating_rate_model.predict(pred_data).iloc[0]))
        pareto_results.append({
            'setpoint': float(sp),
            'daily_heat_hrs': float(hf * wake_hours_per_day),
            'discomfort_dh': float(compute_comfort(sp))
        })
        
    pareto_df = pd.DataFrame(pareto_results)
    
    # Cleanup heavy objects before finalizing
    del df, hourly_df, before_hourly, wake_intervals, wake_in_temp
    gc.collect()
    
    min_cost = pareto_df['daily_heat_hrs'].min()
    max_cost = pareto_df['daily_heat_hrs'].max()
    min_disc = pareto_df['discomfort_dh'].min()
    max_disc = pareto_df['discomfort_dh'].max()
    
    pareto_df['norm_cost'] = (pareto_df['daily_heat_hrs'] - min_cost) / (max_cost - min_cost + 1e-9)
    pareto_df['norm_disc'] = (pareto_df['discomfort_dh'] - min_disc) / (max_disc - min_disc + 1e-9)
    pareto_df['dist'] = pareto_df['norm_cost']**2 + pareto_df['norm_disc']**2
    
    optimal_row = pareto_df.loc[pareto_df['dist'].idxmin()]
    optimal_wake_setpoint = optimal_row['setpoint']
    
    return {
        "optimal_wake_setpoint": float(optimal_wake_setpoint),
        "pareto_curve": pareto_df[['setpoint', 'daily_heat_hrs', 'discomfort_dh']].to_dict(orient='records')
    }
