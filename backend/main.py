from fastapi import FastAPI, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import gc
from home_efficiency import (
    parse_nest_jsonl_from_zip,
    fetch_weather_by_zip,
    build_daily_master,
    evaluate_envelope,
    optimize_thermostat_schedule
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/analyze")
async def analyze_home(
    zipcode: str = Form(...),
    intervention_date: str = Form(...),
    benchmark: float = Form(...),
    file: UploadFile = Form(...)
):
    try:
        contents = await file.read()
        raw_df = parse_nest_jsonl_from_zip(contents)
        
        min_date = raw_df['date'].min().strftime("%Y-%m-%d")
        max_date = raw_df['date'].max().strftime("%Y-%m-%d")
        
        weather_df = fetch_weather_by_zip(zipcode, min_date, max_date)
        master_df = build_daily_master(raw_df, weather_df, intervention_date)
        
        stats_results = evaluate_envelope(master_df)
        schedule_results = optimize_thermostat_schedule(raw_df, master_df, intervention_date)
        
        pct_change = stats_results.get("efficiency_degradation_pct", 0) / 100.0
        projected_bill = benchmark * (1 + pct_change)
        
        return {
            "metadata": {
                "zip_code": zipcode,
                "intervention_date": intervention_date,
                "data_span_days": int((raw_df['date'].max() - raw_df['date'].min()).days)
            },
            "statistics": stats_results,
            "schedule_recommendations": schedule_results,
            "financials": {
                "baseline_bill_usd": float(benchmark),
                "projected_post_intervention_usd": round(float(projected_bill), 2),
                "estimated_loss_usd": round(float(projected_bill - benchmark), 2)
            }
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        gc.collect()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
