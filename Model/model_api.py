from fastapi import FastAPI
from fastapi import Header, HTTPException
from pathlib import Path
import sys
import os

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db import session_scope
from src.services.ingestion_service import run_daily, run_weekly
from src.services.inference import InferenceInput, run_investment_analysis, suggest_spend_from_target

app = FastAPI()


def _authorize_jobs(x_api_token: str | None) -> None:
    expected = os.getenv("JOBS_API_TOKEN")
    if not expected:
        return
    if x_api_token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.post("/predict")
def predict(data: dict):
    inp = InferenceInput(
        eg_adm=float(data["eg_adm"]),
        sa_adm=float(data["sa_adm"]),
        ae_adm=float(data["ae_adm"]),
        imdb=float(data["imdb"]),
        elcinema=float(data["elcinema"]),
        letterboxd_rating=float(data["letterboxd_rating"]),
        letterboxd_votes=int(data["letterboxd_votes"]),
        stability=float(data["stability"]),
        marketing_spend=float(data["marketing_spend"]),
    )
    return run_investment_analysis(inp)


@app.post("/suggest-spend")
def suggest_spend(data: dict):
    return suggest_spend_from_target(
        target_first_watch=float(data["target_first_watch"]),
        eg_adm=float(data["eg_adm"]),
        sa_adm=float(data["sa_adm"]),
        ae_adm=float(data["ae_adm"]),
        imdb=float(data["imdb"]),
        elcinema=float(data["elcinema"]),
        letterboxd_rating=float(data["letterboxd_rating"]),
        letterboxd_votes=int(data["letterboxd_votes"]),
        stability=float(data["stability"]),
    )


@app.post("/run-daily")
def run_daily_fetch(x_api_token: str | None = Header(default=None)) -> dict:
    _authorize_jobs(x_api_token)
    with session_scope() as session:
        result = run_daily(session)
    return {"job": "daily", "result": result}


@app.post("/run-weekly")
def run_weekly_fetch(x_api_token: str | None = Header(default=None)) -> dict:
    _authorize_jobs(x_api_token)
    with session_scope() as session:
        result = run_weekly(session)
    return {"job": "weekly", "result": result}