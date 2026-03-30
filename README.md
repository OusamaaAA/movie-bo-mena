# MENA Box Office Intelligence + Marketing Score App

Production-minded internal app replacing Google Sheets/App Script operations with:
- Streamlit UI
- Python ingestion + matching + scoring engine
- Supabase/Postgres source-aware data model

## Key Principles

- Raw source rows stay source-distinct.
- Unified reporting combines only semantically comparable records.
- Daily/weekly/backfill/on-demand ingestion are first-class entrypoints.
- Matching and review queue preserve confidence and analyst control.

## Project Tree

```text
.
├─ app.py
├─ pyproject.toml
├─ .env.example
├─ README.md
├─ migrations/
│  ├─ 001_init_schema.sql
│  └─ 002_seed_sources.sql
├─ pages/
│  ├─ 01_Search_Lookup.py
│  ├─ 02_Film_Report.py
│  ├─ 03_Source_Explorer.py
│  ├─ 04_Review_Queue.py
│  ├─ 05_Data_Admin.py
│  └─ 06_Marketing_Inputs.py
├─ scripts/
│  ├─ bootstrap_db.py
│  └─ seed_demo_data.py
└─ src/
   ├─ cli.py
   ├─ config.py
   ├─ db.py
   ├─ logging_config.py
   ├─ models.py
   ├─ schema_types.py
   ├─ repositories/
   ├─ services/
   └─ sources/
      ├─ filmyard/
      ├─ elcinema/
      ├─ boxofficemojo/
      └─ imdb/
```

## Quick Start

1) Create virtualenv and install:

```bash
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -e .
```

2) Configure env:

```bash
copy .env.example .env
```

Set `DATABASE_URL` to your Supabase Postgres connection string.

3) Apply SQL migrations:

```bash
python scripts/bootstrap_db.py
```

4) Seed optional demo data:

```bash
python scripts/seed_demo_data.py
```

5) Run Streamlit app:

```bash
streamlit run app.py
```

## Prediction Modes (Deployment Ready)

The app supports two prediction modes without changing formulas:

- `direct` (default): Streamlit loads `Model/film_prediction_model.pkl` and runs inference in-process.
- `api`: Streamlit calls an external API (`PREDICTION_API_URL`) for `/predict` and `/suggest-spend`.

Environment/secrets keys:

- `PREDICTION_MODE=direct|api`
- `MODEL_PATH=Model/film_prediction_model.pkl`
- `PREDICTION_API_URL=http://127.0.0.1:8000` (used only in `api` mode)

## Streamlit Community Cloud

Use `requirements.txt` for dependency install and set app entrypoint to:

- `app.py`

Set secrets in Streamlit Cloud (App > Settings > Secrets), example:

```toml
DATABASE_URL = "postgresql+psycopg://USER:PASSWORD@HOST:5432/postgres"
PREDICTION_MODE = "direct"
MODEL_PATH = "Model/film_prediction_model.pkl"
```

Notes:

- Supabase remains external; no schema or write-path changes are required.
- No separate FastAPI/uvicorn process is required in `direct` mode.
- Existing local API development still works in `api` mode.

## Automation Endpoints (optional API mode)

When running `Model/model_api.py` with FastAPI, you can trigger ingestion jobs via:

- `POST /run-daily`
- `POST /run-weekly`

These endpoints call the same existing job functions (`run_daily`, `run_weekly`) with no formula/logic changes.

Optional protection:

- Set `JOBS_API_TOKEN` and pass it as header `x-api-token`.

Example:

```bash
curl -X POST http://127.0.0.1:8000/run-daily -H "x-api-token: YOUR_TOKEN"
curl -X POST http://127.0.0.1:8000/run-weekly -H "x-api-token: YOUR_TOKEN"
```

## Ingestion Jobs

- Daily run (Filmyard + IMDb ratings refresh for tracked IMDb IDs in `market_reference` where `reference_type='imdb_title_id'`):
  - `mbi jobs daily`
- Weekly run (elCinema + BOM weekly):
  - `mbi jobs weekly`
- Historic backfill:
  - `mbi jobs backfill --source filmyard --days 90`
- On-demand title refresh:
  - `mbi jobs refresh-title --title "Dune: Part Two"`

## Notes on Migration from Legacy `code.gs`

- Preserves core concepts: raw evidence, normalized evidence, reconciled evidence, film master, aliases, review queue, run logging, source status.
- Adopts explicit semantics fields per evidence record:
  `record_scope`, `record_granularity`, `record_semantics`, `evidence_type`, period fields, confidence fields.
- Uses last-effective logic principle for duplicated legacy functions by implementing cleaner single-responsibility modules in Python.

## Operational Assumptions

- Source endpoints and HTML selectors vary over time. Parsers are designed for safe failure + run logging.
- Marketing spend and target outcome start as manual inputs in UI.
- Scoring engine is transparent weighted rules; no black-box ML by default.
- Backfill jobs iterate historical periods (days/weeks) and gracefully skip unavailable archive pages.

