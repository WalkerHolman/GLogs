## GLogs: Weather Ingestion and Analytics with Snowflake + dbt

### Overview
This project ingests hourly weather data from Open‑Meteo, stores it in Snowflake, and transforms it into an analytics‑friendly fact model using dbt.

- `weather.py`: pulls hourly data via Open‑Meteo APIs, writes a CSV for local inspection, and upserts rows into Snowflake (auto‑creates the table if needed).
- dbt models: stage and enrich the raw data into an incremental hourly fact table for easy analysis.

### Tech
- Python 3.9+
- Open‑Meteo `openmeteo-requests`
- Snowflake Python connector
- pandas, requests, python‑dotenv
- dbt Core with `dbt-snowflake`

### Install
From the project root:

```bash
python -m pip install --upgrade pip
pip install .
```

This installs all dependencies defined in `pyproject.toml` (including `dbt-snowflake`).

### Configure environment (.env)
Create a `.env` in the project root with your Snowflake settings. Example:

```env
SNOWFLAKE_USER=YourUser
SNOWFLAKE_PASSWORD=YourPassword
SNOWFLAKE_ACCOUNT=ORG-ACCOUNT      # e.g. JWEXMUN-RPC93691
SNOWFLAKE_HOST=ORG-ACCOUNT.snowflakecomputing.com
SNOWFLAKE_REGION=us-east-1         # optional when using account locator format
SNOWFLAKE_WAREHOUSE=COMPUTE_WH     # must exist and be granted to your role
SNOWFLAKE_ROLE=SYSADMIN            # or your working role with grants
SNOWFLAKE_DATABASE=GLOGS           # or your database
SNOWFLAKE_SCHEMA=PUBLIC            # or your schema

# Optional for dbt; env_run.py sets this automatically if .dbt/ exists
DBT_PROFILES_DIR=.dbt
```

Security tip: do not commit `.env` to source control.

### Snowflake table schema
The raw landing table created/used by `weather.py` is `WEATHER` (in `SNOWFLAKE_DATABASE.SNOWFLAKE_SCHEMA`), with columns:

- `LOCATION` VARCHAR
- `TIME` TIMESTAMP_NTZ
- `TEMPERATURE` FLOAT         (Celsius)
- `PRECIPITATION_PROBABILITY` FLOAT (0–100)
- `PRECIPITATION` FLOAT       (mm)
- `IS_DAY` NUMBER             (0/1)

`weather.py` will:
- create the table if missing,
- sanitize NaN/Inf to NULLs,
- convert timestamps to Snowflake‑friendly strings,
- batch insert with `executemany`.

### Ingest: run the loader
`weather.py` loads `.env` automatically, fetches the previous 24 hours by default, writes `weather.csv`, and inserts into Snowflake:

```bash
python weather.py
```

Key options (see function signature in `weather.py`):
- Choose locations by name (geocoded via Open‑Meteo) and time window.
- Set `only_previous_hour=True` to pull just the previous hour via the forecast endpoint.
- Toggle `save_csv` and `write_outputs` flags.

### dbt project
Models live under `models/`:

- `sources.yml`: declares source table `WEATHER` in your configured database/schema.
- `staging/stg_weather.sql`:
  - normalizes `LOCATION`,
  - truncates `TIME` to hourly grain as `TIME_HOUR`,
  - clamps `PRECIPITATION_PROBABILITY` to [0,100] with defaults,
  - ensures non‑negative `PRECIPITATION_MM`,
  - derives booleans (`IS_PRECIPITATING`, `IS_DAY`),
  - computes `TEMPERATURE_F` from Celsius.
- `marts/fct_weather_hourly.sql`:
  - adds date/time dimensions (`DATE`, `HOUR_OF_DAY`, weekday/month names and numbers, `IS_WEEKEND`),
  - human‑readable `DAY_NIGHT`,
  - `PRECIPITATION_INTENSITY` bucket,
  - `PRECIPITATION_PROB_BUCKET_START` (0,20,40,60,80,100),
  - incremental MERGE on `UNIQUE_KEY = [LOCATION, TIME_HOUR]`,
  - incremental filter on `TIME_HOUR > max(TIME_HOUR)` (auto‑fallback to legacy `TIME` if needed).

Basic tests in `models/schema.yml` check non‑null and uniqueness on keys.

### Running dbt
You can run dbt two ways.

1) Using the provided wrapper (loads `.env` and points to `.dbt/profiles.yml` automatically):
```bash
python env_run.py dbt debug
python env_run.py dbt run --select stg_weather
python env_run.py dbt run --select +fct_weather_hourly   # includes parents
```

2) Using python‑dotenv directly (no wrapper):
```bash
python -m dotenv -f .env run -- dbt debug
python -m dotenv -f .env run -- dbt run --select +fct_weather_hourly
```

Full refresh of the fact model:
```bash
python env_run.py dbt run --select fct_weather_hourly --full-refresh
```

### Incremental loading logic
The fact model is materialized as `incremental` with `merge`:
- Key: `[LOCATION, TIME_HOUR]`
- On re‑runs, only rows with `TIME_HOUR` later than the current `max(TIME_HOUR)` in the target are considered, preventing duplicates.
- If a legacy version of the table exists without `TIME_HOUR`, the model falls back to `TIME` to keep working until you full‑refresh.

### Common issues and fixes
- Missing env vars in dbt:
  - dbt reads shell env, not `.env`. Use `env_run.py` or `python -m dotenv ...`.
- No active warehouse selected:
  - Ensure `SNOWFLAKE_WAREHOUSE` points to an existing warehouse and your role has USAGE.
- 404 host / bad account identifier:
  - Prefer `SNOWFLAKE_ACCOUNT=ORG-ACCOUNT` (e.g. `JWEXMUN-RPC93691`) or set `SNOWFLAKE_HOST=ORG-ACCOUNT.snowflakecomputing.com`.
- Personal database restrictions:
  - Do not target `USER$...` personal DBs—use/create a real database and grant your role.
- Timestamp / NaN binding errors:
  - `weather.py` normalizes timestamps to strings and converts NaN/Inf to NULLs before insert.

### Project structure
```
GLogs/
├─ weather.py                 # ingestion to CSV and Snowflake
├─ schema.py                  # Snowflake table schema (reference)
├─ env_run.py                 # helper: loads .env and runs commands
├─ dbt_project.yml
├─ .dbt/
│  └─ profiles.yml            # dbt profile (uses env vars)
├─ models/
│  ├─ sources.yml
│  ├─ staging/
│  │  └─ stg_weather.sql
│  └─ marts/
│     └─ fct_weather_hourly.sql
├─ pyproject.toml             # dependencies
└─ README.md
```

### Notes
- The Open‑Meteo Historical API and Forecast API are documented at `https://open-meteo.com/en/docs/historical-weather-api`.
- Update locations/time windows in `weather.py` as needed.
- Consider scheduling the pipeline via an orchestrator (Airflow/Dagster/Prefect) or dbt Cloud once validated locally.

