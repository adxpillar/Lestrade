## Local Airflow (Docker) + Supabase Postgres

This repo includes an open-source Airflow stack for running the ingestion DAGs without AWS MWAA.

### What runs where

- **Airflow metadata DB**: local Postgres container from `docker-compose.yml` (service `airflow-db`)
- **Lestrade data DB**: **Supabase Postgres** (configured via Airflow Connection `lestrade_rds`)

### Prerequisites

- Docker Desktop (or Docker Engine) with Compose
- A Supabase project with Postgres credentials

### Start Airflow

From the repo root:

```bash
cp docker/airflow/airflow.env.example .env
docker compose up airflow-init
docker compose up -d
```

Open the Airflow UI at `http://localhost:8080` (credentials come from `.env`).

### Point the DAGs to Supabase

The DAGs default to using an Airflow connection id named **`lestrade_rds`**.

You can configure it either via UI or environment variable.

#### Option A: Environment variable (recommended)

Set in `.env`:

```bash
AIRFLOW_CONN_LESTRADE_RDS=postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require
```

Supabase typically uses:

- **DBNAME**: `postgres`
- **sslmode**: `require`

#### Option B: Airflow UI

In Airflow UI:

- Admin → Connections → Add
- Conn Id: `lestrade_rds`
- Conn Type: Postgres
- Host/Schema/Login/Password/Port: from Supabase
- Extras: `{"sslmode":"require"}`

### Create tables

Apply SQL migrations in **`db/migrations/`** in order (see **`db/README.md`**) against your Supabase (or other Postgres) before running the DAGs.

### Run a small backfill to test

In Airflow UI:

- Drop **exactly one** highs and one lows CSV into **`barchart_report_today/`** (see README); optional **`LESTRADE_UNIVERSE_TRADING_DATE`** must match the date in both filenames; trigger DAG **`universe_snapshot`**
- Then trigger DAG **`edgar_daily_incremental`**
  - `universe_snapshot` archives processed CSVs into `barchart_report_archive/YYYY-MM-DD/` by default (override with `LESTRADE_BARCHART_ARCHIVE_DIR`).

#### Universe config

Put Barchart files in the mounted repo folder **`barchart_report_today/`** (shown as **`/opt/airflow/lestrade/barchart_report_today`** inside containers unless you override):

```bash
LESTRADE_UNIVERSE_TRADING_DATE=2026-05-06
# Optional absolute path when not using Compose default mount:
# LESTRADE_BARCHART_DIR=/opt/airflow/lestrade/barchart_report_today
```

#### Backfill (optional)

Trigger DAG `edgar_backfill`:

```json
{"start_date":"2025-01-02","end_date":"2025-01-02","use_submissions":false}
```

### Stop / reset

```bash
docker compose down
```

To wipe local Airflow metadata/logs volumes:

```bash
docker compose down -v
```

