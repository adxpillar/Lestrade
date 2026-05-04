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

This repo documents the Phase 1 schema in `docs/DATA_CONTRACT.md`. Create those tables in Supabase before running the DAGs.

### Run a small backfill to test

In Airflow UI:

- Trigger DAG `edgar_backfill`
- Config example:

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

