# YouTube ELT Pipeline

An end-to-end ELT pipeline for ingesting YouTube channel data, transforming it into a structured Postgres data warehouse, and maintaining both current-state analytics and historical daily metrics using Apache Airflow.

This project is designed for incremental ingestion, idempotent updates, and audit-friendly analytics.

---

## Architecture Overview

```
YouTube API
    ↓
Raw JSON (data/)
    ↓
staging.yt_api      (raw + lightly typed)
    ↓
core.yt_api         (cleaned, transformed, canonical)
    ↓
core.yt_api_metrics_daily  (daily snapshots)
```

### Key Design Principles
- Schema-on-write in the warehouse
- Mutable facts handled via upserts
- Time-aware analytics via daily snapshot tables
- Separation of concerns between ingestion, transformation, and quality checks
- Testable at every layer (unit, integration, DAG-level)

---

## Tech Stack
- Apache Airflow (DAG orchestration)
- PostgreSQL (data warehouse)
- Python 3.10
- psycopg2 (DB access)
- pytest (unit & integration testing)
- Docker / Docker Compose

---

## Repository Structure

```
.
├── dags/                   # Airflow DAG definitions
│   └── youtube_api_ingestion.py
|
├── data/                    # Raw JSON API snapshots
|
├── docker/
│   └── postgres/            # DB initialization scripts
|
├── src/elt/                 # Application code (packaged)
│   ├── api/                 # YouTube API extraction logic
│   ├── data_quality/        # Soda checks & validation
│   └── dwh/                 # Warehouse logic
│
├── tests/
│   ├── unit/                # Pure Python tests (no DB)
│   └── integration/         # Real Postgres tests
│
├── .env
├── .gitignore
├── docker-compose.yml
├── dockerfile
├── pytest.ini
├── README.md
└── requiremnets.txt
```

---

## Data Model

### staging.yt_api
- Mirrors API payload structure
- Minimal transformation
- Supports frequent updates

### core.yt_api
- Canonical representation
- Typed fields (INTERVAL, BIGINT, etc.)
- One row per video (current state)

### core.yt_api_metrics_daily
- Daily snapshot of engagement metrics
- Composite PK: (Video_ID, Snapshot_Date)
- Enables trend analysis without overwriting history

---

## Airflow DAGs

### 1. youtube_api_ingestion
- Fetches video metadata from the YouTube API
- Writes raw JSON snapshots to disk

### 2. youtube_db_load
- Loads JSON → staging.yt_api
- Transforms & upserts into core.yt_api
- Handles deletes for removed videos

### 3. data_quality_soda_check
- Runs Soda checks against staging and core layers

---

## Timestamps & Mutability

Each layer supports operational timestamps:

| Column | Purpose |
|------|--------|
| Ingested_At | When the row was first ingested |
| Snapshot_Date | Logical date for metrics snapshots |
| Logical Date (Airflow) | Used for deterministic backfills |

Daily metrics use Airflow’s logical date, not `date.today()`, enabling safe backfills and reruns.

---

## Testing Strategy

### Unit Tests
- Pure Python
- No database access
- Validate transformation logic and SQL construction

```
pytest tests/unit
```

### Integration Tests
- Run against a real Postgres instance
- Use isolated schemas per test
- Validate DDL + DML behavior

```
pytest tests/integration
```

### Key Guarantees Tested
- Idempotent upserts
- Correct deletes
- Foreign key integrity
- Snapshot date correctness

---

## Local Development

### Start the Stack
```
docker compose up -d
```

### Run Tests
```
docker exec -it airflow-webserver pytest
```

### Access Airflow UI
```
http://localhost:8080
```

---

## Configuration

Environment variables are managed via `.env` and injected into Airflow:
- API keys via `AIRFLOW_VAR_*`
- Database connections via `AIRFLOW_CONN_*`
- Separate databases for prod and test

---
