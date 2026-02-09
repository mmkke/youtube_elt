## Continuous Integration (CI)

[![CI](https://github.com/mmkke/youtube_elt/actions/workflows/ci.yml/badge.svg)](
https://github.com/mmkke/youtube_elt/actions/workflows/ci.yml
)

This project uses **GitHub Actions** to run a full Continuous Integration (CI) pipeline on every pull request and push to the `main` / `master` branches.

The CI pipeline is designed to ensure code quality, correctness, and reproducibility for a production-style data engineering workflow.

### CI Workflow Jobs

The CI workflow is split into two logical jobs:

#### 1. Linting
Runs directly on the GitHub runner using Python 3.10.

- **Ruff**: Static analysis and style enforcement
- **Black**: Code formatting validation

These checks ensure consistent style, catch unused imports, and prevent common Python errors before code is merged.

#### 2. Testing
Runs inside a fully containerized Airflow + Postgres environment using Docker Compose.

This job:
- Builds a custom Airflow image
- Starts Postgres, Redis, and Airflow services
- Initializes multiple databases (metadata, Celery backend, ELT, test ELT)
- Runs:
  - Unit tests
  - Integration tests
  - DAG import sanity checks
- Executes Soda data quality checks as part of the DAG layer

All tests run against an isolated CI environment using a generated `.env` file with non-sensitive dummy credentials.

## Continuous Delivery

N/A
