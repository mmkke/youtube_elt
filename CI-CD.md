
# Ci/CD Steps

## Step 1 - set a checklist tha tmust be passed before merging

    Unit Tests: PYTHONPATH=src pytest -q tests/unit

    Integration Tests: PYTHONPATH=src pytest -q tests/integration

    DAG import: PYTHONPATH=src python -c "import dags.youtube_api_ingestion"

    Formatting/linting


## Step 2 - Create CI workflow

    Make a CI safe config:
        - Removes reliance on .env
        - Uses Github Secrets
    
    Add Github Actions Workflow


