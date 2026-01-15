# Airflow + Docker (Compose) Cheat Sheet

This doc explains common Airflow CLI and Docker Compose commands, plus practical tips for running Airflow locally with Docker.

---

## Airflow CLI commands

### `airflow --help`
Shows top-level help for the Airflow CLI, including available subcommands and global flags.

Use when:
- You can’t remember a command name
- You want to explore Airflow functionality

```bash
airflow --help
airflow dags --help
airflow tasks --help
```

---

### `airflow info`
Displays information about the current Airflow environment:
- Airflow home
- Config paths
- Installed providers
- Python version
- Database configuration summary

Use when:
- Verifying environment configuration
- Debugging config or env var issues

```bash
airflow info
```

Inside Docker:
```bash
docker compose exec airflow-webserver airflow info
```

---

### `airflow cheat-sheet -v`
Prints a curated list of commonly used Airflow CLI commands.
The `-v` flag provides a more verbose output.

Use when:
- Learning Airflow
- You need a quick reminder of common CLI workflows

```bash
airflow cheat-sheet -v
```

---

## Docker Compose commands (Airflow stack)

### `docker compose up -d`
Starts all services in detached (background) mode.

Use when:
- Running Airflow normally during development

```bash
docker compose up -d
```

---

### `docker compose up -d --build`
Builds images (if needed) and then starts services in detached mode.

Use when:
- You changed the Dockerfile
- You updated `requirements.txt`
- You changed build arguments

```bash
docker compose up -d --build
```

---

### `docker compose down`
Stops and removes containers and networks.
Volumes (database data) are preserved.

Use when:
- Stopping Airflow cleanly
- Restarting containers without losing data

```bash
docker compose down
```

---

### `docker compose down -v`
Stops containers and removes volumes.
This wipes Postgres data and forces init scripts to rerun.

Use when:
- You need a full reset
- DB credentials or init scripts changed

⚠️ **Warning:** This deletes all persisted database data.

```bash
docker compose down -v
```

---

## Useful Docker + Airflow tips

### Check service status
```bash
docker compose ps
```

Expected state:
- `postgres`, `redis`: healthy
- `airflow-webserver`, `airflow-scheduler`, `airflow-worker`: running
- `airflow-init`: exited (0)

---

### View logs
```bash
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker
```

Limit output:
```bash
docker compose logs --tail=200 -f airflow-scheduler
```

---

### Run Airflow CLI inside a container
```bash
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow tasks list <dag_id>
```

---

### Restart a single service
```bash
docker compose restart airflow-scheduler
docker compose restart airflow-webserver
docker compose restart airflow-worker
```

---

### Common reset workflow
```bash
docker compose down -v
docker compose up -d --build
```

---

## Quick reference

- Start stack: `docker compose up -d`
- Start + rebuild: `docker compose up -d --build`
- Stop stack: `docker compose down`
- Full reset: `docker compose down -v`
- Status: `docker compose ps`
- Logs: `docker compose logs -f <service>`
- Airflow CLI: `docker compose exec airflow-webserver airflow <command>`
