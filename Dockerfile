# Runs the full pipeline (land -> stage -> gate -> marts -> docs) in a
# container, entirely offline -- DuckDB + the bundled CSV fixtures, no
# external services required.
#
# NOT used by the default test suite (`pytest` runs directly on the host /
# CI runner against a plain venv) -- this image is for demoing/deploying the
# pipeline itself. Build and run it with:
#
#   docker build -t lakehouse-quality-gate .
#   docker run --rm lakehouse-quality-gate --chaos-mode none
#   docker run --rm lakehouse-quality-gate --chaos-mode null_flood   # gate blocks this (exit 1)

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DUCKDB_PATH=/app/data/warehouse.duckdb \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "scripts.run_pipeline"]
CMD ["--chaos-mode", "none"]
