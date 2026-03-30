# Maxio Repo

This folder contains the Maxio-specific code and tests extracted from the main workspace.

## Contents

- `src/extract/maxio_client.py`
- `src/extract/utils.py`
- `src/load/azure_ingest_maxio.py`
- `src/load/azure_ingest_maxio_staging.py`
- `src/load/utils.py`
- `src/load/test_maxio.py`

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Run

```bash
python src/load/test_maxio.py
python src/load/azure_ingest_maxio.py
python src/load/azure_ingest_maxio_staging.py
python src/load/azure_ingest_maxio_both.py
```

The staging loader writes flat CSV files directly into the `maxio` container root.
Container names are fixed to `raw` for the hierarchical loader and `maxio` for the staging loader.

`azure_ingest_maxio_both.py` runs both:
- Raw: timestamped folder hierarchy under `raw`
- Staging: flat files (latest only) under `maxio`
