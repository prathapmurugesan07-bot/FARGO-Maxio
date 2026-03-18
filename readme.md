# Maxio Repo

This folder contains the Maxio-specific code and tests extracted from the main workspace.

## Contents

- `src/maxio_client.py`
- `src/maxio_test.py`
- `src/azure_ingest_maxio.py`
- `src/test_maxio_azure.py`
- `test_maxio_token.py`

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Run

```bash
python src/maxio_test.py
python src/azure_ingest_maxio.py
python test_maxio_token.py
```
