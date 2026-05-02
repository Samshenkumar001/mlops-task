# 🚀 MLOps Signal Pipeline

A production-ready MLOps batch job that processes OHLCV financial data, computes rolling mean signals, and outputs structured metrics — fully Dockerized and reproducible.

---

## 📋 Table of Contents
- [Overview](#overview)
- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Local Setup & Run](#local-setup--run)
- [Docker Setup & Run](#docker-setup--run)
- [Configuration](#configuration)
- [Output Format](#output-format)
- [Design Decisions](#design-decisions)

---

## 🎯 Overview

This pipeline:
1. Loads configuration from a YAML file (seed, window, version)
2. Reads and validates an OHLCV dataset (CSV format)
3. Computes a rolling mean on the `close` column
4. Generates a binary trading signal (`1` = buy, `0` = hold)
5. Outputs structured metrics to JSON + detailed logs
6. Runs locally and inside Docker with a single command

**Three core MLOps principles demonstrated:**
- ✅ **Reproducibility** — deterministic runs via `numpy.random.seed` from config
- ✅ **Observability** — structured timestamped logs + machine-readable metrics JSON
- ✅ **Deployment Readiness** — fully Dockerized, one-command execution

---

## 📁 Project Structure

```
mlops-task/
├── run.py            # Main pipeline script
├── config.yaml       # Configuration (seed, window, version)
├── data.csv          # Input OHLCV dataset (10,000 rows)
├── requirements.txt  # Python dependencies
├── Dockerfile        # Container definition
├── metrics.json      # Sample output from successful run
├── run.log           # Sample log from successful run
└── README.md         # This file
```

---

## ⚙️ Requirements

- Python 3.9+
- pip
- Docker Desktop (for Docker run)

---

## 💻 Local Setup & Run

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/mlops-task.git
cd mlops-task
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the pipeline
```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

### 4. View results
```bash
# View metrics (Windows)
type metrics.json

# View logs (Windows)
type run.log
```

---

## 🐳 Docker Setup & Run

### Build the image
```bash
docker build -t mlops-task .
```

### Run the container
```bash
docker run --rm mlops-task
```

The container will:
- Run the full pipeline using bundled `data.csv` and `config.yaml`
- Print final `metrics.json` to stdout
- Exit with code `0` on success, non-zero on failure

---

## 🔧 Configuration

Edit `config.yaml` to change pipeline behavior:

```yaml
seed: 42      # Random seed for reproducibility
window: 5     # Rolling mean window size (days)
version: "v1" # Pipeline version tag
```

| Field | Type | Description |
|-------|------|-------------|
| `seed` | int | Fixes random state — same seed = same output every run |
| `window` | int | Number of periods for rolling mean calculation |
| `version` | string | Version tag included in metrics output |

---

## 📊 Output Format

### metrics.json — Success
```json
{
  "version": "v1",
  "rows_processed": 9996,
  "metric": "signal_rate",
  "value": 0.4991,
  "latency_ms": 524,
  "seed": 42,
  "status": "success"
}
```

### metrics.json — Error
```json
{
  "version": "v1",
  "status": "error",
  "error_message": "Description of what went wrong"
}
```

### run.log — Sample
```
2026-05-02T13:57:08 | INFO     | Job Started
2026-05-02T13:57:08 | INFO     | Config validated — seed=42, window=5, version=v1
2026-05-02T13:57:08 | INFO     | Dataset loaded — 10000 rows
2026-05-02T13:57:08 | INFO     | Rolling mean computed — 9996 valid rows
2026-05-02T13:57:08 | INFO     | Signal generated — 4989 buy, 5007 hold
2026-05-02T13:57:08 | INFO     | Job completed successfully
```

---

## 🏗️ Design Decisions

### NaN Handling
The first `window - 1` rows (4 rows with window=5) produce NaN rolling mean values because there is insufficient price history. These rows are **excluded** from signal computation. This is intentional — generating signals without enough data would produce misleading results.

This is why `rows_processed = 9996` instead of `10000`.

### Error Handling
All validation errors (missing file, invalid CSV, missing `close` column, bad config) write an error-status `metrics.json` and exit with code `1`. This ensures every run is observable — even failures.

### No Hardcoded Paths
All file paths are passed via CLI arguments (`--input`, `--config`, `--output`, `--log-file`). This makes the pipeline flexible and environment-agnostic.

### Reproducibility
`numpy.random.seed(seed)` is set immediately after config validation, before any computation. Combined with config-driven parameters, this guarantees identical outputs for identical inputs across all environments.

---

## 📈 Signal Logic

```
For each row (after warm-up period):
  if close_price > rolling_mean(window):
      signal = 1  (BUY — price above recent average, upward momentum)
  else:
      signal = 0  (HOLD — price at or below recent average)

signal_rate = mean(all signals) = fraction of BUY signals
```

---

## ✅ Validation Checks

| Check | What it does |
|-------|-------------|
| Config exists | Raises error if config.yaml not found |
| Required config fields | Validates seed, window, version exist with correct types |
| Input file exists | Raises error if CSV not found |
| Non-empty CSV | Raises error if file is empty |
| Valid CSV format | Raises error if file cannot be parsed |
| close column present | Raises error if required column is missing |

---

*Built for Primetrade.ai ML Engineering Internship Assessment*
