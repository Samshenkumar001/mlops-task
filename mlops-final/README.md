# MLOps Signal Pipeline

A minimal MLOps-style batch job that reads OHLCV stock data, computes a rolling mean, generates a binary trading signal, and outputs structured metrics with full observability.

---

## Project Structure

```
mlops-task/
├── run.py           # Main pipeline script
├── config.yaml      # Configuration (seed, window, version)
├── data.csv         # Input OHLCV dataset (10,000 rows)
├── requirements.txt # Python dependencies
├── Dockerfile       # Container definition
├── metrics.json     # Sample output from successful run
├── run.log          # Sample log from successful run
└── README.md        # This file
```

---

## Local Run Instructions

### 1. Prerequisites
- Python 3.9+
- pip

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the Pipeline
```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

### 4. Check Output
```bash
cat metrics.json
cat run.log
```

---

## Docker Build & Run

### Build the Image
```bash
docker build -t mlops-task .
```

### Run the Container
```bash
docker run --rm mlops-task
```

The container will:
- Run the full pipeline using bundled `data.csv` and `config.yaml`
- Print the final `metrics.json` to stdout
- Exit with code `0` on success, non-zero on failure

---

## Example metrics.json

```json
{
  "version": "v1",
  "rows_processed": 9996,
  "metric": "signal_rate",
  "value": 0.4991,
  "latency_ms": 29,
  "seed": 42,
  "status": "success"
}
```

> Note: `rows_processed` is 9996 (not 10000) because the first `window-1 = 4` rows have no rolling mean yet (warm-up period) and are excluded from signal computation. This is by design and documented in the code.

---

## Config Reference

| Field     | Type   | Description                        |
|-----------|--------|------------------------------------|
| `seed`    | int    | Random seed for reproducibility    |
| `window`  | int    | Rolling mean window size           |
| `version` | string | Pipeline version tag               |

---

## Design Decisions

- **NaN handling**: First `window-1` rows produce NaN rolling mean and are excluded from signal and metrics. This avoids misleading signals on insufficient data.
- **Determinism**: `numpy.random.seed(seed)` is set immediately after config load, before any computation.
- **Error handling**: All errors write an error `metrics.json` and exit with code 1, so failures are always observable.
- **No hardcoded paths**: All file paths come from CLI arguments.
