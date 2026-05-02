"""
MLOps Batch Job - Trading Signal Pipeline
Author: ML Engineering Intern Assessment
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


def setup_logging(log_file: str) -> logging.Logger:
    """Configure structured logging to both file and stdout."""
    logger = logging.getLogger("mlops_pipeline")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )

    # File handler
    fh = logging.FileHandler(log_file, mode="w")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def write_metrics(output_path: str, payload: dict, logger: logging.Logger) -> None:
    """Write metrics JSON to output file — always called, even on error."""
    try:
        with open(output_path, "w") as f:
            json.dump(payload, f, indent=2)
        logger.info(f"Metrics written to {output_path}")
    except Exception as e:
        logger.error(f"Failed to write metrics file: {e}")


def load_config(config_path: str, logger: logging.Logger) -> dict:
    """Load and validate YAML config."""
    logger.info(f"Loading config from: {config_path}")

    if not Path(config_path).exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ValueError("Config must be a YAML mapping/dictionary.")

    required_fields = {"seed": int, "window": int, "version": str}
    for field, expected_type in required_fields.items():
        if field not in config:
            raise KeyError(f"Missing required config field: '{field}'")
        if not isinstance(config[field], expected_type):
            raise TypeError(
                f"Config field '{field}' must be {expected_type.__name__}, "
                f"got {type(config[field]).__name__}"
            )

    if config["window"] < 1:
        raise ValueError(f"'window' must be >= 1, got {config['window']}")

    logger.info(
        f"Config validated — seed={config['seed']}, "
        f"window={config['window']}, version={config['version']}"
    )
    return config


def load_dataset(input_path: str, logger: logging.Logger) -> pd.DataFrame:
    """Load and validate the OHLCV CSV dataset."""
    logger.info(f"Loading dataset from: {input_path}")

    if not Path(input_path).exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    try:
        # Try normal read first
        df = pd.read_csv(input_path)
        # If all columns are in one string, entire rows are quoted — fix by reading raw
        if len(df.columns) == 1 and "," in df.columns[0]:
            import io
            with open(input_path, "r", encoding="utf-8") as f:
                raw = f.read().replace('"', '')
            df = pd.read_csv(io.StringIO(raw))
    except Exception as e:
        raise ValueError(f"Failed to parse CSV: {e}")

    if df.empty:
        raise ValueError("Input CSV is empty.")

    if "close" not in df.columns:
        raise ValueError(
            f"Required column 'close' not found. "
            f"Available columns: {list(df.columns)}"
        )

    if df["close"].isnull().all():
        raise ValueError("Column 'close' contains only null values.")

    logger.info(f"Dataset loaded — {len(df)} rows, columns: {list(df.columns)}")
    return df


def compute_rolling_mean(df: pd.DataFrame, window: int, logger: logging.Logger) -> pd.Series:
    """
    Compute rolling mean on 'close' column.
    First (window-1) rows will be NaN — these rows are excluded from signal computation.
    """
    logger.info(f"Computing rolling mean with window={window}")
    rolling_mean = df["close"].rolling(window=window, min_periods=window).mean()
    valid_count = rolling_mean.notna().sum()
    logger.info(
        f"Rolling mean computed — {valid_count} valid rows "
        f"({window - 1} warm-up rows excluded as NaN)"
    )
    return rolling_mean


def compute_signal(df: pd.DataFrame, rolling_mean: pd.Series, logger: logging.Logger) -> pd.Series:
    """
    Generate binary signal:
      signal = 1 if close > rolling_mean, else 0
    Rows where rolling_mean is NaN are excluded (set to NaN, then dropped).
    """
    logger.info("Generating binary trading signal (1=buy, 0=hold)")
    signal = pd.Series(np.nan, index=df.index)
    valid_mask = rolling_mean.notna()
    signal[valid_mask] = (df.loc[valid_mask, "close"] > rolling_mean[valid_mask]).astype(int)
    buy_count = int(signal[valid_mask].sum())
    logger.info(
        f"Signal generated — {valid_mask.sum()} rows evaluated, "
        f"{buy_count} buy signals (1), "
        f"{valid_mask.sum() - buy_count} hold signals (0)"
    )
    return signal


def main():
    parser = argparse.ArgumentParser(description="MLOps Batch Signal Pipeline")
    parser.add_argument("--input",   required=True, help="Path to input CSV file")
    parser.add_argument("--config",  required=True, help="Path to YAML config file")
    parser.add_argument("--output",  required=True, help="Path to output metrics JSON")
    parser.add_argument("--log-file", required=True, help="Path to log file")
    args = parser.parse_args()

    logger = setup_logging(args.log_file)
    logger.info("=" * 60)
    logger.info("MLOps Signal Pipeline — Job Started")
    logger.info("=" * 60)

    start_time = time.time()
    version = "v1"  # fallback before config loads

    try:
        # Step 1: Load + validate config
        config = load_config(args.config, logger)
        version = config["version"]
        seed = config["seed"]
        window = config["window"]

        # Step 2: Set random seed for reproducibility
        np.random.seed(seed)
        logger.info(f"Random seed set: {seed}")

        # Step 3: Load + validate dataset
        df = load_dataset(args.input, logger)

        # Step 4: Compute rolling mean
        rolling_mean = compute_rolling_mean(df, window, logger)

        # Step 5: Compute signal
        signal = compute_signal(df, rolling_mean, logger)

        # Step 6: Compute metrics (only over valid rows)
        valid_mask = signal.notna()
        rows_processed = int(valid_mask.sum())
        signal_rate = round(float(signal[valid_mask].mean()), 4)
        latency_ms = round((time.time() - start_time) * 1000)

        logger.info(f"rows_processed : {rows_processed}")
        logger.info(f"signal_rate    : {signal_rate}")
        logger.info(f"latency_ms     : {latency_ms}")

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": signal_rate,
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }

        write_metrics(args.output, metrics, logger)
        logger.info("Job completed successfully ✓")
        logger.info("=" * 60)

        # Print final metrics JSON to stdout (Docker requirement)
        print(json.dumps(metrics, indent=2))
        sys.exit(0)

    except Exception as exc:
        latency_ms = round((time.time() - start_time) * 1000)
        logger.error(f"Pipeline failed: {exc}", exc_info=True)

        error_metrics = {
            "version": version,
            "status": "error",
            "error_message": str(exc)
        }
        write_metrics(args.output, error_metrics, logger)
        logger.info("=" * 60)

        print(json.dumps(error_metrics, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()