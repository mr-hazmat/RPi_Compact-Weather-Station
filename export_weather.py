#!/usr/bin/env python3
"""
export_weather.py — Export the weather SQLite DB to a JSON file for the dashboard.

Usage:
    python3 export_weather.py                          # defaults
    python3 export_weather.py --db /path/to/weather.db --output /path/to/weather-data.json
    python3 export_weather.py --days 7                 # export only the last 7 days
"""
import argparse
import json
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

# ----------------------
# DEFAULTS
# (Resolved relative to this script's location so the script can be called
#  from any working directory, e.g. via cron or subprocess.)
# ----------------------
_HERE = Path(__file__).parent
DEFAULT_DB = _HERE / "logs" / "weather.db"
DEFAULT_OUTPUT = _HERE / "docs" / "data" / "weather-data.json"

# ----------------------
# VALIDATION RULES
# ----------------------
VALIDATION_RULES = {
    "temp_c": {
        "low": -40.0,
        "high": 80.0,
        "window": 11,
        "n_sigma": 6.0,
        "min_abs_dev": 3.0,
        "jump_threshold": 6.0,
        "neighbor_tol": 1.0,
    },
    "pressure_hpa": {
        "low": 850.0,
        "high": 1100.0,
        "window": 11,
        "n_sigma": 6.0,
        "min_abs_dev": 8.0,
        "jump_threshold": 10.0,
        "neighbor_tol": 2.0,
    },
    "humidity": {
        "low": 0.0,
        "high": 100.0,
        "window": 11,
        "n_sigma": 6.0,
        "min_abs_dev": 10.0,
        "jump_threshold": 15.0,
        "neighbor_tol": 5.0,
    },
}


# ----------------------
# CLEANING
# ----------------------
def clean_sensor_series(
    series: pd.Series,
    *,
    low: float | None = None,
    high: float | None = None,
    window: int = 11,
    n_sigma: float = 6.0,
    min_abs_dev: float = 0.0,
    jump_threshold: float | None = None,
    neighbor_tol: float | None = None,
) -> tuple[pd.Series, pd.Series]:
    """
    Validate a sensor series and return (cleaned_series, invalid_mask).

    Values are marked invalid when they are:
      1. Outside broad physical bounds.
      2. Strong local outliers vs. a rolling median/MAD baseline.
      3. Isolated spikes that sharply disagree with both neighbours.
    """
    s = pd.to_numeric(series, errors="coerce").astype(float).copy()
    invalid = pd.Series(False, index=s.index)

    # 1. Hard bounds
    if low is not None:
        invalid |= s < low
    if high is not None:
        invalid |= s > high

    # 2. Rolling median / MAD outlier detection
    min_periods = max(3, window // 2)
    rolling_median = s.rolling(window=window, center=True, min_periods=min_periods).median()
    absolute_deviation = (s - rolling_median).abs()
    rolling_mad = absolute_deviation.rolling(window=window, center=True, min_periods=min_periods).median()
    robust_sigma = 1.4826 * rolling_mad

    local_outlier = absolute_deviation > np.maximum(
        n_sigma * robust_sigma.fillna(np.inf),
        min_abs_dev,
    )
    invalid |= local_outlier.fillna(False)

    # 3. Isolated spike detection
    if jump_threshold is not None and neighbor_tol is not None:
        prev = s.shift(1)
        nxt = s.shift(-1)
        isolated_spike = (
            prev.notna()
            & nxt.notna()
            & ((prev - nxt).abs() <= neighbor_tol)
            & ((s - prev).abs() >= jump_threshold)
            & ((s - nxt).abs() >= jump_threshold)
        )
        invalid |= isolated_spike

    return s.mask(invalid), invalid


# ----------------------
# HELPERS
# ----------------------
def last_valid_entry(df: pd.DataFrame, column: str) -> dict:
    """Return the timestamp and value of the most recent non-null reading."""
    valid = df.loc[df[column].notna(), ["timestamp", column]]
    if valid.empty:
        return {"timestamp": None, "value": None}
    row = valid.iloc[-1]
    return {
        "timestamp": row["timestamp"],
        "value": round(float(row[column]), 2),
    }


# ----------------------
# PAYLOAD BUILDER
# ----------------------
def build_payload(df: pd.DataFrame) -> dict:
    cleaned = df.copy()

    temp_c_clean, temp_c_invalid = clean_sensor_series(cleaned["temp_c"], **VALIDATION_RULES["temp_c"])
    pressure_clean, pressure_invalid = clean_sensor_series(cleaned["pressure_hpa"], **VALIDATION_RULES["pressure_hpa"])
    humidity_clean, humidity_invalid = clean_sensor_series(cleaned["humidity"], **VALIDATION_RULES["humidity"])

    # temp_f is always derived from cleaned temp_c — never use the raw DB column
    cleaned["temp_c"] = temp_c_clean
    cleaned["temp_f"] = temp_c_clean * 9.0 / 5.0 + 32.0
    cleaned["pressure_hpa"] = pressure_clean
    cleaned["humidity"] = humidity_clean

    validation = {
        "temp_c":       {"removed": int(temp_c_invalid.sum())},
        "temp_f":       {"removed": int(temp_c_invalid.sum())},  # mirrors temp_c
        "pressure_hpa": {"removed": int(pressure_invalid.sum())},
        "humidity":     {"removed": int(humidity_invalid.sum())},
    }

    summary = {
        "count": int(len(cleaned)),
        "start": cleaned.iloc[0]["timestamp"],
        "end":   cleaned.iloc[-1]["timestamp"],
        "latest": {
            "temp_c":       last_valid_entry(cleaned, "temp_c"),
            "temp_f":       last_valid_entry(cleaned, "temp_f"),
            "pressure_hpa": last_valid_entry(cleaned, "pressure_hpa"),
            "humidity":     last_valid_entry(cleaned, "humidity"),
        },
        "validation_total_removed": int(
            validation["temp_c"]["removed"]
            + validation["pressure_hpa"]["removed"]
            + validation["humidity"]["removed"]
        ),
    }

    # Build readings list efficiently — avoid iterrows() for large DataFrames
    cleaned_rounded = cleaned.copy()
    for col in ("temp_c", "temp_f", "pressure_hpa", "humidity"):
        cleaned_rounded[col] = cleaned_rounded[col].round(2)

    records = cleaned_rounded[["timestamp", "temp_c", "temp_f", "pressure_hpa", "humidity"]].to_dict("records")

    readings = [
        {
            "timestamp":    row["timestamp"],
            "temp_c":       None if pd.isna(row["temp_c"]) else row["temp_c"],
            "temp_f":       None if pd.isna(row["temp_f"]) else row["temp_f"],
            "pressure_hpa": None if pd.isna(row["pressure_hpa"]) else row["pressure_hpa"],
            "humidity":     None if pd.isna(row["humidity"]) else row["humidity"],
        }
        for row in records
    ]

    return {
        "summary": summary,
        "validation": validation,
        "readings": readings,
    }


# ----------------------
# MAIN
# ----------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export weather DB to JSON for the dashboard.")
    parser.add_argument("--db",     type=Path, default=DEFAULT_DB,     help="Path to weather.db")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,  help="Path for weather-data.json")
    parser.add_argument("--days",   type=int,  default=None,           help="Export only the last N days (default: all)")
    parser.add_argument("--from",   dest="from_date", type=str, default=None, help="Export from date onwards, format: YYYY-MM-DD")
    return parser.parse_args()

def main() -> None:
    args = parse_args()

    if not args.db.exists():
        raise FileNotFoundError(f"Database not found: {args.db.resolve()}")

    args.output.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    if args.from_date:
        df = pd.read_sql_query(
            "SELECT timestamp, temp_c, temp_f, pressure_hpa, humidity FROM readings WHERE timestamp >= ? ORDER BY timestamp",
        conn, params=(args.from_date,)
    )
    elif args.days is not None:
        df = pd.read_sql_query(
            "SELECT timestamp, temp_c, temp_f, pressure_hpa, humidity FROM readings WHERE timestamp >= datetime('now', ?) ORDER BY timestamp",
           conn, params=(f"-{args.days} days",)
        )
    else:
        df = pd.read_sql_query(
            "SELECT timestamp, temp_c, temp_f, pressure_hpa, humidity FROM readings ORDER BY timestamp",
            conn,
        )
    conn.close()

    if df.empty:
        raise ValueError("No rows found in the readings table.")

    payload = build_payload(df)
    args.output.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")

    print(f"Wrote {args.output.resolve()} — {len(df):,} rows")
    for metric, info in payload["validation"].items():
        if info["removed"]:
            print(f"  {metric}: removed {info['removed']} suspect value(s)")


if __name__ == "__main__":
    main()

