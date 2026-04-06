"""
Reusable write / read helpers for raw and processed data.
All file I/O in the pipeline should go through these functions.
"""
import json
from pathlib import Path

import pandas as pd

from utils.logger import get_logger
from utils.paths import RAW_DIR, PROCESSED_DIR

logger = get_logger(__name__)


# ── Write helpers ─────────────────────────────────────────────────────────────

def write_raw_json(data: list, filename: str) -> Path:
    """Serialise a list to JSON in the raw data directory."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2, default=str)
    logger.info(f"Raw JSON written  → {path}  ({len(data)} records)")
    return path


def write_processed_csv(df: pd.DataFrame, filename: str) -> Path:
    """Write a DataFrame to CSV in the processed data directory."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    path = PROCESSED_DIR / filename
    df.to_csv(path, index=False, encoding="utf-8")
    logger.info(f"Processed CSV written → {path}  ({len(df)} rows × {len(df.columns)} cols)")
    return path


def write_processed_parquet(df: pd.DataFrame, filename: str) -> Path:
    """Write a DataFrame to Parquet in the processed data directory."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    path = PROCESSED_DIR / filename
    df.to_parquet(path, index=False)
    logger.info(f"Processed Parquet written → {path}  ({len(df)} rows)")
    return path


# ── Read helpers ──────────────────────────────────────────────────────────────

def read_processed_csv(filename: str) -> pd.DataFrame:
    """Read a processed CSV back into a DataFrame."""
    path = PROCESSED_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"Processed file not found: {path}\n"
            "  → Run 'python orchestrator.py' first."
        )
    df = pd.read_csv(path, encoding="utf-8")
    logger.info(f"Read CSV ← {path}  ({len(df)} rows)")
    return df


def read_raw_json(filename: str) -> list:
    """Read a raw JSON file back into a list."""
    path = RAW_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Raw file not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    logger.info(f"Read JSON ← {path}  ({len(data)} records)")
    return data
