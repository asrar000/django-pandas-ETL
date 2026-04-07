"""
Centralised path constants for the ETL pipeline.
All other modules import paths from here — never hard-code paths elsewhere.
"""
from pathlib import Path
from config.loader import get

# Absolute project root (two levels up from this file)
BASE_DIR: Path = Path(__file__).resolve().parent.parent

RAW_DIR: Path      = BASE_DIR / get("paths.raw_dir",       "data/raw")
PROCESSED_DIR: Path = BASE_DIR / get("paths.processed_dir", "data/processed")
ICEBERG_DIR: Path  = BASE_DIR / get("paths.iceberg_dir",   "data/iceberg")
ICEBERG_WAREHOUSE_DIR: Path = BASE_DIR / get(
    "iceberg.warehouse_dir",
    "data/iceberg/warehouse",
)
LOGS_DIR: Path     = BASE_DIR / get("paths.logs_dir",       "logs")


def ensure_dirs() -> None:
    """Create all pipeline directories if they do not already exist."""
    for directory in (RAW_DIR, PROCESSED_DIR, ICEBERG_DIR, ICEBERG_WAREHOUSE_DIR, LOGS_DIR):
        directory.mkdir(parents=True, exist_ok=True)
