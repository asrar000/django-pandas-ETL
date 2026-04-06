"""
Config loader — single source of truth for all configuration values.
Usage:
    from config.loader import get
    db_host = get("database.host")
"""
from pathlib import Path
import yaml

_CONFIG: dict | None = None
_CONFIG_PATH = Path(__file__).parent / "config.yml"


def load_config() -> dict:
    """Load and cache config.yml. Thread-safe for read-after-init."""
    global _CONFIG
    if _CONFIG is None:
        with open(_CONFIG_PATH, "r", encoding="utf-8") as fh:
            _CONFIG = yaml.safe_load(fh)
    return _CONFIG


def get(key_path: str, default=None):
    """
    Retrieve a nested config value using dot-notation.

    Examples:
        get("api.base_url")          → "https://dummyjson.com"
        get("database.port")         → 5432
        get("missing.key", "oops")   → "oops"
    """
    cfg = load_config()
    for key in key_path.split("."):
        if isinstance(cfg, dict):
            cfg = cfg.get(key)
        else:
            return default
        if cfg is None:
            return default
    return cfg
