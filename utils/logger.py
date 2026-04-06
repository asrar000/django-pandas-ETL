"""
Centralised logger factory.
All modules call get_logger(__name__) — never configure logging elsewhere.
"""
import logging
import sys
from config.loader import get
from utils.paths import LOGS_DIR


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger with both console and rotating-file handlers.
    Handlers are attached only once even when called multiple times.
    """
    logger = logging.getLogger(name)
    if logger.handlers:          # already configured — return as-is
        return logger

    level_str: str = get("logging.level", "INFO")
    level: int = getattr(logging, level_str.upper(), logging.INFO)
    fmt: str = get(
        "logging.format",
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    logger.setLevel(level)
    formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")

    # ── Console handler ───────────────────────────────────────────────────────
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # ── File handler ──────────────────────────────────────────────────────────
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOGS_DIR / "etl_pipeline.log", encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.propagate = False
    return logger
