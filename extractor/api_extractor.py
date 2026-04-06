"""
API Extractor — fetches raw cart and user data from dummyjson.com.
Implements retry with exponential backoff on all transient failures.
"""
import time
from typing import Optional

import requests

from config.loader import get
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
_BASE_URL: str      = get("api.base_url",        "https://dummyjson.com")
_MAX_RETRIES: int   = int(get("api.max_retries",  5))
_BACKOFF: int       = int(get("api.backoff_factor", 2))
_TIMEOUT: int       = int(get("api.timeout",      30))

# HTTP status codes that should NOT be retried (client errors)
_NO_RETRY_CODES = {400, 401, 403, 404, 422}


def _fetch_with_retry(url: str, params: Optional[dict] = None) -> dict:
    """
    GET *url* with automatic retries and exponential back-off.

    Retry schedule (BACKOFF_FACTOR=2):
        attempt 0 → immediate
        attempt 1 → wait 2 s
        attempt 2 → wait 4 s
        attempt 3 → wait 8 s
        attempt 4 → wait 16 s
    """
    last_exc: Exception | None = None

    for attempt in range(_MAX_RETRIES):
        try:
            logger.info(f"GET {url}  [attempt {attempt + 1}/{_MAX_RETRIES}]")
            resp = requests.get(url, params=params, timeout=_TIMEOUT)
            resp.raise_for_status()
            logger.info(f"✓ HTTP {resp.status_code}  ←  {url}")
            return resp.json()

        except requests.exceptions.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else 0
            logger.warning(f"HTTP {code} on {url}: {exc}")
            if code in _NO_RETRY_CODES:
                raise  # don't retry genuine client errors
            last_exc = exc

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            logger.warning(f"Network error on {url}: {exc}")
            last_exc = exc

        except requests.exceptions.RequestException as exc:
            logger.warning(f"Request error on {url}: {exc}")
            last_exc = exc

        if attempt < _MAX_RETRIES - 1:
            wait = _BACKOFF ** attempt
            logger.info(f"  ↻  Retrying in {wait}s…")
            time.sleep(wait)

    raise RuntimeError(
        f"All {_MAX_RETRIES} attempts failed for {url}"
    ) from last_exc


def fetch_carts(limit: int = 100) -> list:
    """Fetch cart/order objects from the API."""
    endpoint = get("api.carts_endpoint", "/carts")
    data = _fetch_with_retry(f"{_BASE_URL}{endpoint}", params={"limit": limit})
    carts = data.get("carts", [])
    logger.info(f"Fetched {len(carts)} carts")
    return carts


def fetch_users(limit: int = 100) -> list:
    """Fetch user objects from the API."""
    endpoint = get("api.users_endpoint", "/users")
    data = _fetch_with_retry(f"{_BASE_URL}{endpoint}", params={"limit": limit})
    users = data.get("users", [])
    logger.info(f"Fetched {len(users)} users")
    return users


def extract_all() -> tuple[list, list]:
    """
    Top-level extraction entry point.
    Returns (carts, users) as plain Python lists.
    """
    logger.info("═══ Starting Data Extraction ═══")
    limit = int(get("api.limit", 100))
    carts = fetch_carts(limit=limit)
    users = fetch_users(limit=limit)
    logger.info(f"Extraction complete — {len(carts)} carts | {len(users)} users")
    return carts, users
