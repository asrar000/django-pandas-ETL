"""OpenSearch client and config helpers."""

from __future__ import annotations

from typing import Any

from opensearchpy import OpenSearch

from config.loader import get


def as_bool(value: Any) -> bool:
    """Normalize common truthy and falsy config values into a boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def build_opensearch_client() -> OpenSearch:
    """Build an OpenSearch client from config/config.yml."""
    username = str(get("opensearch.username", "") or "")
    password = str(get("opensearch.password", "") or "")
    verify_certs = as_bool(get("opensearch.verify_certs", False))

    return OpenSearch(
        hosts=[
            {
                "host": get("opensearch.host", "localhost"),
                "port": int(get("opensearch.port", 9200)),
            }
        ],
        http_auth=(username, password) if username and password else None,
        use_ssl=as_bool(get("opensearch.use_ssl", False)),
        verify_certs=verify_certs,
        ssl_assert_hostname=verify_certs,
        ssl_show_warn=verify_certs,
        timeout=int(get("opensearch.timeout", 30)),
        http_compress=True,
    )
