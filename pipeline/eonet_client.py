"""Shared helpers for fetching data from the NASA EONET REST API (v2.1).

EONET is open-access; no API key is required. API docs:
https://eonet.gsfc.nasa.gov/docs/v2.1
"""

import requests

BASE_URL = "https://eonet.gsfc.nasa.gov/api/v2.1"

# Events window fetched on each pipeline refresh (matches the notebook build).
EVENTS_LOOKBACK_DAYS = 365


def fetch_endpoint(endpoint: str, params: dict | None = None) -> dict:
    """GET {BASE_URL}/{endpoint} and return the parsed JSON payload."""
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params or {}, timeout=60)
    response.raise_for_status()
    return response.json()
