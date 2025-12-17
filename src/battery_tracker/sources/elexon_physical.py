from __future__ import annotations

import time
from typing import Any, Dict, List

import requests

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"
PHYSICAL_PATH = "/balancing/physical"


def _parse_payload(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValueError("Unexpected response format: expected a JSON object with a 'data' key.")
    if "data" not in payload:
        raise ValueError("Unexpected response format: missing 'data' key.")

    data = payload["data"]
    if not isinstance(data, list):
        raise ValueError("Unexpected response format: 'data' is not a list.")

    for record in data:
        if not isinstance(record, dict):
            raise ValueError("Unexpected response format: record is not a JSON object.")
    return data


def fetch_physical(from_ts: str, to_ts: str, bm_unit: str) -> List[Dict[str, Any]]:
    """Fetch physical notifications for the given window and BM Unit."""

    url = BASE_URL + PHYSICAL_PATH
    params = {"from": from_ts, "to": to_ts, "bmUnit": bm_unit}
    attempts = 3
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return _parse_payload(response.json())
        except Exception as exc:  # noqa: BLE001 - broad to include HTTP/JSON errors
            last_error = exc
            if attempt == attempts:
                break
            time.sleep(2**attempt)

    assert last_error is not None
    raise RuntimeError(
        f"Failed to fetch physical data for BM Unit {bm_unit} from {from_ts} to {to_ts}: {last_error}"
    )


__all__ = ["fetch_physical", "BASE_URL", "PHYSICAL_PATH"]
