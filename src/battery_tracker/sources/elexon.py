from __future__ import annotations

import time
from datetime import date
from typing import Any, Dict, Iterable, List

import requests

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"
SYSTEM_PRICES_PATH = "/balancing/settlement/system-prices/{settlement_date}"

# Candidate keys that may contain the system sell price in the API response.
SELL_PRICE_KEYS: tuple[str, ...] = (
    "sellPrice",
    "systemSellPrice",
    "sell_price",
    "ssp",
)


def _extract_sell_price(record: Dict[str, Any]) -> Any:
    for key in SELL_PRICE_KEYS:
        if key in record:
            return record[key]
    raise ValueError("No sell-price field found in record.")


def _parse_response_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        if "data" in payload:
            payload = payload["data"]
        else:
            # If the top level is a dict without a data key, treat it as an error.
            raise ValueError("Unexpected response format: missing 'data' key.")

    if not isinstance(payload, Iterable) or isinstance(payload, (str, bytes)):
        raise ValueError("Unexpected response format: expected a list of records.")

    records = list(payload)
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("Unexpected response format: record is not a JSON object.")
        _ = _extract_sell_price(record)
    return records


def fetch_system_prices_for_date(settlement_date: date) -> List[Dict[str, Any]]:
    url = BASE_URL + SYSTEM_PRICES_PATH.format(settlement_date=settlement_date.isoformat())
    attempts = 3
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            payload = response.json()
            return _parse_response_payload(payload)
        except Exception as exc:  # noqa: BLE001 - broad to include HTTP/JSON errors
            last_error = exc
            if attempt == attempts:
                break
            time.sleep(2 ** attempt)

    assert last_error is not None
    raise RuntimeError(f"Failed to fetch system prices for {settlement_date}: {last_error}")


__all__ = ["fetch_system_prices_for_date", "SELL_PRICE_KEYS"]
