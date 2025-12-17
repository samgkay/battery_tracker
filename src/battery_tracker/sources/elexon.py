from __future__ import annotations

import time
from datetime import date
from typing import Any, Dict, List

import requests

BASE_URL = "https://data.elexon.co.uk/balancing/settlement/system-prices"


class ElexonAPIError(Exception):
    """Raised when the Elexon API returns unexpected data."""


def _validate_record(record: Dict[str, Any], index: int) -> None:
    if not isinstance(record, dict):
        raise ElexonAPIError(f"Record {index} is not an object: {record!r}")

    if "systemBuyPrice" not in record:
        raise ElexonAPIError(
            "Missing 'systemBuyPrice' in response record. Ensure the System Buy Price endpoint is used, not System Sell Price."
        )

    if "settlementDate" not in record or "settlementPeriod" not in record:
        raise ElexonAPIError(
            "Response record missing 'settlementDate' or 'settlementPeriod'; the system prices endpoint contract may have changed."
        )


def fetch_system_buy_prices_for_date(
    settlement_date: date,
    *,
    timeout: float = 15.0,
    retries: int = 3,
    backoff: float = 1.0,
) -> List[Dict[str, Any]]:
    """Fetch system buy prices for a given settlement date.

    The response is validated to ensure System Buy Price data is present
    and that settlement identifiers are included.
    """

    url = f"{BASE_URL}/{settlement_date.isoformat()}"
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
        except Exception as exc:  # requests fallback raises RequestException
            last_error = exc
        else:
            if response.status_code != 200:
                last_error = ElexonAPIError(
                    f"Unexpected status {response.status_code} for {url}. Expected 200 to confirm SBP endpoint."
                )
            else:
                try:
                    payload = response.json()
                except Exception as exc:  # pragma: no cover - defensive parsing
                    raise ElexonAPIError(f"Failed to parse JSON from {url}") from exc

                if not isinstance(payload, dict):
                    raise ElexonAPIError(f"Unexpected response type {type(payload)} from {url}")

                data = payload.get("data")
                if not isinstance(data, list):
                    raise ElexonAPIError(
                        "Response missing 'data' list. Verify the system prices endpoint and contract."
                    )

                for idx, record in enumerate(data):
                    _validate_record(record, idx)

                return data

        if attempt < retries:
            time.sleep(backoff * attempt)

    raise ElexonAPIError(f"Failed to fetch system buy prices for {settlement_date}: {last_error}")
