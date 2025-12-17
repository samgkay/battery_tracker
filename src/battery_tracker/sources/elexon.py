"""
Elexon BMRS API client helpers.

The System Buy Price (SBP) data is exposed via the ``/balancing/system-prices``
endpoint on https://data.elexon.co.uk/bmrs/api/v1. The previously suggested
``/balancing/pricing/market-index`` endpoint serves Market Index Price (MIP)
data rather than SBP/SSP values, so this module targets the system-prices
endpoint instead.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable, List, Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"
SYSTEM_PRICES_ENDPOINT = f"{BASE_URL}/balancing/system-prices"


class ElexonAPIError(RuntimeError):
    pass


def _isoformat_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso_datetime(value: str) -> datetime:
    # Minimal ISO-8601 parser that supports the Z suffix.
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _settlement_period_start(date_str: str, period: int) -> datetime:
    base = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    return base + timedelta(minutes=30 * (period - 1))


def _normalize_record(record: dict) -> Optional[dict]:
    price = record.get("systemBuyPrice")
    if price is None:
        return None

    ts: Optional[datetime] = None
    for key in (
        "startTime",
        "settlementStartTime",
        "settlementPeriodStart",
        "settlementPeriodCommencing",
    ):
        raw_ts = record.get(key)
        if raw_ts:
            ts = _parse_iso_datetime(str(raw_ts))
            break

    if ts is None and record.get("settlementDate") and record.get("settlementPeriod"):
        try:
            ts = _settlement_period_start(str(record["settlementDate"]), int(record["settlementPeriod"]))
        except Exception as exc:  # noqa: BLE001 - propagate normalized error below
            raise ElexonAPIError(
                f"Unable to derive timestamp from settlementDate/settlementPeriod: {exc}"
            ) from exc

    if ts is None:
        raise ElexonAPIError("SBP record missing a parsable timestamp field")

    return {
        "ts": ts,
        "sbp_gbp_per_mwh": Decimal(str(price)),
    }


def _request_json(url: str, params: dict, retries: int = 3, backoff_seconds: float = 1.0) -> dict:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=20)
            if response.status_code >= 500:
                raise ElexonAPIError(
                    f"Elexon API server error {response.status_code} for {response.url}: {response.text}"
                )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            logger.warning("Elexon request failed (attempt %s/%s): %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(backoff_seconds * attempt)
            else:
                break
    raise ElexonAPIError(f"Failed to fetch data from {url} with params {params}: {last_error}")


def fetch_system_buy_prices(start: datetime, end: datetime) -> List[dict]:
    """
    Fetch System Buy Price rows between ``start`` and ``end`` (inclusive).

    The BMRS API returns both system buy and sell prices; this helper keeps
    only the SBP fields and normalises timestamps to UTC ``datetime`` objects.
    """
    if end < start:
        raise ValueError("end must be greater than or equal to start")

    params = {"from": _isoformat_utc(start), "to": _isoformat_utc(end)}
    payload = _request_json(SYSTEM_PRICES_ENDPOINT, params=params)
    records: Iterable[dict]
    if isinstance(payload, dict) and "data" in payload:
        records = payload.get("data") or []
    elif isinstance(payload, list):
        records = payload
    else:
        raise ElexonAPIError(f"Unexpected response structure from Elexon: {payload}")

    normalized: List[dict] = []
    for record in records:
        normalized_record = _normalize_record(record)
        if normalized_record is not None:
            normalized.append(normalized_record)

    return normalized
