from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import psycopg2

from battery_tracker.sources.elexon import SELL_PRICE_KEYS


def settlement_period_to_utc(settlement_date: date, settlement_period: int) -> datetime:
    if settlement_period < 1:
        raise ValueError("settlement_period must be >= 1")
    start_of_day = datetime.combine(settlement_date, time(0, 0, tzinfo=timezone.utc))
    return start_of_day + timedelta(minutes=30 * (settlement_period - 1))


def _get_settlement_date(record: Dict[str, Any]) -> date:
    value = record.get("settlementDate") or record.get("settlement_date")
    if not value:
        raise ValueError("Record missing settlement date")
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _get_settlement_period(record: Dict[str, Any]) -> int:
    value = (
        record.get("settlementPeriod")
        or record.get("settlement_period")
        or record.get("period")
    )
    if value is None:
        raise ValueError("Record missing settlement period")
    return int(value)


def _get_sell_price(record: Dict[str, Any]) -> Decimal:
    for key in SELL_PRICE_KEYS:
        if key in record:
            return Decimal(str(record[key]))
    raise ValueError("Record missing sell price")


def normalize_records(records: Iterable[Dict[str, Any]]) -> List[Tuple[datetime, Decimal]]:
    normalized: List[Tuple[datetime, Decimal]] = []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("Record is not a mapping")
        settlement_date = _get_settlement_date(record)
        settlement_period = _get_settlement_period(record)
        ts = settlement_period_to_utc(settlement_date, settlement_period)
        price = _get_sell_price(record)
        normalized.append((ts, price))
    return normalized


def upsert_system_sell_prices(conn, rows: Sequence[Tuple[datetime, Decimal]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO system_sell_price (ts, ssp_gbp_per_mwh)
            VALUES (%s, %s)
            ON CONFLICT (ts) DO UPDATE
            SET ssp_gbp_per_mwh = EXCLUDED.ssp_gbp_per_mwh,
                ingested_at = NOW()
            """,
            rows,
        )
    conn.commit()


def backfill_system_sell_price_2025(database_url: str) -> None:
    from battery_tracker.sources.elexon import fetch_system_prices_for_date

    start_date = date(2025, 1, 1)
    end_date = date(2025, 12, 31)

    current_date = start_date
    with psycopg2.connect(database_url) as conn:
        while current_date <= end_date:
            records = fetch_system_prices_for_date(current_date)
            rows = normalize_records(records)
            upsert_system_sell_prices(conn, rows)
            print(
                f"{current_date}: fetched {len(records)} records, upserted {len(rows)} rows",
                flush=True,
            )
            current_date += timedelta(days=1)


__all__ = [
    "backfill_system_sell_price_2025",
    "normalize_records",
    "settlement_period_to_utc",
    "upsert_system_sell_prices",
]
