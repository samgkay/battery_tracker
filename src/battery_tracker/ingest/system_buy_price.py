from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal
from typing import Iterable, List, Sequence, Tuple

import psycopg2
from psycopg2.extensions import connection as PGConnection

from battery_tracker.sources.elexon import ElexonAPIError, fetch_system_buy_prices_for_date


def settlement_period_to_utc(settlement_date: date, settlement_period: int) -> datetime:
    if settlement_period < 1:
        raise ValueError(f"Invalid settlement period {settlement_period}; must be >= 1")
    start = datetime.combine(settlement_date, dt_time(0, 0, tzinfo=timezone.utc))
    return start + timedelta(minutes=30 * (settlement_period - 1))


def normalize_records(raw_records: Iterable[dict]) -> List[Tuple[datetime, Decimal]]:
    normalized: List[Tuple[datetime, Decimal]] = []

    for idx, record in enumerate(raw_records):
        settlement_date = record.get("settlementDate")
        period = record.get("settlementPeriod")
        sbp_value = record.get("systemBuyPrice")

        if settlement_date is None or period is None or sbp_value is None:
            raise ElexonAPIError(f"Record {idx} missing required fields: {record!r}")

        try:
            parsed_date = date.fromisoformat(str(settlement_date))
        except Exception as exc:
            raise ElexonAPIError(f"Invalid settlementDate in record {idx}: {settlement_date!r}") from exc

        try:
            parsed_period = int(period)
        except Exception as exc:
            raise ElexonAPIError(f"Invalid settlementPeriod in record {idx}: {period!r}") from exc

        try:
            sbp_decimal = Decimal(str(sbp_value))
        except Exception as exc:
            raise ElexonAPIError(f"Invalid systemBuyPrice in record {idx}: {sbp_value!r}") from exc

        ts = settlement_period_to_utc(parsed_date, parsed_period)
        normalized.append((ts, sbp_decimal))

    return normalized


def upsert_system_buy_prices(conn: PGConnection, rows: Sequence[Tuple[datetime, Decimal]]) -> int:
    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO system_buy_price (ts, sbp_gbp_per_mwh)
            VALUES (%s, %s)
            ON CONFLICT (ts) DO UPDATE
            SET sbp_gbp_per_mwh = EXCLUDED.sbp_gbp_per_mwh,
                ingested_at = now()
            """,
            rows,
        )
    return len(rows)


def ingest_system_buy_price_for_date(conn: PGConnection, target_date: date) -> Tuple[int, int]:
    raw_records = fetch_system_buy_prices_for_date(target_date)
    normalized = normalize_records(raw_records)
    upserted = upsert_system_buy_prices(conn, normalized)
    return len(normalized), upserted


def backfill_system_buy_price_2025(database_url: str) -> None:
    start = date(2025, 1, 1)
    end = date(2025, 12, 31)

    with psycopg2.connect(database_url) as conn:
        current = start
        while current <= end:
            records_fetched, rows_upserted = ingest_system_buy_price_for_date(conn, current)
            conn.commit()
            print(
                f"Processed {current.isoformat()}: fetched {records_fetched} records, upserted {rows_upserted} rows",
                flush=True,
            )
            current += timedelta(days=1)
