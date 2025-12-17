from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable, List

import psycopg2

from battery_tracker.sources.elexon import ElexonAPIError, fetch_system_buy_prices

logger = logging.getLogger(__name__)


def _normalize_rows(raw_rows: Iterable[dict]) -> List[dict]:
    normalized: List[dict] = []
    for row in raw_rows:
        ts = row["ts"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)

        price = row["sbp_gbp_per_mwh"]
        if not isinstance(price, Decimal):
            price = Decimal(str(price))

        normalized.append({"ts": ts, "sbp_gbp_per_mwh": price})
    return normalized


def upsert_rows(conn, rows: Iterable[dict]) -> int:
    rows = list(rows)
    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO system_buy_price (ts, sbp_gbp_per_mwh)
            VALUES (%(ts)s, %(sbp_gbp_per_mwh)s)
            ON CONFLICT (ts) DO UPDATE SET sbp_gbp_per_mwh = EXCLUDED.sbp_gbp_per_mwh
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def backfill(conn, start: datetime, end: datetime, chunk_days: int = 7) -> int:
    """
    Backfill SBP data between the provided UTC datetimes.
    Data is fetched in chunks to avoid overly large API requests.
    """
    if start.tzinfo is None or start.utcoffset() != timedelta(0):
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None or end.utcoffset() != timedelta(0):
        end = end.replace(tzinfo=timezone.utc)

    total = 0
    current_start = start
    delta = timedelta(days=chunk_days)

    while current_start <= end:
        current_end = min(current_start + delta, end)
        logger.info("Requesting SBP data %s -> %s", current_start, current_end)
        try:
            fetched = fetch_system_buy_prices(current_start, current_end)
        except ElexonAPIError:
            logger.exception("Failed to fetch SBP data for %s to %s", current_start, current_end)
            raise

        rows = _normalize_rows(fetched)
        inserted = upsert_rows(conn, rows)
        total += inserted

        logger.info(
            "Chunk complete: %s rows fetched, %s rows inserted/updated", len(rows), inserted
        )

        # Advance to the next chunk; overlap by 30 minutes to avoid potential off-by-one gaps.
        current_start = current_end + timedelta(minutes=30)

    return total


def backfill_2025(conn) -> int:
    start = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(2025, 12, 31, 23, 59, tzinfo=timezone.utc)
    return backfill(conn, start, end)
