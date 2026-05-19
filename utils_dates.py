"""
NexaMart M1 — Silver T1: date format utilities.

The 6 source date/timestamp formats and their PySpark conversions.
Failed parses NULL the column and append 'DATE_PARSE_FAIL' to the
anomaly_reason_code column (caller handles that part).

See docs/date_formats.md for full mapping of which source columns use which format.

⚠️  pg_transactions format is ambiguous: dictionary says YYYY-MM-DD HH:MM:SS,
    UNDERSTANDING.md says Unix epoch. M5 must verify on Day 2 before locking
    parse_pg_timestamp() below.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Format 1 — DD/MM/YYYY (POS, store inventory)
# Used by: pos_transactions.txn_date, si_inventory_*
# ---------------------------------------------------------------------------
def parse_ddmmyyyy(col: Column) -> Column:
    """
    Parse DD/MM/YYYY → DateType.
    Example: '08/08/2024' → 2024-08-08
    Example: '01/08/2024' → 2024-08-01 (1 August, not 8 January)
    """
    return F.to_date(col, "dd/MM/yyyy")


# ---------------------------------------------------------------------------
# Format 2 — YYYY-MM-DD (ISO date) — most ec_*, wh_*, dc_*, rr_*, ts_*
# ---------------------------------------------------------------------------
def parse_iso_date(col: Column) -> Column:
    """
    Parse YYYY-MM-DD → DateType.
    Spark auto-parses, but explicit format is safer.
    """
    return F.to_date(col, "yyyy-MM-dd")


# ---------------------------------------------------------------------------
# Format 3 — ISO 8601 with T separator (delivery, NL, reviews)
# ---------------------------------------------------------------------------
def parse_iso_timestamp(col: Column) -> Column:
    """
    Parse YYYY-MM-DDTHH:MM:SS → TimestampType.
    Spark's to_timestamp without a format auto-handles ISO 8601.
    """
    return F.to_timestamp(col)


# ---------------------------------------------------------------------------
# Format 4 — DD-Mon-YYYY (rr_return_requests only)
# ---------------------------------------------------------------------------
def parse_ddmonyyyy(col: Column) -> Column:
    """
    Parse DD-MMM-YYYY → DateType.
    Example: '05-Sep-2024' → 2024-09-05
    """
    return F.to_date(col, "dd-MMM-yyyy")


# ---------------------------------------------------------------------------
# Format 5 — YYYY/MM/DD HH:MM (cs_cases, cs_case_events)
# ---------------------------------------------------------------------------
def parse_slash_datetime(col: Column) -> Column:
    """
    Parse YYYY/MM/DD HH:MM → TimestampType.
    Example: '2024/08/08 14:23' → 2024-08-08 14:23:00
    """
    return F.to_timestamp(col, "yyyy/MM/dd HH:mm")


# ---------------------------------------------------------------------------
# Format 6 — Unix epoch integer (pg_transactions)
# ⚠️ See module docstring; M5 must verify the actual format on Day 2.
# ---------------------------------------------------------------------------
def parse_unix_epoch(col: Column) -> Column:
    """
    Parse Unix epoch (integer seconds since 1970-01-01 UTC) → TimestampType.
    Example: 1723046400 → 2024-08-07 16:00:00 UTC
    """
    return F.from_unixtime(col).cast("timestamp")


def parse_pg_timestamp(col: Column) -> Column:
    """
    pg_transactions timestamp parser. Currently configured for Unix epoch
    per UNDERSTANDING.md. If M5 finds the actual values are
    'YYYY-MM-DD HH:MM:SS' strings, swap to:
        return F.to_timestamp(col, 'yyyy-MM-dd HH:mm:ss')
    """
    return parse_unix_epoch(col)


# ---------------------------------------------------------------------------
# Dispatcher — pick a parser by hint name
# ---------------------------------------------------------------------------
_DISPATCH = {
    "ddmmyyyy":          parse_ddmmyyyy,
    "iso_date":          parse_iso_date,
    "iso_timestamp":     parse_iso_timestamp,
    "ddmonyyyy":         parse_ddmonyyyy,
    "slash_datetime":    parse_slash_datetime,
    "unix_epoch":        parse_unix_epoch,
    "pg_timestamp":      parse_pg_timestamp,
}


def parse_date(col: Column, format_hint: str) -> Column:
    """
    Dispatch a date column to the appropriate parser by hint name.

    Args:
        col: Spark Column to parse.
        format_hint: one of: ddmmyyyy, iso_date, iso_timestamp, ddmonyyyy,
                     slash_datetime, unix_epoch, pg_timestamp.

    Returns:
        Spark Column of DateType or TimestampType. NULL on parse failure
        (caller is responsible for flagging via anomaly_reason_code).

    Raises:
        ValueError: unknown format_hint.
    """
    if format_hint not in _DISPATCH:
        raise ValueError(
            f"Unknown format_hint '{format_hint}'. "
            f"Valid: {sorted(_DISPATCH.keys())}"
        )
    return _DISPATCH[format_hint](col)


def is_parse_failure(parsed_col: Column, raw_col: Column) -> Column:
    """
    Returns a BooleanType column: TRUE where the raw value was non-null
    but parsed value is null (= parse failed).

    Use this to drive the DATE_PARSE_FAIL flag:
        df = df.withColumn(
            'order_date_parsed',
            parse_date(F.col('order_date_raw'), 'iso_date')
        )
        df = df.withColumn(
            'parse_failed',
            is_parse_failure(F.col('order_date_parsed'), F.col('order_date_raw'))
        )
        df = flag(df, F.col('parse_failed'),
                  reason_code='DATE_PARSE_FAIL',
                  status='FLAGGED_ANOMALY',
                  certainty='UNRELIABLE')
    """
    return raw_col.isNotNull() & parsed_col.isNull()
