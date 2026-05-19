"""
NexaMart M1 — Silver T3: surrogate key generation.

Deterministic SHA-256 hashes over natural keys, NULL-safe.
Idempotent by construction: same input always produces same key, so
re-running the pipeline doesn't break Gold dimension joins.

Why SHA-256:
- Collision probability is effectively zero for our row counts (< 1M rows).
- Hex output is 64 chars; fits in VARCHAR(64).
- Deterministic — no auto-increment surprises across reruns.

Why NULL-safe:
- A natural key with NULL components must still produce a stable key.
- We coalesce each component to '~NULL~' before concat, so NULL behaves
  like a sentinel value rather than poisoning the whole hash.
"""

from typing import Iterable
from pyspark.sql import Column
from pyspark.sql import functions as F


_NULL_SENTINEL = "~NULL~"
_SEPARATOR     = "|"


def surrogate_key(*cols: Column) -> Column:
    """
    Build a deterministic SHA-256 surrogate key from one or more natural-key columns.

    Args:
        *cols: one or more Spark Column expressions.

    Returns:
        StringType Column, 64-char hex SHA-256 digest.

    Example:
        df = df.withColumn(
            'customer_master_key',
            surrogate_key(F.col('email_lower'), F.col('phone_e164'))
        )

    NULL handling: each input is coalesced to '~NULL~' before concat,
    so the key is stable even when components are NULL.
    """
    if not cols:
        raise ValueError("surrogate_key requires at least one column")

    safe_cols = [
        F.coalesce(c.cast("string"), F.lit(_NULL_SENTINEL))
        for c in cols
    ]
    return F.sha2(F.concat_ws(_SEPARATOR, *safe_cols), 256)


def composite_business_key(*cols: Column) -> Column:
    """
    Human-readable composite key for debugging (NOT for joins — use surrogate_key).

    Returns the concatenated string with separator. Useful when surfacing
    the natural key alongside the surrogate in audit columns.

    Example:
        df = df.withColumn('cust_natkey_debug',
                           composite_business_key(F.col('email'), F.col('phone')))
    """
    if not cols:
        raise ValueError("composite_business_key requires at least one column")
    return F.concat_ws(
        _SEPARATOR,
        *[F.coalesce(c.cast("string"), F.lit(_NULL_SENTINEL)) for c in cols]
    )


def anonymous_key(prefix: str = "ANON") -> Column:
    """
    Constant surrogate key for the 'anonymous' bucket (e.g. guest checkouts
    with no contact info, customer identity confidence < 0.70).

    All anonymous rows route to the SAME surrogate key — they're treated
    as one logical entity for analytics purposes.

    Example:
        df = df.withColumn(
            'customer_master_key',
            F.when(F.col('identity_confidence') < 0.70, anonymous_key())
             .otherwise(surrogate_key(F.col('email_lower')))
        )
    """
    return F.sha2(F.lit(f"__{prefix}_BUCKET__"), 256)
