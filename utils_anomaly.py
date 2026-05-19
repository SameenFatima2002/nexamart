"""
NexaMart M1 — Silver mandatory column helpers.

Every Silver table must carry these 4 columns:
- anomaly_flag           : BooleanType
- anomaly_reason_code    : StringType (comma-separated when multiple codes apply)
- data_quality_status    : StringType in {CLEAN, FLAGGED_ANOMALY,
                                          FLAGGED_AMBIGUOUS, EXCLUDED_WITH_REASON,
                                          RECONSTRUCTED}
- metric_certainty_level : StringType in {CONFIRMED, INFERRED, ESTIMATED, UNRELIABLE}

Use add_anomaly_columns() once at the start of each Silver table build,
then flag() to mark specific conditions. Both are NULL-safe and idempotent.

Reason codes must come from docs/anomaly_taxonomy.md. The code list below
is duplicated for IDE autocomplete only — the doc is authoritative.
"""

from typing import Optional
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F


# Severity rankings (higher index = more severe / less certain)
_STATUS_RANK = {
    "CLEAN":                 0,
    "RECONSTRUCTED":         1,
    "FLAGGED_AMBIGUOUS":     2,
    "FLAGGED_ANOMALY":       3,
    "EXCLUDED_WITH_REASON":  4,
}

_CERTAINTY_RANK = {
    "CONFIRMED":  0,
    "INFERRED":   1,
    "ESTIMATED":  2,
    "UNRELIABLE": 3,
}


def add_anomaly_columns(
    df: DataFrame,
    default_status: str = "CLEAN",
    default_certainty: str = "CONFIRMED",
) -> DataFrame:
    """
    Initialise the 4 mandatory columns on a Silver DataFrame.

    Args:
        df: input Spark DataFrame.
        default_status: starting data_quality_status (default CLEAN).
        default_certainty: starting metric_certainty_level (default CONFIRMED).

    Returns:
        DataFrame with 4 new columns appended, all rows initially CLEAN/CONFIRMED.

    Example:
        df = (spark.read.format('snowflake').options(**sf_opts)
              .option('dbtable', 'cl_customers').load())
        df = add_anomaly_columns(df)
    """
    if default_status not in _STATUS_RANK:
        raise ValueError(f"Invalid default_status '{default_status}'. "
                         f"Valid: {sorted(_STATUS_RANK.keys())}")
    if default_certainty not in _CERTAINTY_RANK:
        raise ValueError(f"Invalid default_certainty '{default_certainty}'. "
                         f"Valid: {sorted(_CERTAINTY_RANK.keys())}")

    return (df
            .withColumn("anomaly_flag", F.lit(False))
            .withColumn("anomaly_reason_code", F.lit(None).cast("string"))
            .withColumn("data_quality_status", F.lit(default_status))
            .withColumn("metric_certainty_level", F.lit(default_certainty)))


def flag(
    df: DataFrame,
    condition: Column,
    reason_code: str,
    status: str = "FLAGGED_ANOMALY",
    certainty: Optional[str] = None,
) -> DataFrame:
    """
    Mark rows matching `condition` with an anomaly reason code and update
    data_quality_status / metric_certainty_level.

    Combining rules when a row already carries flags:
    - anomaly_flag becomes/stays TRUE
    - anomaly_reason_code: comma-separated append (deduplicated)
    - data_quality_status: keep the more severe (higher rank)
    - metric_certainty_level: keep the less certain (higher rank)
        - if `certainty` arg is None, only the reason_code/status are updated.

    Args:
        df: input DataFrame (must already have add_anomaly_columns applied).
        condition: BooleanType Spark Column expression.
        reason_code: code from docs/anomaly_taxonomy.md.
        status: target status (defaults to FLAGGED_ANOMALY).
        certainty: optional certainty level; only updates if more severe than current.

    Example:
        df = flag(
            df,
            condition=(F.col('order_status') == 'CANCELLED') & (F.col('subtotal_excl_tax') > 0),
            reason_code='CANCELLED_WITH_REVENUE',
            status='FLAGGED_ANOMALY',
            certainty='UNRELIABLE',
        )
    """
    if status not in _STATUS_RANK:
        raise ValueError(f"Invalid status '{status}'. "
                         f"Valid: {sorted(_STATUS_RANK.keys())}")
    if certainty is not None and certainty not in _CERTAINTY_RANK:
        raise ValueError(f"Invalid certainty '{certainty}'. "
                         f"Valid: {sorted(_CERTAINTY_RANK.keys())}")

    # Build the appended reason_code: dedupe via array→array_distinct→join
    new_reason_array = F.when(
        condition,
        F.when(
            F.col("anomaly_reason_code").isNull(),
            F.array(F.lit(reason_code))
        ).otherwise(
            F.array_distinct(
                F.array_union(
                    F.split(F.col("anomaly_reason_code"), ","),
                    F.array(F.lit(reason_code))
                )
            )
        )
    ).otherwise(
        F.when(F.col("anomaly_reason_code").isNotNull(),
               F.split(F.col("anomaly_reason_code"), ","))
         .otherwise(F.array().cast("array<string>"))
    )

    # Status: keep the more severe rank
    new_status = F.when(
        condition,
        _max_status_expr(F.col("data_quality_status"), F.lit(status))
    ).otherwise(F.col("data_quality_status"))

    # Certainty: keep the less certain rank (only if certainty arg provided)
    if certainty is None:
        new_certainty = F.col("metric_certainty_level")
    else:
        new_certainty = F.when(
            condition,
            _max_certainty_expr(F.col("metric_certainty_level"), F.lit(certainty))
        ).otherwise(F.col("metric_certainty_level"))

    return (df
            .withColumn("anomaly_flag",
                        F.col("anomaly_flag") | condition)
            .withColumn("anomaly_reason_code",
                        F.when(F.size(new_reason_array) > 0,
                               F.concat_ws(",", new_reason_array))
                         .otherwise(F.lit(None).cast("string")))
            .withColumn("data_quality_status", new_status)
            .withColumn("metric_certainty_level", new_certainty))


# ---------------------------------------------------------------------------
# Internal: rank-based MAX expressions for status / certainty escalation
# ---------------------------------------------------------------------------
def _max_status_expr(a: Column, b: Column) -> Column:
    """Return whichever status has the higher _STATUS_RANK."""
    return _max_by_rank(a, b, _STATUS_RANK)


def _max_certainty_expr(a: Column, b: Column) -> Column:
    """Return whichever certainty has the higher _CERTAINTY_RANK (= less certain)."""
    return _max_by_rank(a, b, _CERTAINTY_RANK)


def _max_by_rank(a: Column, b: Column, ranks: dict) -> Column:
    """
    Compare two string columns by an external rank dict; return the
    higher-ranked value. Implemented via chained when() so it works in pure SQL.
    """
    # Build expr: rank(a) >= rank(b) ? a : b
    rank_a = _to_rank_expr(a, ranks)
    rank_b = _to_rank_expr(b, ranks)
    return F.when(rank_a >= rank_b, a).otherwise(b)


def _to_rank_expr(col: Column, ranks: dict) -> Column:
    """Map string column to its numeric rank using a chained when()."""
    expr = F.lit(-1)
    for value, rank in ranks.items():
        expr = F.when(col == value, F.lit(rank)).otherwise(expr)
    return expr


# ---------------------------------------------------------------------------
# Convenience flags for very common cases
# ---------------------------------------------------------------------------
def flag_date_parse_failure(df: DataFrame, parsed_col: str, raw_col: str) -> DataFrame:
    """
    One-liner to flag DATE_PARSE_FAIL on rows where raw was non-null
    but parsed is null.
    """
    condition = F.col(raw_col).isNotNull() & F.col(parsed_col).isNull()
    return flag(df, condition,
                reason_code="DATE_PARSE_FAIL",
                status="FLAGGED_ANOMALY",
                certainty="UNRELIABLE")


def flag_orphan_fk(df: DataFrame, fk_col: str) -> DataFrame:
    """
    Flag rows where the joined target was missing (fk_col is null after a left join).
    Use after a `df.join(target, on=..., how='left')` where you've named the
    target's PK column distinctively.
    """
    condition = F.col(fk_col).isNull()
    return flag(df, condition,
                reason_code="ORPHAN_FK",
                status="FLAGGED_ANOMALY",
                certainty="UNRELIABLE")
