"""
NexaMart M1 — Snowflake I/O helper for Databricks Free Edition (serverless).

Why this exists:
The original assignment brief (Section 7.4) instructed installing the
spark-snowflake Maven JAR on a Databricks cluster:

    Cluster → Libraries → Install New → Maven →
    net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4

Then writing via the Spark connector:

    df.write.format('net.snowflake.spark.snowflake').options(...).save()

That instruction targets Databricks Community Edition, which Databricks
discontinued in 2024-2025 in favour of Databricks Free Edition. Free Edition
runs notebooks on serverless Spark Connect — there is NO cluster Libraries
tab, and Maven JARs cannot be installed at the workspace level.

This module replaces the spark-snowflake JAR with snowflake-connector-python
(pure Python, pre-installable via `%pip install snowflake-connector-python`
at the top of any notebook). It uses the connector's `write_pandas()` helper
under the hood, which streams in chunks via Snowflake's PUT/COPY internally.

Performance: the largest table (`si_inventory_movements`, 438k rows) writes
in ~1-2 minutes on Free Edition serverless. For our 843,304 total rows this
adds ~3-4 minutes total to Bronze ingestion vs the JAR path. Acceptable.

Required at the top of every notebook that uses this helper:

    %pip install -q snowflake-connector-python
    dbutils.library.restartPython()

    dbutils.widgets.text("sf_account",   "rhxendw-yb24678")
    dbutils.widgets.text("sf_user",      "NEXAMART_LEAD")
    dbutils.widgets.text("sf_password",  "")  # paste at notebook run time
    dbutils.widgets.text("sf_warehouse", "NEXAMART_WH")
    dbutils.widgets.text("sf_role",      "ACCOUNTADMIN")

Then:
    import sys; sys.path.append('/Workspace/<repo path>/notebooks/_shared')
    from utils_snowflake import write_to_snowflake, read_from_snowflake
"""

from __future__ import annotations

from typing import Iterable
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _widget(name: str) -> str:
    """Read a Databricks widget; raise a helpful message if missing."""
    try:
        # `dbutils` is a Databricks-provided global — not importable
        return dbutils.widgets.get(name)  # type: ignore[name-defined] # noqa: F821
    except Exception as exc:
        raise RuntimeError(
            f"Widget '{name}' not set. Add this cell to the top of your notebook:\n"
            f"  dbutils.widgets.text('{name}', '<value>')"
        ) from exc


def get_connection(database: str = "NEXAMART_DW", schema: str = "NEXAMART_BRONZE"):
    """
    Build a Snowflake connection from the widget-provided credentials.

    Reuses the same widgets across all NexaMart notebooks. Returns a context
    manager — use with `with get_connection(...) as ctx: ...`.
    """
    return snowflake.connector.connect(
        account=_widget("sf_account"),
        user=_widget("sf_user"),
        password=_widget("sf_password"),
        warehouse=_widget("sf_warehouse"),
        role=_widget("sf_role"),
        database=database,
        schema=schema,
    )


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def write_pandas_to_snowflake(
    pdf: pd.DataFrame,
    table_name: str,
    schema: str = "NEXAMART_BRONZE",
    overwrite: bool = True,
    auto_create_table: bool = True,
    chunk_size: int = 16_000,
) -> int:
    """
    Write a pandas DataFrame to a Snowflake table.

    - Column names are uppercased (Snowflake's default identifier behavior).
    - Empty DataFrames are handled: an empty table with the right schema
      is still created (matches the brief's idempotency contract for
      pc_price_history which has 0 rows).
    - Idempotent when `overwrite=True`: re-running produces an identical table.

    Returns the row count actually written (== len(pdf)).
    """
    if pdf is None:
        raise ValueError("pdf is None")

    pdf = pdf.copy()
    pdf.columns = [c.upper() for c in pdf.columns]

    with get_connection(schema=schema) as ctx:
        if len(pdf) == 0 and auto_create_table:
            # write_pandas refuses empty frames; emulate by issuing a
            # CREATE OR REPLACE TABLE with an inferred schema.
            cols_ddl = ", ".join(f'"{c}" STRING' for c in pdf.columns)
            sql = (
                f"CREATE OR REPLACE TABLE {schema.upper()}.{table_name.upper()} "
                f"({cols_ddl})"
            )
            ctx.cursor().execute(sql)
            return 0

        success, _, nrows, _ = write_pandas(
            ctx,
            pdf,
            table_name=table_name.upper(),
            schema=schema.upper(),
            database="NEXAMART_DW",
            auto_create_table=auto_create_table,
            overwrite=overwrite,
            chunk_size=chunk_size,
            quote_identifiers=False,
        )
        if not success:
            raise RuntimeError(
                f"write_pandas failed for {schema}.{table_name}"
            )
        return nrows


def write_to_snowflake(
    df,
    table_name: str,
    schema: str = "NEXAMART_BRONZE",
    overwrite: bool = True,
) -> int:
    """
    Write a Spark DataFrame to Snowflake.

    Collects to driver via toPandas() then defers to write_pandas_to_snowflake.
    For our scale (≤438k rows per table) this fits in driver memory comfortably.
    For >5M rows you'd want chunked iteration; not needed here.
    """
    pdf = df.toPandas()
    return write_pandas_to_snowflake(
        pdf,
        table_name=table_name,
        schema=schema,
        overwrite=overwrite,
    )


# ---------------------------------------------------------------------------
# Read helper
# ---------------------------------------------------------------------------

def read_from_snowflake(
    spark,
    table_name: str,
    schema: str = "NEXAMART_BRONZE",
    select: str = "*",
    where: str | None = None,
):
    """
    Read a Snowflake table into a Spark DataFrame.

    Uses snowflake.connector.cursor.fetch_pandas_all() under the hood, then
    converts to Spark via spark.createDataFrame(). For tables larger than
    driver memory, push down filters via `where`.
    """
    sql = f'SELECT {select} FROM {schema.upper()}.{table_name.upper()}'
    if where:
        sql += f" WHERE {where}"
    with get_connection(schema=schema) as ctx:
        cur = ctx.cursor()
        cur.execute(sql)
        pdf = cur.fetch_pandas_all()
    # Lowercase column names back to source convention for Spark side
    pdf.columns = [c.lower() for c in pdf.columns]
    return spark.createDataFrame(pdf)


# ---------------------------------------------------------------------------
# DDL convenience
# ---------------------------------------------------------------------------

def execute_sql(sql: str | Iterable[str], schema: str = "NEXAMART_BRONZE") -> list:
    """
    Execute one or more SQL statements (e.g. SHOW, DROP, CREATE).
    Returns a list of fetchall() results, one per statement.
    """
    statements = [sql] if isinstance(sql, str) else list(sql)
    out = []
    with get_connection(schema=schema) as ctx:
        cur = ctx.cursor()
        for stmt in statements:
            cur.execute(stmt)
            try:
                out.append(cur.fetchall())
            except snowflake.connector.errors.NotSupportedError:
                # DDL statements don't return rows
                out.append(None)
    return out
