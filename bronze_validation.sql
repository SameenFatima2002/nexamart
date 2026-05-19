-- ============================================================================
-- NexaMart M1 — Bronze validation
-- Asserts that NEXAMART_BRONZE matches the source SQLite database exactly.
-- Run after notebook 01_bronze_ingestion.ipynb completes.
-- Pass = empty result on the final assertion query.
-- ============================================================================

USE DATABASE NEXAMART_DW;
USE SCHEMA NEXAMART_BRONZE;

-- ----------------------------------------------------------------------------
-- 1. Sanity: total row count must equal 843,304 across all 61 tables
-- ----------------------------------------------------------------------------

SELECT SUM(row_count) AS total_bronze_rows
FROM _INGESTION_LOG
WHERE source_table NOT LIKE '\\_%' ESCAPE '\\';
-- Expected: 843304

SELECT COUNT(*) AS table_count
FROM _INGESTION_LOG
WHERE source_table NOT LIKE '\\_%' ESCAPE '\\';
-- Expected: 61

-- ----------------------------------------------------------------------------
-- 2. Per-table row count assertion
-- Embedded VALUES = ground-truth from SQLite source (verified 10 May 2026).
-- ----------------------------------------------------------------------------

WITH expected (source_table, expected_rows) AS (
    SELECT * FROM (VALUES
      ('cl_customers',                  2501),
      ('cl_loyalty_tiers',                 4),
      ('cl_loyalty_transactions',       2860),
      ('cs_agents',                       25),
      ('cs_case_events',                 225),
      ('cs_cases',                       129),
      ('cs_complaint_categories',         12),
      ('dc_carriers',                      5),
      ('dc_delivery_events',            3542),
      ('dc_event_types',                  10),
      ('dc_shipments',                   771),
      ('ec_delivery_methods',              5),
      ('ec_order_lines',                1840),
      ('ec_order_status_codes',            9),
      ('ec_order_status_history',       3307),
      ('ec_orders',                      963),
      ('nl_categories',                   13),
      ('nl_event_types',                  14),
      ('nl_listing_events',            38706),
      ('nl_listings',                   1253),
      ('nl_user_accounts',               356),
      ('pc_brands',                       30),
      ('pc_categories',                   27),
      ('pc_condition_codes',              10),
      ('pc_price_history',                 0),
      ('pc_products',                     65),
      ('pg_instrument_types',             12),
      ('pg_status_codes',                  8),
      ('pg_transactions',                963),
      ('pos_cashiers',                   160),
      ('pos_payment_methods',              7),
      ('pos_status_codes',                 6),
      ('pos_stores',                      20),
      ('pos_transaction_lines',        24507),
      ('pos_transactions',             10868),
      ('rr_refund_events',                74),
      ('rr_return_reasons',               12),
      ('rr_return_receipts',              74),
      ('rr_return_requests',              74),
      ('rv_reviews',                     377),
      ('si_inventory_movements',      438018),
      ('si_inventory_snapshots',      216645),
      ('si_movement_types',               10),
      ('ts_fulfilment_events',           593),
      ('ts_marketplace_orders',          252),
      ('ts_report_reasons',               11),
      ('ts_risk_signals',                 58),
      ('ts_safety_reports',               91),
      ('ts_seller_listings',             400),
      ('ts_seller_status_codes',           5),
      ('ts_seller_types',                  4),
      ('ts_sellers',                     100),
      ('ts_signal_types',                 10),
      ('wh_inbound_receipts',             57),
      ('wh_inventory_movements',       30437),
      ('wh_inventory_snapshots',       38610),
      ('wh_movement_types',               12),
      ('wh_warehouses',                    3),
      ('ws_event_types',                  17),
      ('ws_page_events',               20757),
      ('ws_sessions',                   3370)
    ) AS t (source_table, expected_rows)
)
-- The assertion: any row returned = a discrepancy.
-- Empty result set = PASS.
SELECT
    e.source_table,
    e.expected_rows,
    COALESCE(l.row_count, -1) AS actual_rows,
    COALESCE(l.row_count, 0) - e.expected_rows AS diff,
    CASE
        WHEN l.row_count IS NULL THEN 'MISSING_FROM_BRONZE'
        WHEN l.row_count <> e.expected_rows THEN 'COUNT_MISMATCH'
    END AS reason
FROM expected e
LEFT JOIN _INGESTION_LOG l ON l.source_table = e.source_table
WHERE l.row_count IS NULL
   OR l.row_count <> e.expected_rows
ORDER BY e.source_table;

-- ----------------------------------------------------------------------------
-- 3. Metadata column presence check (sample)
-- Every Bronze table must have: _source_table, _ingestion_timestamp, _source_row_number
-- Spot-check on cl_customers; expect 3 metadata columns + table's own columns.
-- ----------------------------------------------------------------------------

SELECT column_name
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'NEXAMART_BRONZE'
  AND table_name = 'CL_CUSTOMERS'
  AND column_name IN ('_SOURCE_TABLE', '_INGESTION_TIMESTAMP', '_SOURCE_ROW_NUMBER')
ORDER BY column_name;
-- Expected: 3 rows (one per metadata column)

-- ----------------------------------------------------------------------------
-- 4. Idempotence check (run notebook twice, then this)
-- _INGESTION_LOG should have same row counts; latest _ingestion_timestamp differs.
-- ----------------------------------------------------------------------------

SELECT MIN(_ingestion_timestamp) AS first_run,
       MAX(_ingestion_timestamp) AS latest_run,
       COUNT(DISTINCT _ingestion_timestamp) AS distinct_runs
FROM CL_CUSTOMERS;
-- Expected: first_run ≤ latest_run; row count in CL_CUSTOMERS still = 2501
