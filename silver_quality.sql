-- ============================================================================
-- NexaMart M1 — Silver quality validation
-- Run after all Silver tables are loaded (end of Phase 4 / start of Phase 7).
-- Each section returns rows ONLY when there's a violation.
-- Empty result set per section = PASS.
--
-- Sections:
--   1. Completeness — non-null % on the 4 mandatory anomaly columns
--   2. Anomaly distribution — sanity check counts per reason_code / status
--   3. FK integrity — no orphan surrogate-key references between Silver tables
--   4. Grain checks — every Silver table is unique on its declared natural key
--   5. Date sanity — no future dates, no pre-window dates
-- ============================================================================

USE DATABASE NEXAMART_DW;
USE SCHEMA NEXAMART_SILVER;

-- ----------------------------------------------------------------------------
-- 1. Completeness: every Silver table must have all 4 mandatory cols
--    populated on every row.
--
-- For each Silver table, count rows where any of the 4 mandatory cols is NULL.
-- Add new tables as members produce them.
-- ----------------------------------------------------------------------------

-- Pattern (run per Silver table, replace <table_name>):
WITH completeness AS (
    SELECT
        '<table_name>' AS silver_table,
        COUNT(*)       AS total_rows,
        SUM(CASE WHEN anomaly_flag IS NULL THEN 1 ELSE 0 END)              AS null_anomaly_flag,
        SUM(CASE WHEN data_quality_status IS NULL THEN 1 ELSE 0 END)       AS null_status,
        SUM(CASE WHEN metric_certainty_level IS NULL THEN 1 ELSE 0 END)    AS null_certainty
        -- anomaly_reason_code IS NULL is OK when anomaly_flag IS FALSE
    FROM <table_name>
)
SELECT * FROM completeness
WHERE null_anomaly_flag > 0
   OR null_status > 0
   OR null_certainty > 0;

-- Run the above per table. To do all at once, UNION ALL the per-table SELECTs.
-- Lead, fill in for every Silver table after Phase 4 ships:
--   silver_customer_master, silver_loyalty_*, silver_product_master, silver_brands, ...
--   silver_store_inventory_snapshots, silver_warehouse_inventory_snapshots, ...
--   silver_pos_transactions, silver_ec_orders, silver_pg_transactions, ...
--   silver_nl_listings, silver_seller_trust_score, silver_reviews, silver_cases, ...

-- ----------------------------------------------------------------------------
-- 2. Anomaly distribution: sanity check that flags exist where expected
-- ----------------------------------------------------------------------------

-- 2a. Distribution by data_quality_status across all Silver tables
-- (Run per table; combine with UNION ALL when all tables are listed)
SELECT
    'silver_ec_orders' AS silver_table,
    data_quality_status,
    COUNT(*) AS row_count
FROM silver_ec_orders
GROUP BY data_quality_status
ORDER BY row_count DESC;

-- 2b. Distribution by anomaly_reason_code (only for flagged rows)
SELECT
    'silver_ec_orders' AS silver_table,
    anomaly_reason_code,
    COUNT(*) AS row_count
FROM silver_ec_orders
WHERE anomaly_flag = TRUE
GROUP BY anomaly_reason_code
ORDER BY row_count DESC;

-- 2c. Verify no rows use a reason_code outside the registered taxonomy.
-- Build the registered set from docs/anomaly_taxonomy.md.
WITH registered_codes AS (
    SELECT * FROM (VALUES
        -- Universal
        ('DATE_PARSE_FAIL'), ('DATE_FUTURE'), ('DATE_BEFORE_RANGE'),
        ('STATUS_UNMAPPED'), ('ORPHAN_FK'), ('NEGATIVE_QTY'),
        ('NEGATIVE_AMOUNT'), ('MISSING_REQUIRED_FIELD'),
        -- Customer
        ('IDENTITY_AMBIGUOUS'), ('PLACEHOLDER_ID_COLLISION'),
        ('FUZZY_MATCH_LOW_CONF'), ('EMAIL_MALFORMED'), ('PHONE_NORMALISATION_FAIL'),
        -- Product
        ('SKU_PRODUCT_MISMATCH'), ('SKU_NOT_IN_CATALOGUE'),
        ('PRODUCT_FUZZY_MATCH'), ('PRODUCT_NAME_CONFLICT'),
        -- Inventory
        ('RECONSTRUCTED_SNAPSHOT'), ('MISSING_SNAPSHOT_DAY'),
        ('ATP_POSITIVE_PHYSICAL_ZERO'), ('OPEN_BOX_AS_NEW'),
        ('MOVEMENT_NULL_REF'), ('OVERSELL'),
        -- Sales
        ('CANCELLED_WITH_REVENUE'), ('PAYMENT_AFTER_CANCEL'),
        ('TAX_INCLUSION_MISMATCH'), ('DELIVERY_BEFORE_SHIP'),
        ('COURIER_CLOCK_DRIFT'), ('BOPIS_NO_PICKUP_EVENT'),
        ('REFUND_PARTIAL_PERIOD_AMBIGUITY'), ('ATTRIBUTION_SESSION_BRIDGE'),
        -- NL/M6
        ('NL_SELLER_SOLD_AS_REVENUE'), ('RELISTED_AFTER_SOLD'),
        ('IMAGE_HASH_REUSED'), ('REVIEW_BEFORE_DELIVERY'),
        ('DUPLICATE_CASE'), ('ESTIMATED_NL_GMV'),
        ('LISTING_LOW_CONFIDENCE'), ('SELLER_HIGH_RISK'),
        ('MANUAL_CHANNEL_ATTRIBUTION')
    ) AS t (code)
)
-- Members: replace silver_X with each Silver table that has anomaly_reason_code populated.
-- Anything returned = an unregistered code in use → fix or register.
SELECT
    'silver_X' AS silver_table,
    raw_code,
    COUNT(*) AS row_count
FROM (
    SELECT TRIM(value) AS raw_code
    FROM silver_X,
         LATERAL FLATTEN(input => SPLIT(anomaly_reason_code, ','))
    WHERE anomaly_reason_code IS NOT NULL
) v
WHERE raw_code NOT IN (SELECT code FROM registered_codes)
GROUP BY raw_code;

-- ----------------------------------------------------------------------------
-- 3. FK integrity — every Silver-table SK FK resolves to its target Silver
-- ----------------------------------------------------------------------------

-- Pattern: silver_ec_orders.customer_master_key must exist in silver_customer_master
SELECT
    'silver_ec_orders.customer_master_key → silver_customer_master' AS fk_check,
    COUNT(*) AS orphan_count
FROM silver_ec_orders eo
LEFT JOIN silver_customer_master cm
    ON eo.customer_master_key = cm.customer_master_key
WHERE cm.customer_master_key IS NULL
  AND eo.customer_master_key IS NOT NULL;

-- silver_ec_order_lines.order_id → silver_ec_orders.order_id
SELECT
    'silver_ec_order_lines.order_id → silver_ec_orders.order_id' AS fk_check,
    COUNT(*) AS orphan_count
FROM silver_ec_order_lines ol
LEFT JOIN silver_ec_orders o ON o.order_id = ol.order_id
WHERE o.order_id IS NULL;

-- silver_ec_order_lines.canonical_product_key → silver_product_master
SELECT
    'silver_ec_order_lines.canonical_product_key → silver_product_master' AS fk_check,
    COUNT(*) AS orphan_count
FROM silver_ec_order_lines ol
LEFT JOIN silver_product_master pm
    ON ol.canonical_product_key = pm.canonical_product_key
WHERE pm.canonical_product_key IS NULL
  AND ol.canonical_product_key IS NOT NULL;

-- Add similar checks for every documented logical FK between Silver tables.
-- The 70 FKs in the data dictionary translate to ~30 cross-Silver checks (after T3 SK substitution).

-- ----------------------------------------------------------------------------
-- 4. Grain checks — every Silver table is unique on its declared natural key
-- ----------------------------------------------------------------------------

-- For each table, COUNT(*) must equal COUNT(DISTINCT <natural_key>).
-- Anything returned = duplicate rows.

-- silver_customer_master — unique by customer_master_key
SELECT 'silver_customer_master' AS silver_table, COUNT(*) AS total, COUNT(DISTINCT customer_master_key) AS distinct_keys
FROM silver_customer_master
HAVING COUNT(*) <> COUNT(DISTINCT customer_master_key);

-- silver_ec_orders — unique by order_id
SELECT 'silver_ec_orders', COUNT(*), COUNT(DISTINCT order_id)
FROM silver_ec_orders
HAVING COUNT(*) <> COUNT(DISTINCT order_id);

-- silver_ec_order_lines — unique by (order_id, line_no)
SELECT 'silver_ec_order_lines', COUNT(*), COUNT(DISTINCT order_id || '|' || line_no)
FROM silver_ec_order_lines
HAVING COUNT(*) <> COUNT(DISTINCT order_id || '|' || line_no);

-- silver_product_master — unique by canonical_product_key
SELECT 'silver_product_master', COUNT(*), COUNT(DISTINCT canonical_product_key)
FROM silver_product_master
HAVING COUNT(*) <> COUNT(DISTINCT canonical_product_key);

-- silver_store_inventory_snapshots — unique by (store_id, sku, snapshot_date)
SELECT 'silver_store_inventory_snapshots', COUNT(*),
       COUNT(DISTINCT store_id || '|' || sku || '|' || snapshot_date)
FROM silver_store_inventory_snapshots
HAVING COUNT(*) <> COUNT(DISTINCT store_id || '|' || sku || '|' || snapshot_date);

-- Add for every Silver table.

-- ----------------------------------------------------------------------------
-- 5. Date sanity — no future dates, no pre-2024-03-01 dates
--    (project window is 2024-03-01 to 2024-09-14)
-- ----------------------------------------------------------------------------

-- Project window
SET project_start = '2024-03-01';
SET project_end   = '2024-09-14';

-- For each Silver table with dates: check for out-of-range values.
-- Pattern (replace table and column):

SELECT
    'silver_ec_orders.order_date out of window' AS check_name,
    COUNT(*) AS violations,
    MIN(order_date) AS min_date,
    MAX(order_date) AS max_date
FROM silver_ec_orders
WHERE order_date < $project_start
   OR order_date > $project_end;

SELECT
    'silver_pos_transactions.txn_date out of window',
    COUNT(*),
    MIN(txn_date),
    MAX(txn_date)
FROM silver_pos_transactions
WHERE txn_date < $project_start
   OR txn_date > $project_end;

-- Add for every dated Silver table.

-- ----------------------------------------------------------------------------
-- 6. Combined PASS summary
-- ----------------------------------------------------------------------------

-- After all sections above return empty (or known/justified small counts),
-- the Silver layer is ready for Gold consumption.

SELECT 'silver_quality_check' AS suite, CURRENT_TIMESTAMP() AS run_at, 'COMPLETE' AS status;
