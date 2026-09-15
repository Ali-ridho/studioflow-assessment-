-- ============================================
-- Gabungkan application + manual adjustments
-- jadi satu tabel fakta revenue sesuai kontrak
-- ============================================

-- Bersihkan HANYA temuan DQ milik tahap fct_revenue_events ini (idempotent)
DELETE FROM staging.stg_load_errors
WHERE error_reason LIKE 'orphan_customer%'
   OR error_reason LIKE 'unsupported_currency%'
   OR error_reason LIKE 'test_account_excluded%'
   OR error_reason LIKE 'out_of_reporting_window%'
   OR error_reason LIKE 'failed_transaction%';

-- Bersihkan tabel kerja & hasil akhir dari run sebelumnya
DROP TABLE IF EXISTS intermediate.wrk_customers;
DROP TABLE IF EXISTS intermediate.wrk_fx;
DROP TABLE IF EXISTS intermediate.wrk_app_with_customer;
DROP TABLE IF EXISTS intermediate.wrk_app_windowed;
DROP TABLE IF EXISTS intermediate.wrk_app_with_fx;
DROP TABLE IF EXISTS intermediate.wrk_application_events;
DROP TABLE IF EXISTS intermediate.wrk_man_with_customer;
DROP TABLE IF EXISTS intermediate.wrk_man_windowed;
DROP TABLE IF EXISTS intermediate.wrk_man_with_fx;
DROP TABLE IF EXISTS intermediate.wrk_manual_events;
DROP TABLE IF EXISTS analytics.fct_revenue_events;


-- ---------- Siapkan lookup ----------
CREATE TABLE intermediate.wrk_customers AS
SELECT customer_id, LOWER(TRIM(is_test_account)) AS is_test_account
FROM staging.stg_customers;

CREATE TABLE intermediate.wrk_fx AS
SELECT month_start::date AS month_start, currency, usd_per_unit::numeric AS usd_per_unit
FROM staging.stg_fx_rates;


-- ============================================
-- BAGIAN APPLICATION
-- ============================================
-- Tahap A1: LEFT JOIN customer (supaya orphan customer TERLIHAT, bukan hilang)
-- CATATAN: filter status_normalized 'succeeded' DIPINDAH ke bawah (Tahap A1.5),
-- supaya baris failed/pending bisa TERCATAT dulu sebelum di-exclude
CREATE TABLE intermediate.wrk_app_with_customer AS
SELECT
    a.*,
    c.customer_id AS matched_customer_id,
    c.is_test_account
FROM intermediate.int_revenue_application_resolved a
LEFT JOIN intermediate.wrk_customers c ON a.customer_id = c.customer_id;

-- --------------------------------------------
-- Tahap A1.5 (BARU): catat transaksi yang GAGAL/PENDING
-- Sesuai kontrak: kategori DQ "failed transaction"
-- --------------------------------------------
INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'application_transactions.csv', source_line_number,
    'transaction_id=' || transaction_id || ' | status=' || status_normalized,
    'failed_transaction: status is ' || status_normalized
FROM intermediate.wrk_app_with_customer
WHERE status_normalized != 'succeeded';

-- Catat orphan customer
INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'application_transactions.csv', source_line_number,
    'transaction_id=' || transaction_id || ' | customer_id=' || customer_id,
    'orphan_customer: customer_id not found in stg_customers'
FROM intermediate.wrk_app_with_customer
WHERE matched_customer_id IS NULL;

-- Catat test account yang dikecualikan
INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'application_transactions.csv', source_line_number,
    'transaction_id=' || transaction_id || ' | customer_id=' || customer_id,
    'test_account_excluded: customer flagged as test account'
FROM intermediate.wrk_app_with_customer
WHERE matched_customer_id IS NOT NULL AND is_test_account = 'true';

-- Tahap A2: filter customer valid + bukan test account
CREATE TABLE intermediate.wrk_app_windowed AS
SELECT *
FROM intermediate.wrk_app_with_customer
WHERE matched_customer_id IS NOT NULL
  AND is_test_account = 'false'
  AND status_normalized = 'succeeded';

-- Catat yang di luar reporting window
INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'application_transactions.csv', source_line_number,
    'transaction_id=' || transaction_id || ' | transaction_ts=' || transaction_ts,
    'out_of_reporting_window: transaction_ts outside [2026-01-01, 2026-07-01)'
FROM intermediate.wrk_app_windowed
WHERE transaction_ts < '2026-01-01'::timestamptz OR transaction_ts >= '2026-07-01'::timestamptz;

-- Tahap A3: LEFT JOIN FX (supaya unsupported currency TERLIHAT), sambil filter window
CREATE TABLE intermediate.wrk_app_with_fx AS
SELECT
    w.*,
    fx.usd_per_unit
FROM intermediate.wrk_app_windowed w
LEFT JOIN intermediate.wrk_fx fx
    ON fx.currency = w.currency
   AND fx.month_start = DATE_TRUNC('month', w.transaction_ts)::date
WHERE w.transaction_ts >= '2026-01-01'::timestamptz
  AND w.transaction_ts < '2026-07-01'::timestamptz;

-- Catat unsupported currency
INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'application_transactions.csv', source_line_number,
    'transaction_id=' || transaction_id || ' | currency=' || currency,
    'unsupported_currency: no FX rate found for this currency/month'
FROM intermediate.wrk_app_with_fx
WHERE usd_per_unit IS NULL;

-- Tahap A4: hasil akhir application
CREATE TABLE intermediate.wrk_application_events AS
SELECT
    'app_' || transaction_id                          AS revenue_event_key,
    'application'                                       AS source_system,
    transaction_id                                      AS source_record_id,
    customer_id,
    transaction_ts                                      AS occurred_at,
    transaction_type                                    AS event_type,
    CASE
        WHEN transaction_type IN ('subscription_renewal', 'token_purchase') THEN amount * usd_per_unit
        WHEN transaction_type IN ('refund', 'chargeback') THEN -1 * amount * usd_per_unit
        ELSE NULL
    END                                                  AS signed_amount_usd,
    ingested_at                                          AS source_updated_at
FROM intermediate.wrk_app_with_fx
WHERE usd_per_unit IS NOT NULL;


-- ============================================
-- BAGIAN MANUAL ADJUSTMENTS (pola identik)
-- ============================================

CREATE TABLE intermediate.wrk_man_with_customer AS
SELECT
    m.*,
    c.customer_id AS matched_customer_id,
    c.is_test_account
FROM intermediate.int_revenue_manual_resolved m
LEFT JOIN intermediate.wrk_customers c ON m.customer_id = c.customer_id;

INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'manual_adjustments.csv', source_line_number,
    'source_row_id=' || source_row_id || ' | customer_id=' || customer_id,
    'orphan_customer: customer_id not found in stg_customers'
FROM intermediate.wrk_man_with_customer
WHERE matched_customer_id IS NULL;

INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'manual_adjustments.csv', source_line_number,
    'source_row_id=' || source_row_id || ' | customer_id=' || customer_id,
    'test_account_excluded: customer flagged as test account'
FROM intermediate.wrk_man_with_customer
WHERE matched_customer_id IS NOT NULL AND is_test_account = 'true';

CREATE TABLE intermediate.wrk_man_windowed AS
SELECT *
FROM intermediate.wrk_man_with_customer
WHERE matched_customer_id IS NOT NULL
  AND is_test_account = 'false';

INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'manual_adjustments.csv', source_line_number,
    'source_row_id=' || source_row_id || ' | occurred_at=' || occurred_at,
    'out_of_reporting_window: occurred_at outside [2026-01-01, 2026-07-01)'
FROM intermediate.wrk_man_windowed
WHERE occurred_at < '2026-01-01'::timestamptz OR occurred_at >= '2026-07-01'::timestamptz;

CREATE TABLE intermediate.wrk_man_with_fx AS
SELECT
    w.*,
    fx.usd_per_unit
FROM intermediate.wrk_man_windowed w
LEFT JOIN intermediate.wrk_fx fx
    ON fx.currency = w.currency
   AND fx.month_start = DATE_TRUNC('month', w.occurred_at)::date
WHERE w.occurred_at >= '2026-01-01'::timestamptz
  AND w.occurred_at < '2026-07-01'::timestamptz;

INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'manual_adjustments.csv', source_line_number,
    'source_row_id=' || source_row_id || ' | currency=' || currency,
    'unsupported_currency: no FX rate found for this currency/month'
FROM intermediate.wrk_man_with_fx
WHERE usd_per_unit IS NULL;

CREATE TABLE intermediate.wrk_manual_events AS
SELECT
    'man_' || source_row_id                            AS revenue_event_key,
    'manual_adjustment'                                  AS source_system,
    source_row_id                                        AS source_record_id,
    customer_id,
    occurred_at,
    adjustment_type                                      AS event_type,
    CASE
        WHEN adjustment_type = 'revenue_addition' THEN amount * usd_per_unit
        WHEN adjustment_type = 'refund' THEN -1 * amount * usd_per_unit
        ELSE NULL
    END                                                   AS signed_amount_usd,
    updated_at                                            AS source_updated_at
FROM intermediate.wrk_man_with_fx
WHERE usd_per_unit IS NOT NULL;


-- ============================================
-- GABUNGKAN JADI TABEL FINAL
-- ============================================
CREATE TABLE analytics.fct_revenue_events AS
SELECT * FROM intermediate.wrk_application_events
WHERE signed_amount_usd IS NOT NULL
UNION ALL
SELECT * FROM intermediate.wrk_manual_events
WHERE signed_amount_usd IS NOT NULL;


-- Bersihkan tabel kerja sementara
DROP TABLE intermediate.wrk_customers;
DROP TABLE intermediate.wrk_fx;
DROP TABLE intermediate.wrk_app_with_customer;
DROP TABLE intermediate.wrk_app_windowed;
DROP TABLE intermediate.wrk_app_with_fx;
DROP TABLE intermediate.wrk_application_events;
DROP TABLE intermediate.wrk_man_with_customer;
DROP TABLE intermediate.wrk_man_windowed;
DROP TABLE intermediate.wrk_man_with_fx;
DROP TABLE intermediate.wrk_manual_events;

-- testing --

SELECT COUNT(*) FROM analytics.fct_revenue_events;

SELECT error_reason, COUNT(*) FROM staging.stg_load_errors WHERE error_reason LIKE 'orphan_customer%' OR error_reason LIKE 'unsupported_currency%' OR error_reason LIKE 'test_account_excluded%' OR error_reason LIKE 'out_of_reporting_window%' GROUP BY error_reason;

SELECT DISTINCT transaction_type FROM intermediate.int_revenue_application_resolved;

SELECT DISTINCT adjustment_type FROM intermediate.int_revenue_manual_resolved;