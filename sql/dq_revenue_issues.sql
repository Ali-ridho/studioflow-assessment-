-- ============================================
-- Konsolidasi semua temuan DQ dari stg_load_errors
-- ke tabel resmi analytics.dq_revenue_issues
-- sesuai struktur kolom kontrak
-- ============================================

DROP TABLE IF EXISTS analytics.dq_revenue_issues;

CREATE TABLE analytics.dq_revenue_issues AS
WITH parsed AS (
    SELECT
        source_file,
        source_line_number,

        -- Tentukan source_system dari nama file
        CASE
            WHEN source_file = 'application_transactions.csv' THEN 'application'
            WHEN source_file = 'manual_adjustments.csv'       THEN 'manual_adjustment'
            ELSE 'unknown'
        END AS source_system,

        -- Ekstrak business key dari raw_content (transaction_id ATAU source_row_id)
        -- Kalau tidak ada keduanya (misal missing_stable_key), fallback ke nomor baris
        COALESCE(
            (regexp_match(raw_content, 'transaction_id=([^ |]+)'))[1],
            (regexp_match(raw_content, 'source_row_id=([^ |]+)'))[1],
            'line:' || source_line_number::text
        ) AS source_record_id,

        -- Ambil reason_code (bagian sebelum ':') dari error_reason
        split_part(error_reason, ':', 1) AS reason_code,

        raw_content AS context,
        detected_at
    FROM staging.stg_load_errors
)

SELECT
    source_file,
    source_line_number,
    source_system,
    source_record_id,
    reason_code,

    -- Tentukan severity berdasarkan jenis reason_code
    CASE reason_code
        WHEN 'field_count_mismatch'      THEN 'error'
        WHEN 'invalid_transaction_ts'    THEN 'error'
        WHEN 'invalid_ingested_at'       THEN 'error'
        WHEN 'invalid_occurred_at'       THEN 'error'
        WHEN 'invalid_updated_at'        THEN 'error'
        WHEN 'invalid_amount'            THEN 'error'
        WHEN 'missing_stable_key'        THEN 'error'
        WHEN 'ambiguous_source_version'  THEN 'error'   -- WAJIB error sesuai kontrak
        WHEN 'orphan_customer'           THEN 'error'
        WHEN 'unsupported_currency'      THEN 'error'
        WHEN 'unapproved_adjustment'     THEN 'warning'
        WHEN 'test_account_excluded'     THEN 'warning'
        WHEN 'out_of_reporting_window'   THEN 'warning'
        WHEN 'failed_transaction'        THEN 'warning'
        ELSE 'warning'
    END AS severity,

    detected_at,
    context
FROM parsed;

-- testing --
SELECT COUNT(*) FROM analytics.dq_revenue_issues;
-- Harus PERSIS sama dengan total semua baris di stg_load_errors

SELECT severity, COUNT(*) FROM analytics.dq_revenue_issues GROUP BY severity;
-- Lihat breakdown error vs warning

SELECT reason_code, source_record_id, context FROM analytics.dq_revenue_issues WHERE source_record_id LIKE 'line:%';
-- Cek baris yang fallback ke nomor baris (harusnya cuma missing_stable_key)

SELECT COUNT(*) FROM analytics.fct_revenue_events;

SELECT COUNT(*) FROM analytics.mart_monthly_revenue; 