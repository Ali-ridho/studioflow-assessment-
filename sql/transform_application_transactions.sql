-- ============================================
-- Resolve versioning untuk application_transactions
-- Aturan: untuk transaction_id yang sama, ingested_at
-- TERBESAR yang menang.
-- ============================================

-- Fungsi PostgreSQL reusable: cast TEXT ke TIMESTAMPTZ,
-- return NULL kalau gagal (bukan error total).
CREATE OR REPLACE FUNCTION safe_to_timestamptz(input_text TEXT)
RETURNS TIMESTAMPTZ AS $$
BEGIN
    RETURN input_text::TIMESTAMPTZ;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Fungsi serupa untuk NUMERIC
CREATE OR REPLACE FUNCTION safe_to_numeric(input_text TEXT)
RETURNS NUMERIC AS $$
BEGIN
    RETURN input_text::NUMERIC;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


DROP TABLE IF EXISTS intermediate.int_revenue_application_resolved;

CREATE TABLE intermediate.int_revenue_application_resolved AS
WITH typed AS (
    SELECT
        transaction_id,
        customer_id,
        safe_to_timestamptz(transaction_ts) AS transaction_ts,
        transaction_type,
        LOWER(TRIM(status))                 AS status_normalized,
        safe_to_numeric(amount)              AS amount,
        currency,
        provider_reference,
        safe_to_timestamptz(ingested_at)    AS ingested_at,
        source_line_number
    FROM staging.stg_application_transactions
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY ingested_at DESC NULLS LAST, source_line_number DESC
        ) AS version_rank
    FROM typed
)
SELECT
    transaction_id,
    customer_id,
    transaction_ts,
    transaction_type,
    status_normalized,
    amount,
    currency,
    provider_reference,
    ingested_at,
    source_line_number
FROM ranked
WHERE version_rank = 1
  AND transaction_ts IS NOT NULL   -- exclude baris dengan timestamp rusak
  AND ingested_at IS NOT NULL
  AND amount IS NOT NULL;           -- exclude baris dengan amount rusak


-- ============================================
-- Catat baris yang GAGAL cast ke stg_load_errors
-- (supaya tidak "hilang diam-diam" sesuai kontrak)
-- ============================================
INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'application_transactions.csv',
    source_line_number,
    transaction_id || ' | ts=' || transaction_ts || ' | ingested=' || ingested_at || ' | amount=' || amount,
    CASE
        WHEN safe_to_timestamptz(transaction_ts) IS NULL THEN 'invalid_transaction_ts: ' || transaction_ts
        WHEN safe_to_timestamptz(ingested_at) IS NULL THEN 'invalid_ingested_at: ' || ingested_at
        WHEN safe_to_numeric(amount) IS NULL THEN 'invalid_amount: ' || amount
    END
FROM staging.stg_application_transactions
WHERE safe_to_timestamptz(transaction_ts) IS NULL
   OR safe_to_timestamptz(ingested_at) IS NULL
   OR safe_to_numeric(amount) IS NULL;

-- testing --
   SELECT COUNT(*) FROM intermediate.int_revenue_application_resolved;
   SELECT COUNT(*) FROM staging.stg_application_transactions;
   SELECT COUNT(*) FROM staging.stg_load_errors WHERE error_reason LIKE 'invalid_%'; 