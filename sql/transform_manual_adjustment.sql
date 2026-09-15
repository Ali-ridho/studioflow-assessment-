

DELETE FROM staging.stg_load_errors
WHERE source_file = 'manual_adjustments.csv'
  AND error_reason NOT LIKE 'field_count_mismatch%';

DROP TABLE IF EXISTS intermediate.wrk_typed_adjustments;
DROP TABLE IF EXISTS intermediate.wrk_valid_adjustments;
DROP TABLE IF EXISTS intermediate.wrk_top_versions;
DROP TABLE IF EXISTS intermediate.wrk_ambiguous_keys;
DROP TABLE IF EXISTS intermediate.int_revenue_manual_resolved;


-- --------------------------------------------
-- Tahap 1: cast tipe data + normalize
-- --------------------------------------------
CREATE TABLE intermediate.wrk_typed_adjustments AS
SELECT
    source_row_id,
    TRIM(source_row_id) AS source_row_id_clean,
    spreadsheet_row,
    safe_to_timestamptz(occurred_at)      AS occurred_at,
    customer_id,
    adjustment_type,
    safe_to_numeric(amount)                AS amount,
    currency,
    reason,
    LOWER(TRIM(approval_status))           AS approval_status_normalized,
    safe_to_timestamptz(updated_at)        AS updated_at,
    source_line_number
FROM staging.stg_manual_adjustments;


-- --------------------------------------------
-- Tahap 2: catat baris dengan source_row_id KOSONG/NULL
-- Sesuai kontrak: "cannot be loaded as accepted events idempotently"
-- --------------------------------------------
INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'manual_adjustments.csv',
    source_line_number,
    'customer_id=' || customer_id || ' | amount=' || COALESCE(amount::text, 'NULL'),
    'missing_stable_key: source_row_id is null or blank'
FROM intermediate.wrk_typed_adjustments
WHERE source_row_id_clean IS NULL OR source_row_id_clean = '';


-- --------------------------------------------
-- Tahap 3: catat baris dengan timestamp/amount RUSAK (gagal cast)
-- --------------------------------------------
INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'manual_adjustments.csv',
    source_line_number,
    'source_row_id=' || source_row_id_clean,
    CASE
        WHEN occurred_at IS NULL THEN 'invalid_occurred_at'
        WHEN updated_at IS NULL THEN 'invalid_updated_at'
        WHEN amount IS NULL THEN 'invalid_amount'
    END
FROM intermediate.wrk_typed_adjustments
WHERE source_row_id_clean IS NOT NULL AND source_row_id_clean != ''
  AND (occurred_at IS NULL OR updated_at IS NULL OR amount IS NULL);


-- --------------------------------------------
-- Tahap 4: kumpulkan baris yang VALID (key ada + cast berhasil)
-- --------------------------------------------
CREATE TABLE intermediate.wrk_valid_adjustments AS
SELECT *
FROM intermediate.wrk_typed_adjustments
WHERE source_row_id_clean IS NOT NULL AND source_row_id_clean != ''
  AND occurred_at IS NOT NULL AND updated_at IS NOT NULL AND amount IS NOT NULL;


-- --------------------------------------------
-- Tahap 5: cari versi TERBESAR (greatest updated_at) per source_row_id
-- --------------------------------------------
CREATE TABLE intermediate.wrk_top_versions AS
SELECT v.*
FROM intermediate.wrk_valid_adjustments v
JOIN (
    SELECT source_row_id_clean, MAX(updated_at) AS max_updated_at
    FROM intermediate.wrk_valid_adjustments
    GROUP BY source_row_id_clean
) m
  ON v.source_row_id_clean = m.source_row_id_clean
 AND v.updated_at = m.max_updated_at;


-- --------------------------------------------
-- Tahap 6: deteksi AMBIGUOUS
-- (lebih dari 1 baris di updated_at TERBESAR = konflik yang
-- tidak bisa diputuskan otomatis, sesuai kontrak)
-- --------------------------------------------
CREATE TABLE intermediate.wrk_ambiguous_keys AS
SELECT source_row_id_clean
FROM intermediate.wrk_top_versions
GROUP BY source_row_id_clean
HAVING COUNT(*) > 1;

-- Catat SEMUA versi yang ambiguous ke DQ log (quarantine)
INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'manual_adjustments.csv',
    t.source_line_number,
    'source_row_id=' || t.source_row_id_clean ||
        ' | amount=' || t.amount ||
        ' | approval=' || t.approval_status_normalized,
    'ambiguous_source_version: multiple conflicting top versions at same updated_at'
FROM intermediate.wrk_top_versions t
JOIN intermediate.wrk_ambiguous_keys a
  ON t.source_row_id_clean = a.source_row_id_clean;


-- --------------------------------------------
-- Tahap 7: HASIL AKHIR
-- Hanya key yang TIDAK ambiguous DAN approval_status = 'approved'
-- --------------------------------------------
CREATE TABLE intermediate.int_revenue_manual_resolved AS
SELECT
    source_row_id_clean         AS source_row_id,
    customer_id,
    occurred_at,
    adjustment_type,
    amount,
    currency,
    reason,
    approval_status_normalized  AS approval_status,
    updated_at,
    source_line_number
FROM intermediate.wrk_top_versions t
WHERE source_row_id_clean NOT IN (SELECT source_row_id_clean FROM intermediate.wrk_ambiguous_keys)
  AND approval_status_normalized = 'approved';

-- --------------------------------------------
-- Tahap 7.5: catat versi TERBARU yang TIDAK approved
-- (misal MANA0008 — winning version = rejected)
-- Supaya tidak "hilang diam-diam" dari akuntansi DQ
-- --------------------------------------------
INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'manual_adjustments.csv',
    t.source_line_number,
    'source_row_id=' || t.source_row_id_clean ||
        ' | amount=' || t.amount ||
        ' | approval=' || t.approval_status_normalized,
    'unapproved_adjustment: latest version has approval_status = ' || t.approval_status_normalized
FROM intermediate.wrk_top_versions t
WHERE t.source_row_id_clean NOT IN (SELECT source_row_id_clean FROM intermediate.wrk_ambiguous_keys)
  AND t.approval_status_normalized != 'approved';


DROP TABLE intermediate.wrk_typed_adjustments;
DROP TABLE intermediate.wrk_valid_adjustments;
DROP TABLE intermediate.wrk_top_versions;
DROP TABLE intermediate.wrk_ambiguous_keys;

--  testing --

SELECT COUNT(*) FROM intermediate.int_revenue_manual_resolved;
SELECT * FROM staging.stg_load_errors WHERE error_reason LIKE '%ambiguous%' OR error_reason LIKE '%missing_stable_key%';
SELECT * FROM staging.stg_load_errors WHERE error_reason LIKE '%unapproved%';

SELECT
    (SELECT COUNT(*) FROM staging.stg_manual_adjustments) AS total_staging,
    (SELECT COUNT(*) FROM staging.stg_load_errors WHERE error_reason LIKE '%missing_stable_key%') AS missing_key,
    (SELECT COUNT(*) FROM staging.stg_load_errors WHERE error_reason LIKE '%ambiguous%') AS ambiguous,
    (SELECT COUNT(*) FROM staging.stg_load_errors WHERE error_reason LIKE '%unapproved%') AS unapproved,
    (SELECT COUNT(*) FROM intermediate.int_revenue_manual_resolved) AS final_approved;

SELECT DISTINCT adjustment_type FROM intermediate.int_revenue_manual_resolved;