-- ============================================
-- Unifikasi capacity records dari 3 logic_version berbeda
-- jadi satu interface konsumen yang konsisten
-- ============================================

-- Membersihkan temuan DQ 
DELETE FROM staging.stg_load_errors
WHERE error_reason LIKE 'invalid_period_start%'
   OR error_reason LIKE 'invalid_capacity_updated_at%'
   OR error_reason LIKE 'unrecognized_logic_version%';

-- Memersihkan tabel kerja & hasil akhir dari run sebelumnya
DROP TABLE IF EXISTS intermediate.wrk_capacity_typed;
DROP TABLE IF EXISTS intermediate.wrk_capacity_valid;
DROP TABLE IF EXISTS intermediate.wrk_capacity_top;
DROP TABLE IF EXISTS analytics.mart_capacity_unified;


-- --------------------------------------------
-- 1.cast tipe data
-- --------------------------------------------
CREATE TABLE intermediate.wrk_capacity_typed AS
SELECT
    record_id,
    safe_to_timestamptz(period_start)   AS period_start,
    team_id,
    designer_id,
    safe_to_numeric(capacity_points)     AS capacity_points,
    safe_to_numeric(slides_completed)    AS slides_completed,
    logic_version,
    safe_to_timestamptz(updated_at)      AS updated_at,
    source_line_number
FROM staging.stg_capacity_records;


-- --------------------------------------------
-- 2. catat baris dengan period_start/updated_at RUSAK
-- --------------------------------------------
INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'capacity_records.csv', source_line_number,
    'record_id=' || record_id,
    CASE
        WHEN period_start IS NULL THEN 'invalid_period_start'
        WHEN updated_at IS NULL THEN 'invalid_capacity_updated_at'
    END
FROM intermediate.wrk_capacity_typed
WHERE period_start IS NULL OR updated_at IS NULL;


-- --------------------------------------------
-- 3. Mencatat logic_version yang TIDAK dikenali
-- 
-- --------------------------------------------
INSERT INTO staging.stg_load_errors (source_file, source_line_number, raw_content, error_reason)
SELECT
    'capacity_records.csv', source_line_number,
    'record_id=' || record_id || ' | logic_version=' || logic_version,
    'unrecognized_logic_version: ' || logic_version
FROM intermediate.wrk_capacity_typed
WHERE period_start IS NOT NULL AND updated_at IS NOT NULL
  AND logic_version NOT IN ('team_points_v1', 'designer_points_v2', 'designer_slides_v3');


-- --------------------------------------------
-- Tahap 4: kumpulkan baris VALID (cast berhasil + logic_version dikenali)
-- --------------------------------------------
CREATE TABLE intermediate.wrk_capacity_valid AS
SELECT *
FROM intermediate.wrk_capacity_typed
WHERE period_start IS NOT NULL AND updated_at IS NOT NULL
  AND logic_version IN ('team_points_v1', 'designer_points_v2', 'designer_slides_v3');


-- --------------------------------------------
-- Tahap 5: resolve versioning (greatest updated_at wins per record_id)
-- --------------------------------------------
CREATE TABLE intermediate.wrk_capacity_top AS
SELECT v.*
FROM intermediate.wrk_capacity_valid v
JOIN (
    SELECT record_id, MAX(updated_at) AS max_updated_at
    FROM intermediate.wrk_capacity_valid
    GROUP BY record_id
) m
  ON v.record_id = m.record_id
 AND v.updated_at = m.max_updated_at;


-- --------------------------------------------
-- Tahap 6: HASIL AKHIR
-- Mapping logic_version -> owner_type, metric_name, metric_unit, owner_id
-- --------------------------------------------
CREATE TABLE analytics.mart_capacity_unified AS
SELECT
    period_start,

    CASE logic_version
        WHEN 'team_points_v1'     THEN 'team'
        WHEN 'designer_points_v2' THEN 'designer'
        WHEN 'designer_slides_v3' THEN 'designer'
    END AS owner_type,

    CASE logic_version
        WHEN 'team_points_v1'     THEN team_id
        WHEN 'designer_points_v2' THEN designer_id
        WHEN 'designer_slides_v3' THEN designer_id
    END AS owner_id,

    team_id,   -- tetap disimpan apapun logic_version-nya (sesuai kontrak: "Team attribution retained")

    CASE logic_version
        WHEN 'team_points_v1'     THEN 'capacity_points'
        WHEN 'designer_points_v2' THEN 'capacity_points'
        WHEN 'designer_slides_v3' THEN 'slides_completed'
    END AS metric_name,

    CASE logic_version
        WHEN 'team_points_v1'     THEN 'points'
        WHEN 'designer_points_v2' THEN 'points'
        WHEN 'designer_slides_v3' THEN 'slides'
    END AS metric_unit,

    CASE logic_version
        WHEN 'team_points_v1'     THEN capacity_points
        WHEN 'designer_points_v2' THEN capacity_points
        WHEN 'designer_slides_v3' THEN slides_completed
    END AS metric_value,

    logic_version,
    record_id AS source_record_id
FROM intermediate.wrk_capacity_top;


-- Bersihkan tabel kerja sementara
DROP TABLE intermediate.wrk_capacity_typed;
DROP TABLE intermediate.wrk_capacity_valid;
DROP TABLE intermediate.wrk_capacity_top;


-- testing --
SELECT COUNT(*) FROM analytics.mart_capacity_unified;
SELECT logic_version, owner_type, metric_name, metric_unit, COUNT(*) 
FROM analytics.mart_capacity_unified 
GROUP BY logic_version, owner_type, metric_name, metric_unit;

SELECT COUNT(*) FROM staging.stg_capacity_records;

SELECT error_reason, COUNT(*) FROM staging.stg_load_errors WHERE error_reason LIKE 'invalid_period_start%' OR error_reason LIKE 'invalid_capacity_updated_at%' OR error_reason LIKE 'unrecognized_logic_version%' GROUP BY error_reason; 

SELECT record_id, COUNT(*) FROM staging.stg_capacity_records GROUP BY record_id HAVING COUNT(*) > 1;