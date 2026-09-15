-- ============================================
-- Mart bulanan: bandingkan hasil kita vs Finance
-- ============================================

DROP TABLE IF EXISTS analytics.mart_monthly_revenue;

CREATE TABLE analytics.mart_monthly_revenue AS
WITH months AS (
    -- Generate 6 baris bulan: Jan-Jun 2026
    SELECT generate_series(
        '2026-01-01'::date,
        '2026-06-01'::date,
        '1 month'::interval
    )::date AS month_start
),
app_agg AS (
    -- Agregasi revenue dari application per bulan
    SELECT
        DATE_TRUNC('month', occurred_at)::date AS month_start,
        SUM(signed_amount_usd) AS application_net_revenue_usd,
        COUNT(*) AS application_record_count
    FROM analytics.fct_revenue_events
    WHERE source_system = 'application'
    GROUP BY DATE_TRUNC('month', occurred_at)::date
),
manual_agg AS (
    -- Agregasi revenue dari manual adjustment per bulan
    SELECT
        DATE_TRUNC('month', occurred_at)::date AS month_start,
        SUM(signed_amount_usd) AS manual_net_revenue_usd,
        COUNT(*) AS manual_record_count
    FROM analytics.fct_revenue_events
    WHERE source_system = 'manual_adjustment'
    GROUP BY DATE_TRUNC('month', occurred_at)::date
),
finance AS (
    SELECT
        month_start::date AS month_start,
        expected_net_revenue_usd::numeric AS finance_control_total_usd
    FROM staging.stg_finance_control_totals
)

SELECT
    m.month_start,
    COALESCE(a.application_net_revenue_usd, 0)  AS application_net_revenue_usd,
    COALESCE(mn.manual_net_revenue_usd, 0)       AS manual_net_revenue_usd,
    COALESCE(a.application_net_revenue_usd, 0) + COALESCE(mn.manual_net_revenue_usd, 0)
                                                   AS net_revenue_usd,
    f.finance_control_total_usd,
    (COALESCE(a.application_net_revenue_usd, 0) + COALESCE(mn.manual_net_revenue_usd, 0))
        - f.finance_control_total_usd             AS variance_usd,
    COALESCE(a.application_record_count, 0)      AS application_record_count,
    COALESCE(mn.manual_record_count, 0)           AS manual_record_count
FROM months m
LEFT JOIN app_agg a ON m.month_start = a.month_start
LEFT JOIN manual_agg mn ON m.month_start = mn.month_start
LEFT JOIN finance f ON m.month_start = f.month_start
ORDER BY m.month_start;

-- teting --
SELECT * FROM analytics.mart_monthly_revenue ORDER BY month_start;