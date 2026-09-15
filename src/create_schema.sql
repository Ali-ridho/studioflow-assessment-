-- ============================================
-- SCHEMA SETUP
-- ============================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS intermediate;
-- ============================================
-- STAGING LAYER
-- ============================================
CREATE TABLE staging.stg_customers (
    customer_id         TEXT,
    customer_name       TEXT,
    country_code        TEXT,
    billing_currency    TEXT,
    signup_date         TEXT,
    is_test_account     TEXT,
    account_status      TEXT,
    source_line_number  INTEGER,
    loaded_at           TIMESTAMPTZ DEFAULT now(),
    row_hash            TEXT UNIQUE
);

CREATE TABLE staging.stg_application_transactions (
    transaction_id      TEXT,
    customer_id         TEXT,
    transaction_ts      TEXT,
    transaction_type    TEXT,
    status              TEXT,
    amount              TEXT,
    currency            TEXT,
    provider_reference  TEXT,
    ingested_at         TEXT,
    source_line_number  INTEGER,
    loaded_at           TIMESTAMPTZ DEFAULT now(),
    row_hash            TEXT UNIQUE

);

CREATE TABLE staging.stg_manual_adjustments (
    source_row_id       TEXT,
    spreadsheet_row     TEXT,
    occurred_at         TEXT,
    customer_id         TEXT,
    adjustment_type     TEXT,
    amount              TEXT,
    currency            TEXT,
    reason              TEXT,
    approval_status     TEXT,
    updated_at          TEXT,
    source_line_number  INTEGER,
    loaded_at           TIMESTAMPTZ DEFAULT now(),
    row_hash            TEXT UNIQUE

);

CREATE TABLE staging.stg_fx_rates (
    month_start         TEXT,
    currency            TEXT,
    usd_per_unit        TEXT,
    source_line_number  INTEGER,
    loaded_at           TIMESTAMPTZ DEFAULT now(),
    row_hash            TEXT UNIQUE
);

CREATE TABLE staging.stg_finance_control_totals (
    month_start                        TEXT,
    application_net_revenue_usd        TEXT,
    manual_net_revenue_usd             TEXT,
    expected_net_revenue_usd           TEXT,
    expected_application_record_count  TEXT,
    expected_manual_record_count       TEXT,
    source_line_number                 INTEGER,
    loaded_at                          TIMESTAMPTZ DEFAULT now(),
    row_hash                           TEXT UNIQUE
);

CREATE TABLE staging.stg_capacity_records (
    record_id           TEXT,
    period_start         TEXT,
    team_id              TEXT,
    designer_id          TEXT,
    capacity_points      TEXT,
    slides_completed     TEXT,
    logic_version        TEXT,
    updated_at           TEXT,
    source_line_number   INTEGER,
    loaded_at            TIMESTAMPTZ DEFAULT now(),
    row_hash             TEXT UNIQUE

);

-- ============================================
-- ERROR LOGGING TABLE
-- ============================================
CREATE TABLE staging.stg_load_errors (
    source_file          TEXT,
    source_line_number   INTEGER,
    raw_content          TEXT,
    error_reason         TEXT,
    detected_at          TIMESTAMPTZ DEFAULT now()

);

