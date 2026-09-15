# Submission — Ali Ridho Muladawila

**Active time spent:** [9-10 hours]

**Recorded demo link or delivery method:** [private link, attachment name, or delivery method]

## 1. What I built

I built a layered PostgreSQL pipeline (staging → intermediate → analytics) that resolves duplicate deliveries and source corrections using a "greatest valid timestamp wins" strategy, with explicit quarantine for ambiguous conflicting versions. I implemented CDC row-hashing at the staging layer (based on my internship experience) to guarantee idempotent reloads, and PostgreSQL functions (`safe_to_timestamptz`, `safe_to_numeric`) to gracefully handle malformed values without crashing the pipeline. Currency conversion, test-account exclusion, and reporting-window filtering are applied before revenue is combined from both application and manual-adjustment sources into `fct_revenue_events`. Every excluded or suspect record is logged to `dq_revenue_issues` with a machine-readable reason code and severity, achieving full source-to-fact accountability. Finally, I orchestrated the entire pipeline with Apache Airflow (Docker Compose), including a Python-based reconciliation gate that fails the DAG run if any month's variance exceeds $0.01.

## 2. How to reproduce the core result

**Prerequisites**

Docker Dekstop, Python 3.10+ with `psycopg2-binary`,`pandas`, and `python-dotenv` instaled. No presquisites beyond that (PostgreSQL runs inside Docker; Airflow orchestration runs inside Docker too).

**Setup command(s)**

```bash
# 1. Start the dataPostgreSQL container
docker run --name studioflow-postgres -e POSTGRES_PASSWORD=studioflow123 -e POSTGRES_DB=studioflow -p 5442:5432 -d postgres:16

# 2. Create a .env file in the project root with:
#    DB_HOST=localhost
#    DB_PORT=5442
#    DB_NAME=studioflow
#    DB_USER=postgres
#    DB_PASSWORD=studioflow123

# 3. Create schema and staging tables
psql -h localhost -p 5442 -U postgres -d studioflow -f src/create_schema.sql

```

**First end-to-end pipeline run**

```bash
python src/load_staging.py
psql -h localhost -p 5442 -U postgres -d studioflow -f sql/transform_application_transactions.sql
psql -h localhost -p 5442 -U postgres -d studioflow -f sql/transform_manual_adjustment.sql
psql -h localhost -p 5442 -U postgres -d studioflow -f sql/fct_revenue_events.sql
psql -h localhost -p 5442 -U postgres -d studioflow -f sql/mart_monthly_revenue.sql
psql -h localhost -p 5442 -U postgres -d studioflow -f sql/dq_revenue_issues.sql
psql -h localhost -p 5442 -U postgres -d studioflow -f sql/build_mart_capacity_unified.sql

```

**Second end-to-end pipeline run**

```bash
# Re-run the exact same commands above. Staging uses SHA-256 row-hashing
# (UNIQUE constraint + ON CONFLICT DO NOTHING) so re-running the loader never
# duplicates rows. Every downstream SQL script starts with DROP TABLE IF EXISTS
# / a scoped DELETE, so the whole pipeline is safely idempotent end-to-end.
python src/load_staging.py
psql -h localhost -p 5442 -U postgres -d studioflow -f sql/transform_application_transactions.sql
psql -h localhost -p 5442 -U postgres -d studioflow -f sql/transform_manual_adjustment.sql
psql -h localhost -p 5442 -U postgres -d studioflow -f sql/fct_revenue_events.sql
psql -h localhost -p 5442 -U postgres -d studioflow -f sql/mart_monthly_revenue.sql
psql -h localhost -p 5442 -U postgres -d studioflow -f sql/dq_revenue_issues.sql
psql -h localhost -p 5442 -U postgres -d studioflow -f sql/build_mart_capacity_unified.sql
```

**Tests and data-quality checks**

```bash
# Reconciliation check (should return 0 rows if all months are within $0.01)
SELECT * FROM analytics.mart_monthly_revenue WHERE ABS(variance_usd) > 0.01;
#Full data-quality accounting by severity
SELECT severity, COUNT(*) FROM analytics.dq_revenue_issues GROUP BY severity;

```

**How to inspect the completed PostgreSQL output**

Connect any PostgreSQL client (e.g. DBeaver, psql) to `localhost:5442`, database `studioflow`, user `postgres` (password as set in `.env`, not committed to the repository). All required relations live in the `analytics` schema.

## 3. Result summary

| Item                                        | Result                           | Evidence/location                                                                                                                                                                                                                     |
| ------------------------------------------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Monthly revenue reconciles with Finance     | Pass — all 6 months within $0.01 | `analytics.mart_monthly_revenue`, column `variance_usd`                                                                                                                                                                               |
| Required PostgreSQL relations are available | Pass                             | `analytics.fct_revenue_events`, `analytics.mart_monthly_revenue`, `analytics.dq_revenue_issues`, `analytics.mart_capacity_unified`                                                                                                    |
| Second run is safe                          | Pass                             | CDC `row_hash` (staging) + `DROP`/`DELETE`-then-rebuild pattern (all transform scripts); verified via Airflow — two consecutive DAG runs produced identical row counts (235 `fct_revenue_events` rows, 6 `mart_monthly_revenue` rows) |
| Capacity points and slides remain separate  | Pass                             | `analytics.mart_capacity_unified`, `metric_unit` column strictly `points` or `slides`, never summed                                                                                                                                   |

## 4. Data-quality findings

| Source/category                              | Count | Severity | Handling                                              | Where to verify           |
| -------------------------------------------- | ----: | -------- | ----------------------------------------------------- | ------------------------- |
| field_count_mismatch (malformed CSV row)     |     1 | error    | Excluded, raw row + line number logged                | `staging.stg_load_errors` |
| invalid_transaction_ts (bad timestamp value) |     2 | error    | Excluded via safe-cast function                       | `staging.stg_load_errors` |
| missing_stable_key (blank source_row_id)     |     2 | error    | Excluded, cannot be resolved idempotently             | `staging.stg_load_errors` |
| ambiguous_source_version (conflicting tie)   |     2 | error    | Both versions quarantined, neither chosen arbitrarily | `staging.stg_load_errors` |
| orphan_customer                              |     4 | error    | Excluded, customer_id not in stg_customers            | `staging.stg_load_errors` |
| unsupported_currency                         |     1 | error    | Excluded, no FX rate for month/currency               | `staging.stg_load_errors` |
| unapproved_adjustment (rejected/pending)     |     3 | warning  | Excluded, latest version not approved                 | `staging.stg_load_errors` |
| test_account_excluded                        |    12 | warning  | Excluded per contract (no stakeholder revenue)        | `staging.stg_load_errors` |
| out_of_reporting_window                      |     2 | warning  | Excluded, outside [2026-01-01, 2026-07-01)            | `staging.stg_load_errors` |
| failed_transaction (status != succeeded)     |    10 | warning  | Excluded, non-succeeded status                        | `staging.stg_load_errors` |

All findings consolidated into `analytics.dq_revenue_issues` (39 total: 12 error, 27 warning). Total accounted: 264 source-resolved records (245 application + 19 manual) = 235 accepted + 29 excluded, all logged — zero silent disappearance.

## 5. Key modelling decisions

| Decision                                                               | Why                                                                                                                                                                      | Trade-off or assumption                                                                                      |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| CDC row-hashing (SHA-256 of full row) at staging                       | Guarantees idempotent reloads and correctly recognizes byte-identical replays as a single delivery event, per contract note                                              | Slightly more storage (extra `row_hash` column); does not replace business-key version resolution downstream |
| `safe_to_timestamptz` / `safe_to_numeric` PostgreSQL functions         | Prevents a single malformed value from aborting the entire transform (`CREATE TABLE AS` would fail hard on bad casts)                                                    | Adds two small reusable functions; documented and tested against real malformed values found in the data     |
| Ambiguous version = tie at greatest `updated_at`, count > 1            | Since CDC already deduplicates byte-identical rows, any physical duplicate at the same timestamp must differ in content — cannot be resolved without a business decision | Both conflicting versions are excluded rather than guessed                                                   |
| Reporting-window filter applied at `fct_revenue_events`, not staging   | Keeps staging as a full historical mirror while ensuring only Jan–Jun 2026 activity contributes to revenue                                                               | Requires re-deriving the window filter if scope changes later                                                |
| Airflow orchestrates existing scripts rather than reimplementing logic | Avoids duplicating business logic in two places; Airflow is a thin orchestration layer over already-tested SQL/Python                                                    | Slightly less "Airflow-native" (fewer custom operators), but safer and faster to build correctly             |

## 6. Components implemented

| Component                    | Status        | How to verify                                                                                                        |
| ---------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL pipeline          | Complete      | All 4 required `analytics` relations populated and reconciled                                                        |
| dbt                          | Not attempted | —                                                                                                                    |
| Airflow or alternative       | Complete      | `dags/studioflow_pipeline.py`, 8 tasks, retries=2, reconciliation gate, verified idempotent across 2 manual DAG runs |
| Docker                       | Complete      | `docker-compose.yml` (PostgreSQL data container + Airflow standalone), reproducible from clean clone                 |
| Google Sheets/dry-run export | Not attempted | —                                                                                                                    |
| CI/CD                        | Not attempted | —                                                                                                                    |
| Spark                        | Not attempted | —                                                                                                                    |
| Data governance              | Partial       | DQ reason codes are closed/machine-readable; no formal ownership/retention doc                                       |
| Database security            | Not attempted | —                                                                                                                    |
| PostgreSQL function          | Complete      | `safe_to_timestamptz`, `safe_to_numeric` — used across all transform stages                                          |
| External-data enrichment     | Not attempted | —                                                                                                                    |

## 7. Recorded end-to-end demo

Confirmed: the recording shows the pipeline entry point (Airflow DAG graph view), a completed execution (all 8 tasks green in a single DAG run), the reconciled revenue output (`mart_monthly_revenue` query showing variance within $0.01 for all 6 months), at least one visible data-quality finding (`dq_revenue_issues` query, e.g. the `ambiguous_source_version` or `test_account_excluded` category), evidence of a second safe run (two consecutive DAG triggers producing identical row counts in `fct_revenue_events` and `mart_monthly_revenue`), and the capacity interface (`mart_capacity_unified` grouped by `logic_version`, showing consistent owner/metric/unit mapping).

## 8. Unfinished work and next steps

I deliberately skipped dbt, CI/CD, Spark, external-data enrichment, and formal data-governance documentation to focus my available time on correctness of the core revenue/DQ/capacity logic and on making the Airflow orchestration genuinely idempotent and verifiable, rather than partially implementing every optional item. In production, my next steps would be: (1) add `LEFT JOIN`-based logging for any future new DQ category before it can silently exclude rows (the same pattern already used for orphan_customer/unsupported_currency), (2) migrate the deprecated `PostgresOperator` calls to `SQLExecuteQueryOperator` per Airflow's deprecation warning, (3) add dbt for testable, documented transformations, and (4) add a CI pipeline that runs the full pipeline twice against a fresh PostgreSQL container to continuously verify idempotency.
