"""
Load semua file CSV di seeds/ ke tabel staging masing-masing.
Baris malformed dicatat ke staging.stg_load_errors, tidak menghentikan proses.
Menggunakan CDC row-hashing supaya idempotent (aman di-rerun tanpa duplikat).
"""
import csv
import hashlib
from db_connection import get_connection


def compute_row_hash(row):
    """Hash SHA-256 dari seluruh isi baris — teknik CDC row-hashing."""
    row_string = "|".join(row)
    return hashlib.sha256(row_string.encode("utf-8")).hexdigest()


def load_csv_to_staging(conn, csv_filename, table_name, expected_columns, column_names):
    """
    Fungsi generik: baca 1 CSV, insert ke 1 tabel staging.
    Dipakai berulang untuk semua file supaya tidak duplikasi kode.
    """
    cursor = conn.cursor()
    inserted_count = 0
    skipped_duplicate_count = 0
    error_count = 0

    csv_path = f"seeds/{csv_filename}"

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

        for line_number, row in enumerate(reader, start=2):
            if len(row) != expected_columns:
                cursor.execute(
                    """
                    INSERT INTO staging.stg_load_errors
                        (source_file, source_line_number, raw_content, error_reason)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        csv_filename,
                        line_number,
                        str(row),
                        f"field_count_mismatch: expected {expected_columns}, got {len(row)}",
                    ),
                )
                error_count += 1
                continue

            row_hash = compute_row_hash(row)

            # Bangun query INSERT dinamis berdasarkan jumlah kolom
            placeholders = ", ".join(["%s"] * (expected_columns + 2))  # +2 = line_number, row_hash
            columns_sql = ", ".join(column_names + ["source_line_number", "row_hash"])
            values = list(row) + [line_number, row_hash]

            cursor.execute(
                f"""
                INSERT INTO {table_name} ({columns_sql})
                VALUES ({placeholders})
                ON CONFLICT (row_hash) DO NOTHING
                """,
                values,
            )

            if cursor.rowcount == 0:
                skipped_duplicate_count += 1
            else:
                inserted_count += 1

    conn.commit()  # <-- FIXED: conn, bukan con

    # Kalau ada error, tampilkan detail baris mana saja yang bermasalah
    print(
        f"{csv_filename}: {inserted_count} baris baru, "
        f"{skipped_duplicate_count} duplikat di-skip (CDC), "
        f"{error_count} baris error"
    )

    if error_count > 0:                              # <-- FIXED: indentasi konsisten
        cursor.execute(
            """
            SELECT source_line_number, error_reason
            FROM staging.stg_load_errors
            WHERE source_file = %s
            ORDER BY source_line_number
            """,
            (csv_filename,),
        )
        error_details = cursor.fetchall()             # <-- di DALAM if, 8 spasi
        for line_num, reason in error_details:
            print(f"  └─ baris {line_num}: {reason}")

    cursor.close()
    
def reset_error_log(conn):
    """
    Kosongkan log error di awal tiap run.
    Beda dengan tabel data (pakai CDC row-hash), log error
    tidak perlu idempotent lewat hash — cukup di-reset tiap run
    supaya selalu mencerminkan kondisi TERKINI, bukan akumulasi lama.
    """
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE staging.stg_load_errors;")
    conn.commit()
    cursor.close()


def main():
    conn = get_connection()
    reset_error_log(conn)
    files_to_load = [
        {
            "csv_filename": "customers.csv",
            "table_name": "staging.stg_customers",
            "expected_columns": 7,
            "column_names": [
                "customer_id", "customer_name", "country_code",
                "billing_currency", "signup_date", "is_test_account", "account_status",
            ],
        },
        {
            "csv_filename": "application_transactions.csv",
            "table_name": "staging.stg_application_transactions",
            "expected_columns": 9,
            "column_names": [
                "transaction_id", "customer_id", "transaction_ts", "transaction_type",
                "status", "amount", "currency", "provider_reference", "ingested_at",
            ],
        },
        {
            "csv_filename": "manual_adjustments.csv",
            "table_name": "staging.stg_manual_adjustments",
            "expected_columns": 10,
            "column_names": [
                "source_row_id", "spreadsheet_row", "occurred_at", "customer_id",
                "adjustment_type", "amount", "currency", "reason",
                "approval_status", "updated_at",
            ],
        },
        {
            "csv_filename": "fx_rates.csv",
            "table_name": "staging.stg_fx_rates",
            "expected_columns": 3,
            "column_names": ["month_start", "currency", "usd_per_unit"],
        },
        {
            "csv_filename": "finance_control_totals.csv",
            "table_name": "staging.stg_finance_control_totals",
            "expected_columns": 6,
            "column_names": [
                "month_start", "application_net_revenue_usd", "manual_net_revenue_usd",
                "expected_net_revenue_usd", "expected_application_record_count",
                "expected_manual_record_count",
            ],
        },
        {
            "csv_filename": "capacity_records.csv",
            "table_name": "staging.stg_capacity_records",
            "expected_columns": 8,
            "column_names": [
                "record_id", "period_start", "team_id", "designer_id",
                "capacity_points", "slides_completed", "logic_version", "updated_at",
            ],
        },
    ]

    for file_config in files_to_load:
        try:
            load_csv_to_staging(conn, **file_config)
        except Exception as e:
            print(f"GAGAL memproses {file_config['csv_filename']}: {e}")
            conn.rollback()

    conn.close()


if __name__ == "__main__":
    main()