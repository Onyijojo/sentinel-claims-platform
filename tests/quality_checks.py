"""
Data quality checks for the Sentinel Claims Platform.
Connects to Redshift Serverless and runs all checks, reporting PASS/FAIL per check.
Raises an exception if any check fails so Airflow marks the task as failed.
"""

import os
import sys
import redshift_connector

CHECKS = [
    (
        "fact_claim PK uniqueness",
        """
        SELECT CASE WHEN COUNT(*) = COUNT(DISTINCT claim_id) THEN 'PASS' ELSE 'FAIL' END AS result,
               COUNT(*) AS total_rows, COUNT(DISTINCT claim_id) AS distinct_keys
        FROM warehouse.fact_claim
        """,
        lambda row: row[0] == "PASS",
    ),
    (
        "fact_claim FK claimant_key",
        """
        SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
               COUNT(*) AS orphan_count
        FROM warehouse.fact_claim fc
        LEFT JOIN warehouse.dim_claimant dc ON fc.claimant_key = dc.claimant_key
        WHERE dc.claimant_key IS NULL AND fc.claimant_key IS NOT NULL
        """,
        lambda row: row[0] == "PASS",
    ),
    (
        "fact_claim non-negative amounts",
        """
        SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
               COUNT(*) AS violation_count
        FROM warehouse.fact_claim
        WHERE claim_amount < 0 OR approved_amount < 0
        """,
        lambda row: row[0] == "PASS",
    ),
    (
        "fact_claim row count >= 4500",
        """
        SELECT CASE WHEN COUNT(*) >= 4500 THEN 'PASS' ELSE 'FAIL' END AS result,
               COUNT(*) AS actual_count
        FROM warehouse.fact_claim
        """,
        lambda row: row[0] == "PASS",
    ),
    (
        "fact_claim valid date keys",
        """
        SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
               COUNT(*) AS invalid_count
        FROM warehouse.fact_claim fc
        LEFT JOIN warehouse.dim_date d ON fc.incident_date_key = d.date_key
        WHERE d.date_key IS NULL AND fc.incident_date_key IS NOT NULL
        """,
        lambda row: row[0] == "PASS",
    ),
    (
        "dim_claimant SCD2 integrity",
        """
        SELECT CASE WHEN MAX(current_count) = 1 THEN 'PASS' ELSE 'FAIL' END AS result
        FROM (
            SELECT claimant_id, COUNT(*) AS current_count
            FROM warehouse.dim_claimant
            WHERE is_current = TRUE
            GROUP BY claimant_id
        )
        """,
        lambda row: row[0] == "PASS",
    ),
    (
        "claim_status standardized",
        """
        SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
               COUNT(*) AS violation_count
        FROM warehouse.fact_claim
        WHERE claim_status NOT IN ('open', 'closed', 'denied')
        """,
        lambda row: row[0] == "PASS",
    ),
    (
        "dim_policy SCD2 integrity",
        """
        SELECT CASE WHEN MAX(current_count) = 1 THEN 'PASS' ELSE 'FAIL' END AS result
        FROM (
            SELECT policy_id, COUNT(*) AS current_count
            FROM warehouse.dim_policy
            WHERE is_current = TRUE
            GROUP BY policy_id
        )
        """,
        lambda row: row[0] == "PASS",
    ),
    (
        "fact_payment FK claim_key",
        """
        SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
               COUNT(*) AS orphan_count
        FROM warehouse.fact_payment fp
        LEFT JOIN warehouse.fact_claim fc ON fp.claim_key = fc.claim_key
        WHERE fc.claim_key IS NULL
        """,
        lambda row: row[0] == "PASS",
    ),
]


def get_connection():
    return redshift_connector.connect(
        host=os.environ["REDSHIFT_HOST"],
        database=os.environ.get("REDSHIFT_DB", "sentinel_dw"),
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
        port=int(os.environ.get("REDSHIFT_PORT", 5439)),
    )


def run_quality_checks(**context):
    conn = get_connection()
    cursor = conn.cursor()

    results = []
    failed = []

    print("\n========== DATA QUALITY CHECKS ==========")
    for check_name, query, passes in CHECKS:
        cursor.execute(query)
        row = cursor.fetchone()
        status = "PASS" if passes(row) else "FAIL"
        print(f"  [{status}] {check_name} — {row}")
        results.append((check_name, status, row))
        if status == "FAIL":
            failed.append(check_name)

    print("=========================================")
    print(f"  {len(results) - len(failed)}/{len(results)} checks passed")

    cursor.close()
    conn.close()

    if failed:
        raise ValueError(f"Quality checks FAILED: {', '.join(failed)}")


if __name__ == "__main__":
    run_quality_checks()
    print("All quality checks passed.")
