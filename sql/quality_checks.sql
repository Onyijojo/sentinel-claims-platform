-- Sentinel Claims Platform — Data Quality Checks
-- Run after every pipeline load to validate data integrity

-- CHECK 1: Primary key uniqueness on fact_claim
SELECT 'fact_claim PK uniqueness' AS check_name,
       CASE WHEN COUNT(*) = COUNT(DISTINCT claim_id) THEN 'PASS' ELSE 'FAIL' END AS result,
       COUNT(*)              AS total_rows,
       COUNT(DISTINCT claim_id) AS distinct_keys
FROM warehouse.fact_claim;

-- CHECK 2: Referential integrity — claims must link to a valid claimant
SELECT 'fact_claim FK claimant_key' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
       COUNT(*) AS orphan_count
FROM warehouse.fact_claim fc
LEFT JOIN warehouse.dim_claimant dc ON fc.claimant_key = dc.claimant_key
WHERE dc.claimant_key IS NULL
  AND fc.claimant_key IS NOT NULL;

-- CHECK 3: No negative monetary amounts
SELECT 'fact_claim non-negative amounts' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
       COUNT(*) AS violation_count
FROM warehouse.fact_claim
WHERE claim_amount < 0
   OR approved_amount < 0;

-- CHECK 4: Row count reasonableness — detect truncated loads
SELECT 'fact_claim row count' AS check_name,
       CASE WHEN COUNT(*) >= 2000 THEN 'PASS' ELSE 'FAIL' END AS result,
       COUNT(*) AS actual_count
FROM warehouse.fact_claim;

-- CHECK 5: Date key validity — every incident date must resolve in dim_date
SELECT 'fact_claim valid date keys' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
       COUNT(*) AS invalid_count
FROM warehouse.fact_claim fc
LEFT JOIN warehouse.dim_date d ON fc.incident_date_key = d.date_key
WHERE d.date_key IS NULL
  AND fc.incident_date_key IS NOT NULL;

-- CHECK 6: SCD2 integrity on dim_claimant — exactly one current record per natural key
SELECT 'dim_claimant SCD2 integrity' AS check_name,
       CASE WHEN MAX(current_count) = 1 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT claimant_id, COUNT(*) AS current_count
    FROM warehouse.dim_claimant
    WHERE is_current = TRUE
    GROUP BY claimant_id
);

-- CHECK 7: claim_status must only contain approved values
SELECT 'claim_status standardized' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
       COUNT(*) AS violation_count
FROM warehouse.fact_claim
WHERE claim_status NOT IN ('open', 'closed', 'denied');

-- CHECK 8: SCD2 integrity on dim_policy
SELECT 'dim_policy SCD2 integrity' AS check_name,
       CASE WHEN MAX(current_count) = 1 THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
    SELECT policy_id, COUNT(*) AS current_count
    FROM warehouse.dim_policy
    WHERE is_current = TRUE
    GROUP BY policy_id
);

-- CHECK 9: fact_payment links to a valid claim
SELECT 'fact_payment FK claim_key' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
       COUNT(*) AS orphan_count
FROM warehouse.fact_payment fp
LEFT JOIN warehouse.fact_claim fc ON fp.claim_key = fc.claim_key
WHERE fc.claim_key IS NULL;

-- CHECK 10: No NULL primary keys on any fact or dimension table
SELECT 'fact_claim no NULL claim_id'   AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
       COUNT(*) AS null_count
FROM warehouse.fact_claim
WHERE claim_id IS NULL
UNION ALL
SELECT 'dim_claimant no NULL claimant_id',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*)
FROM warehouse.dim_claimant
WHERE claimant_id IS NULL
UNION ALL
SELECT 'dim_policy no NULL policy_id',
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
       COUNT(*)
FROM warehouse.dim_policy
WHERE policy_id IS NULL;
