# Runbook: SCD2 Correction

## What is SCD Type 2?
Slowly Changing Dimension Type 2 (SCD2) tracks historical changes by creating a new row for each change rather than overwriting the existing record. The current record has `is_current = TRUE` and `effective_to = NULL`.

## Affected Tables
- `warehouse.dim_claimant`
- `warehouse.dim_employer`
- `warehouse.dim_policy`

## Detecting SCD2 Issues

### Multiple current records for the same natural key
```sql
SELECT claimant_id, COUNT(*) AS current_count
FROM warehouse.dim_claimant
WHERE is_current = TRUE
GROUP BY claimant_id
HAVING COUNT(*) > 1;
```

### Gap in history (missing period)
```sql
SELECT claimant_id, effective_from, effective_to
FROM warehouse.dim_claimant
WHERE claimant_id = <id>
ORDER BY effective_from;
```

### Overlapping date ranges
```sql
SELECT a.claimant_key, a.effective_from, a.effective_to,
       b.claimant_key, b.effective_from, b.effective_to
FROM warehouse.dim_claimant a
JOIN warehouse.dim_claimant b
  ON a.claimant_id = b.claimant_id
  AND a.claimant_key <> b.claimant_key
  AND a.effective_from < b.effective_to
  AND a.effective_to > b.effective_from;
```

## Fixing a Duplicate Current Record

If two rows both have `is_current = TRUE` for the same natural key:

```sql
-- Step 1: identify the correct current record (most recent effective_from)
SELECT claimant_key, claimant_id, effective_from, effective_to, is_current
FROM warehouse.dim_claimant
WHERE claimant_id = <id>
ORDER BY effective_from DESC;

-- Step 2: close the incorrect current record
UPDATE warehouse.dim_claimant
SET is_current  = FALSE,
    effective_to = CURRENT_DATE
WHERE claimant_key = <wrong_surrogate_key>;
```

## Fixing an Incorrect Historical Record

```sql
-- Step 1: identify the record to correct
SELECT * FROM warehouse.dim_claimant WHERE claimant_key = <key>;

-- Step 2: update the incorrect field
UPDATE warehouse.dim_claimant
SET <column> = '<correct_value>'
WHERE claimant_key = <key>;
```

## Closing a Record That Should Be Closed

```sql
UPDATE warehouse.dim_claimant
SET is_current   = FALSE,
    effective_to = '<end_date>'
WHERE claimant_id = <id>
  AND is_current = TRUE;
```

## After Any Manual Fix

Re-run the SCD2 quality check to confirm the fix:
```sql
SELECT claimant_id, COUNT(*) AS current_count
FROM warehouse.dim_claimant
WHERE is_current = TRUE
GROUP BY claimant_id
HAVING COUNT(*) > 1;
-- Should return 0 rows
```

## Prevention
SCD2 corrections should be rare. The root cause is usually a bug in the transformation or loading job. After fixing data manually, investigate and fix the underlying pipeline logic to prevent recurrence.
