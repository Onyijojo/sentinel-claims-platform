# Data Dictionary — Sentinel Claims Platform

## Schemas

| Schema | Purpose |
|---|---|
| `staging` | Temporary landing area. Truncated and reloaded each pipeline run from S3 landing zone. |
| `warehouse` | Permanent dimensional model (facts, dimensions, SCD2). |
| `analytics` | Views and aggregations for reporting consumers. |

---

## Staging Tables

### `staging.stg_claimants`
| Column | Type | Description |
|---|---|---|
| `claimant_id` | INTEGER | Natural key — unique claimant identifier |
| `first_name` | VARCHAR(100) | Claimant first name |
| `last_name` | VARCHAR(100) | Claimant last name |
| `date_of_birth` | DATE | Claimant date of birth |
| `gender` | VARCHAR(10) | Claimant gender |
| `employment_start_date` | DATE | Date claimant started employment |
| `employer_id` | INTEGER | FK to employer |
| `created_at` | TIMESTAMP | Record creation timestamp |
| `updated_at` | TIMESTAMP | Record last updated timestamp |
| `effective_from` | DATE | SCD2 validity start date |
| `effective_to` | DATE | SCD2 validity end date (NULL = current) |
| `is_current` | BOOLEAN | SCD2 current record flag |

### `staging.stg_claims`
| Column | Type | Description |
|---|---|---|
| `claim_id` | INTEGER | Natural key — unique claim identifier |
| `claimant_id` | INTEGER | FK to claimant |
| `policy_id` | INTEGER | FK to policy (NULL for ~50% of records — dropped in transformation) |
| `incident_date` | DATE | Date of the workplace incident |
| `report_date` | DATE | Date the claim was reported |
| `claim_type` | VARCHAR(50) | Type of claim (e.g. medical, lost wages) |
| `claim_status` | VARCHAR(50) | Standardised: `open`, `closed`, `denied` |
| `claim_amount` | FLOAT8 | Total claimed amount |
| `approved_amount` | FLOAT8 | Amount approved for payment |
| `created_at` | TIMESTAMP | Record creation timestamp |
| `updated_at` | TIMESTAMP | Record last updated timestamp |
| `claim_severity` | VARCHAR(20) | Severity level — NULL for v1 records |

### `staging.stg_employers`
| Column | Type | Description |
|---|---|---|
| `employer_id` | INTEGER | Natural key — unique employer identifier |
| `company_name` | VARCHAR(200) | Legal company name |
| `industry` | VARCHAR(100) | Industry sector |
| `location` | VARCHAR(200) | Company location |
| `policy_id` | INTEGER | FK to associated policy |
| `created_at` | TIMESTAMP | Record creation timestamp |
| `updated_at` | TIMESTAMP | Record last updated timestamp |
| `effective_from` | DATE | SCD2 validity start date |
| `effective_to` | DATE | SCD2 validity end date (NULL = current) |
| `is_current` | BOOLEAN | SCD2 current record flag |

### `staging.stg_payments`
| Column | Type | Description |
|---|---|---|
| `payment_id` | INTEGER | Natural key — unique payment identifier |
| `claim_id` | INTEGER | FK to claim |
| `payment_date` | DATE | Date payment was made |
| `payment_amount` | FLOAT8 | Payment amount |
| `payment_type` | VARCHAR(50) | Type of payment (e.g. medical, indemnity) |
| `created_at` | TIMESTAMP | Record creation timestamp |

### `staging.stg_policies`
| Column | Type | Description |
|---|---|---|
| `policy_id` | INTEGER | Natural key — unique policy identifier |
| `policy_number` | VARCHAR(50) | Human-readable policy number |
| `coverage_type` | VARCHAR(50) | Type of coverage |
| `start_date` | DATE | Policy start date |
| `end_date` | DATE | Policy end date |
| `premium_amount` | FLOAT8 | Annual premium amount |
| `created_at` | TIMESTAMP | Record creation timestamp |
| `updated_at` | TIMESTAMP | Record last updated timestamp |
| `effective_from` | DATE | SCD2 validity start date |
| `effective_to` | DATE | SCD2 validity end date (NULL = current) |
| `is_current` | BOOLEAN | SCD2 current record flag |

---

## Warehouse Tables

### `warehouse.dim_date`
Pre-populated calendar dimension covering the full date range of the dataset.

| Column | Type | Description |
|---|---|---|
| `date_key` | INTEGER | Surrogate key (format: YYYYMMDD) |
| `full_date` | DATE | Calendar date |
| `year` | INTEGER | Year |
| `quarter` | INTEGER | Quarter (1–4) |
| `month` | INTEGER | Month number (1–12) |
| `month_name` | VARCHAR(20) | Month name (e.g. January) |
| `day_of_month` | INTEGER | Day of month (1–31) |
| `day_of_week` | INTEGER | Day of week (1=Sunday) |
| `day_name` | VARCHAR(20) | Day name (e.g. Monday) |
| `is_weekend` | BOOLEAN | TRUE if Saturday or Sunday |
| `is_month_end` | BOOLEAN | TRUE if last day of month |

### `warehouse.dim_claimant` *(SCD Type 2)*
| Column | Type | Description |
|---|---|---|
| `claimant_key` | INTEGER | Surrogate key (IDENTITY) |
| `claimant_id` | INTEGER | Natural key |
| `first_name` | VARCHAR(100) | Claimant first name |
| `last_name` | VARCHAR(100) | Claimant last name |
| `date_of_birth` | DATE | Date of birth |
| `gender` | VARCHAR(10) | Gender |
| `employment_start_date` | DATE | Employment start date |
| `employer_id` | INTEGER | FK to employer |
| `created_at` | TIMESTAMP | Source record creation timestamp |
| `updated_at` | TIMESTAMP | Source record last updated timestamp |
| `effective_from` | DATE | SCD2 validity start date |
| `effective_to` | DATE | SCD2 validity end date (NULL = current record) |
| `is_current` | BOOLEAN | TRUE for the active version of the record |

### `warehouse.dim_employer` *(SCD Type 2)*
| Column | Type | Description |
|---|---|---|
| `employer_key` | INTEGER | Surrogate key (IDENTITY) |
| `employer_id` | INTEGER | Natural key |
| `company_name` | VARCHAR(200) | Legal company name |
| `industry` | VARCHAR(100) | Industry sector |
| `location` | VARCHAR(200) | Company location |
| `policy_id` | INTEGER | Associated policy |
| `created_at` | TIMESTAMP | Source record creation timestamp |
| `updated_at` | TIMESTAMP | Source record last updated timestamp |
| `effective_from` | DATE | SCD2 validity start date |
| `effective_to` | DATE | SCD2 validity end date (NULL = current record) |
| `is_current` | BOOLEAN | TRUE for the active version of the record |

### `warehouse.dim_policy` *(SCD Type 2)*
| Column | Type | Description |
|---|---|---|
| `policy_key` | INTEGER | Surrogate key (IDENTITY) |
| `policy_id` | INTEGER | Natural key |
| `policy_number` | VARCHAR(50) | Human-readable policy number |
| `coverage_type` | VARCHAR(50) | Type of coverage |
| `start_date` | DATE | Policy start date |
| `end_date` | DATE | Policy end date |
| `premium_amount` | DECIMAL(12,2) | Annual premium |
| `created_at` | TIMESTAMP | Source record creation timestamp |
| `updated_at` | TIMESTAMP | Source record last updated timestamp |
| `effective_from` | DATE | SCD2 validity start date |
| `effective_to` | DATE | SCD2 validity end date (NULL = current record) |
| `is_current` | BOOLEAN | TRUE for the active version of the record |

### `warehouse.fact_claim`
| Column | Type | Description |
|---|---|---|
| `claim_key` | INTEGER | Surrogate key (IDENTITY) |
| `claim_id` | INTEGER | Natural key |
| `claimant_key` | INTEGER | FK to `dim_claimant` |
| `policy_key` | INTEGER | FK to `dim_policy` |
| `employer_key` | INTEGER | FK to `dim_employer` |
| `incident_date_key` | INTEGER | FK to `dim_date` (incident date) |
| `report_date_key` | INTEGER | FK to `dim_date` (report date) |
| `claim_type` | VARCHAR(50) | Type of claim |
| `claim_status` | VARCHAR(50) | `open`, `closed`, or `denied` |
| `claim_severity` | VARCHAR(20) | Severity level |
| `claim_amount` | DECIMAL(12,2) | Total claimed amount |
| `approved_amount` | DECIMAL(12,2) | Amount approved for payment |

### `warehouse.fact_payment`
| Column | Type | Description |
|---|---|---|
| `payment_key` | INTEGER | Surrogate key (IDENTITY) |
| `payment_id` | INTEGER | Natural key |
| `claim_key` | INTEGER | FK to `fact_claim` |
| `payment_date_key` | INTEGER | FK to `dim_date` |
| `payment_amount` | DECIMAL(12,2) | Payment amount |
| `payment_type` | VARCHAR(50) | Type of payment |
