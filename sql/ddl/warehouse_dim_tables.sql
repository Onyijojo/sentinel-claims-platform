-- Sentinel Claims Platform — Warehouse Dimension Tables
-- Date dimension (pre-populated calendar table)
CREATE TABLE warehouse.dim_date (
    date_key        INTEGER       NOT NULL PRIMARY KEY,
    full_date       DATE          NOT NULL,
    year            INTEGER       NOT NULL,
    quarter         INTEGER       NOT NULL,
    month           INTEGER       NOT NULL,
    month_name      VARCHAR(20)   NOT NULL,
    day_of_month    INTEGER       NOT NULL,
    day_of_week     INTEGER       NOT NULL,
    day_name        VARCHAR(20)   NOT NULL,
    is_weekend      BOOLEAN       NOT NULL,
    is_month_end    BOOLEAN       NOT NULL
)
DISTSTYLE ALL
SORTKEY (full_date);

-- Claimant dimension (SCD Type 2)

CREATE TABLE warehouse.dim_claimant (
    claimant_key          INTEGER       IDENTITY(1,1) PRIMARY KEY,
    claimant_id           INTEGER       NOT NULL,
    first_name            VARCHAR(100),
    last_name             VARCHAR(100),
    date_of_birth         DATE,
    gender                VARCHAR(10),
    employment_start_date DATE,
    employer_id           INTEGER,
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    effective_from        DATE          NOT NULL,
    effective_to          DATE,
    is_current            BOOLEAN       NOT NULL DEFAULT TRUE
)
DISTSTYLE ALL
SORTKEY (claimant_id, effective_from);

-- Employer dimension (SCD Type 2)
CREATE TABLE warehouse.dim_employer (
    employer_key          INTEGER       IDENTITY(1,1) PRIMARY KEY,
    employer_id           INTEGER       NOT NULL,
    company_name          VARCHAR(200),
    industry              VARCHAR(100),
    location              VARCHAR(200),
    policy_id             INTEGER,
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    effective_from        DATE          NOT NULL,
    effective_to          DATE,
    is_current            BOOLEAN       NOT NULL DEFAULT TRUE
)
DISTSTYLE ALL
SORTKEY (employer_id, effective_from);

-- Policy dimension (SCD Type 2)

CREATE TABLE warehouse.dim_policy (
    policy_key            INTEGER       IDENTITY(1,1) PRIMARY KEY,
    policy_id             INTEGER       NOT NULL,
    policy_number         VARCHAR(50),
    coverage_type         VARCHAR(50),
    start_date            DATE,
    end_date              DATE,
    premium_amount        DECIMAL(12,2),
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    effective_from        DATE          NOT NULL,
    effective_to          DATE,
    is_current            BOOLEAN       NOT NULL DEFAULT TRUE
)
DISTSTYLE ALL
SORTKEY (policy_id, effective_from);
