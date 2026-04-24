-- Sentinel Claims Platform — Staging Tables
-- Mirrors the columns in the S3 landing Parquet files exactly.
-- These tables are truncated and reloaded on every pipeline run.

CREATE TABLE IF NOT EXISTS staging.stg_claimants (
    claimant_id           INTEGER        NOT NULL,
    first_name            VARCHAR(100),
    last_name             VARCHAR(100),
    date_of_birth         DATE,
    gender                VARCHAR(10),
    employment_start_date DATE,
    employer_id           INTEGER,
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    effective_from        DATE,
    effective_to          DATE,
    is_current            BOOLEAN
);

CREATE TABLE IF NOT EXISTS staging.stg_claims (
    claim_id              INTEGER        NOT NULL,
    claimant_id           INTEGER,
    policy_id             INTEGER,
    incident_date         DATE,
    report_date           DATE,
    claim_type            VARCHAR(50),
    claim_status          VARCHAR(50),
    claim_amount          FLOAT8,
    approved_amount       FLOAT8,
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    claim_severity        VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS staging.stg_employers (
    employer_id           INTEGER        NOT NULL,
    company_name          VARCHAR(200),
    industry              VARCHAR(100),
    location              VARCHAR(200),
    policy_id             INTEGER,
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    effective_from        DATE,
    effective_to          DATE,
    is_current            BOOLEAN
);

CREATE TABLE IF NOT EXISTS staging.stg_payments (
    payment_id            INTEGER        NOT NULL,
    claim_id              INTEGER,
    payment_date          DATE,
    payment_amount        FLOAT8,
    payment_type          VARCHAR(50),
    created_at            TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_policies (
    policy_id             INTEGER        NOT NULL,
    policy_number         VARCHAR(50),
    coverage_type         VARCHAR(50),
    start_date            DATE,
    end_date              DATE,
    premium_amount        FLOAT8,
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    effective_from        DATE,
    effective_to          DATE,
    is_current            BOOLEAN
);
