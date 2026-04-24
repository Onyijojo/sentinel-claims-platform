-- Sentinel Claims Platform — Warehouse Fact Tables
-- Run after warehouse_dim_tables.sql (foreign key references must exist first).

-- Fact: Claims
CREATE TABLE warehouse.fact_claim (
    claim_key             INTEGER       IDENTITY(1,1) PRIMARY KEY,
    claim_id              INTEGER       NOT NULL,
    claimant_key          INTEGER       REFERENCES warehouse.dim_claimant(claimant_key),
    policy_key            INTEGER       REFERENCES warehouse.dim_policy(policy_key),
    employer_key          INTEGER       REFERENCES warehouse.dim_employer(employer_key),
    incident_date_key     INTEGER       REFERENCES warehouse.dim_date(date_key),
    report_date_key       INTEGER       REFERENCES warehouse.dim_date(date_key),
    claim_type            VARCHAR(50),
    claim_status          VARCHAR(50),
    claim_severity        VARCHAR(20),
    claim_amount          DECIMAL(12,2),
    approved_amount       DECIMAL(12,2)
)
DISTKEY (claim_id)
SORTKEY (incident_date_key);

-- Fact: Payments
CREATE TABLE warehouse.fact_payment (
    payment_key           INTEGER       IDENTITY(1,1) PRIMARY KEY,
    payment_id            INTEGER       NOT NULL,
    claim_key             INTEGER       REFERENCES warehouse.fact_claim(claim_key),
    payment_date_key      INTEGER       REFERENCES warehouse.dim_date(date_key),
    payment_amount        DECIMAL(12,2),
    payment_type          VARCHAR(50)
)
DISTKEY (claim_key)
SORTKEY (payment_date_key);
