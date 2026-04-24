-- Sentinel Claims Platform — Schema Evolution Log Table
-- Stores a record every time a column is added or removed from any entity.

CREATE TABLE IF NOT EXISTS warehouse.schema_evolution_log (
    log_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    entity           VARCHAR(100)    NOT NULL,
    detection_date   DATE            NOT NULL,
    previous_version VARCHAR(20),
    current_version  VARCHAR(20),
    previous_columns VARCHAR(MAX),
    current_columns  VARCHAR(MAX),
    added_columns    VARCHAR(MAX),
    removed_columns  VARCHAR(MAX)
)
SORTKEY (entity, detection_date);
