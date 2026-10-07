-- Run once in a Databricks SQL editor or notebook.
-- Creates the catalog, the medallion schemas and a Volume for the raw files.

CREATE CATALOG IF NOT EXISTS rides;

CREATE SCHEMA IF NOT EXISTS rides.bronze COMMENT 'Raw events and lookup tables';
CREATE SCHEMA IF NOT EXISTS rides.silver COMMENT 'Parsed, validated and enriched rides';
CREATE SCHEMA IF NOT EXISTS rides.gold   COMMENT 'Star schema and aggregates';

-- Upload everything in the repo's data/ folder here (Catalog Explorer > Upload,
-- or: databricks fs cp -r data dbfs:/Volumes/rides/bronze/raw)
CREATE VOLUME IF NOT EXISTS rides.bronze.raw COMMENT 'Landing zone for raw JSON files';
