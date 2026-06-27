-- ----------------------------------------------------------------------------
-- 01-create-datasets: BigQuery datasets (3)
-- Migrated from Hive databases (staging, ods, dm) in the NBCS CDH warehouse.
-- Each dataset corresponds to a logical layer in the data pipeline.
-- ----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS staging
OPTIONS (
  description = 'Sqoop + SFTP landing mirrors (epoch dates live here)'
);

CREATE SCHEMA IF NOT EXISTS ods
OPTIONS (
  description = 'Cleansed / conformed / merged (all TIMESTAMPs)'
);

CREATE SCHEMA IF NOT EXISTS dm
OPTIONS (
  description = 'Dimensional marts + all views'
);
