-- ----------------------------------------------------------------------------
-- 01: BigQuery datasets (staging -> ods -> dm)
-- Translated from: hive/ddl/01-create-databases.hql
-- All Hive LOCATION directives dropped — BQ manages storage natively.
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
