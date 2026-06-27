-- ----------------------------------------------------------------------------
-- 01-create-datasets.sql  — BigQuery datasets (staging, ods, dm)
-- Migrated from: hive/ddl/01-create-databases.hql
-- Source: NBCS CDH 6.3.4 legacy warehouse → BigQuery
-- Objects: 3 datasets
-- ----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS staging
  OPTIONS(description='Sqoop + SFTP landing mirrors (epoch dates live here)');

CREATE SCHEMA IF NOT EXISTS ods
  OPTIONS(description='Cleansed / conformed / merged (all TIMESTAMPs)');

CREATE SCHEMA IF NOT EXISTS dm
  OPTIONS(description='Dimensional marts + all views');
