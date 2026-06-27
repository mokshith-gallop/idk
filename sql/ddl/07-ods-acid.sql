-- ----------------------------------------------------------------------------
-- 07-ods-acid.sql  — ods: former Hive ACID transactional tables (4)
-- Migrated from: hive/ddl/07-ods-acid.hql
-- Source: NBCS CDH 6.3.4 legacy warehouse → BigQuery
--
-- Type mappings applied:
--   BIGINT → INT64, STRING → STRING, TIMESTAMP → TIMESTAMP,
--   DECIMAL(p,s) → NUMERIC(p,s)
--
-- Hive constructs dropped:
--   STORED AS ORC, TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY'),
--   CLUSTERED BY ... INTO N BUCKETS
--
-- BigQuery native tables — BQ DML is always transactional; no ACID
-- TBLPROPERTIES needed.
--
-- Clustering (per locked Schema bucketing translation):
--   ods_agent_acid:   CLUSTER BY (agent_id)   — from Hive CLUSTERED BY (agent_id)
--   ods_ticket_acid:  CLUSTER BY (ticket_id)  — from Hive CLUSTERED BY (ticket_id)
--   ods_client_acid:  no clustering (tiny table, 10 rows)
--   ods_invoice_acid: no clustering (tiny table, 290 rows)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ods.ods_client_acid (
  client_id                   INT64,
  client_code                 STRING,
  client_name                 STRING,
  industry                    STRING,
  hq_country                  STRING,
  status                      STRING,
  created_ts                  TIMESTAMP,
  updated_ts                  TIMESTAMP
);

-- ods_agent_acid: CLUSTER BY (agent_id) — bucketing translation
CREATE TABLE IF NOT EXISTS ods.ods_agent_acid (
  agent_id                    INT64,
  employee_no                 STRING,
  full_name                   STRING,
  email                       STRING,
  org_unit_id                 INT64,
  job_grade                   STRING,
  employment_type             STRING,
  hire_ts                     TIMESTAMP,
  term_ts                     TIMESTAMP,
  status                      STRING
)
CLUSTER BY agent_id;

-- ods_ticket_acid: CLUSTER BY (ticket_id) — bucketing translation
CREATE TABLE IF NOT EXISTS ods.ods_ticket_acid (
  ticket_id                   INT64,
  ticket_no                   STRING,
  program_id                  INT64,
  category_id                 INT64,
  assigned_agent_id           INT64,
  priority                    STRING,
  status                      STRING,
  created_ts                  TIMESTAMP,
  updated_ts                  TIMESTAMP,
  resolved_ts                 TIMESTAMP
)
CLUSTER BY ticket_id;

CREATE TABLE IF NOT EXISTS ods.ods_invoice_acid (
  invoice_id                  INT64,
  invoice_no                  STRING,
  client_id                   INT64,
  program_id                  INT64,
  period_month                STRING,
  issued_ts                   TIMESTAMP,
  due_ts                      TIMESTAMP,
  currency                    STRING,
  total_amount                NUMERIC(14,2),
  status                      STRING
);
