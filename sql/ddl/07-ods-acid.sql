-- ----------------------------------------------------------------------------
-- 07-ods-acid: ods: Former Hive ACID transactional tables (4)
-- Migrated from Hive ORC ACID tables to native BigQuery tables.
-- Dropped: CLUSTERED BY...INTO N BUCKETS, STORED AS ORC,
--          TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY').
-- BQ DML is always transactional — no TBLPROPERTIES equivalent needed.
-- Type map: BIGINT→INT64, STRING→STRING, TIMESTAMP→TIMESTAMP,
--           DECIMAL(p,s)→NUMERIC(p,s).
-- ods_agent_acid: CLUSTER BY (agent_id)  — Schema bucketing translation.
-- ods_ticket_acid: CLUSTER BY (ticket_id) — Schema bucketing translation.
-- ods_client_acid, ods_invoice_acid: no clustering (tiny tables).
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
CLUSTER BY (agent_id);

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
CLUSTER BY (ticket_id);

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
