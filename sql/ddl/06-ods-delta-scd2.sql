-- ----------------------------------------------------------------------------
-- 06-ods-delta-scd2.sql  — ods: delta-merged entities (8) + SCD-2 histories (3)
-- Migrated from: hive/ddl/06-ods-delta-scd2.hql
-- Source: NBCS CDH 6.3.4 legacy warehouse → BigQuery
--
-- Type mappings applied:
--   BIGINT → INT64, INT → INT64, STRING → STRING, BOOLEAN → BOOL,
--   TIMESTAMP → TIMESTAMP, DECIMAL(p,s) → NUMERIC(p,s)
--
-- Hive constructs dropped:
--   STORED AS PARQUET, TBLPROPERTIES ('parquet.compression'='SNAPPY')
--
-- Partition convention:
--   Hive STRING partition columns promoted to DATE and appended at end.
--   YYYY-MM month columns (work_month, period_month, swap_month,
--   event_month) promoted to first-of-month DATE.
--   Date columns (event_date, snapshot_date) promoted to DATE.
--   SCD-2 tables: eff_from_year INT → INT64, appended at end.
--   All ODS delta/SCD-2 tables are UNPARTITIONED in BigQuery.
-- ----------------------------------------------------------------------------

-- ============================================================================
-- Delta-merged entities (8)
-- ============================================================================

CREATE TABLE IF NOT EXISTS ods.ods_timesheet (
  timesheet_id                INT64,
  agent_id                    INT64,
  work_date                   STRING,
  program_id                  INT64,
  billable_minutes            INT64,
  nonbillable_minutes         INT64,
  approved_flag               BOOL,
  last_change_ts              TIMESTAMP,
  -- former partition column work_month promoted from STRING to DATE
  -- (YYYY-MM → first-of-month DATE)
  work_month                  DATE
);

CREATE TABLE IF NOT EXISTS ods.ods_payroll_adjustment (
  adjustment_id               INT64,
  agent_id                    INT64,
  adj_type                    STRING,
  amount                      NUMERIC(12,2),
  last_change_ts              TIMESTAMP,
  -- former partition column period_month promoted from STRING to DATE
  period_month                DATE
);

CREATE TABLE IF NOT EXISTS ods.ods_sla_credit (
  sla_credit_id               INT64,
  program_id                  INT64,
  sla_target_id               INT64,
  credit_amount               NUMERIC(12,2),
  reason                      STRING,
  last_change_ts              TIMESTAMP,
  period_month                DATE
);

CREATE TABLE IF NOT EXISTS ods.ods_callback_request (
  callback_id                 INT64,
  call_id                     INT64,
  queue_id                    INT64,
  requested_ts                TIMESTAMP,
  scheduled_ts                TIMESTAMP,
  completed_flag              BOOL,
  last_change_ts              TIMESTAMP,
  event_date                  DATE
);

CREATE TABLE IF NOT EXISTS ods.ods_shift_swap (
  swap_id                     INT64,
  requesting_agent_id         INT64,
  accepting_agent_id          INT64,
  schedule_id                 INT64,
  swap_date                   STRING,
  status                      STRING,
  last_change_ts              TIMESTAMP,
  -- former partition column swap_month promoted from STRING to DATE
  swap_month                  DATE
);

CREATE TABLE IF NOT EXISTS ods.ods_ticket_worklog (
  worklog_id                  INT64,
  ticket_id                   INT64,
  agent_id                    INT64,
  minutes_logged              INT64,
  log_ts                      TIMESTAMP,
  note                        STRING,
  last_change_ts              TIMESTAMP,
  event_date                  DATE
);

CREATE TABLE IF NOT EXISTS ods.ods_attrition_event (
  attrition_event_id          INT64,
  agent_id                    INT64,
  notice_ts                   TIMESTAMP,
  last_day                    STRING,
  attrition_type              STRING,
  reason_code                 STRING,
  regrettable_flag            BOOL,
  last_change_ts              TIMESTAMP,
  -- former partition column event_month promoted from STRING to DATE
  event_month                 DATE
);

CREATE TABLE IF NOT EXISTS ods.ods_rate_card (
  rate_card_id                INT64,
  program_id                  INT64,
  service_code                STRING,
  rate                        NUMERIC(12,4),
  currency                    STRING,
  effective_ts                TIMESTAMP,
  expiry_ts                   TIMESTAMP,
  last_change_ts              TIMESTAMP,
  snapshot_date               DATE
);

-- ============================================================================
-- SCD-2 histories (3)
-- Partition column eff_from_year stays INT64 (not date-like).
-- ============================================================================

CREATE TABLE IF NOT EXISTS ods.ods_agent_scd2 (
  agent_history_id            STRING,
  agent_id                    INT64,
  employee_no                 STRING,
  org_unit_id                 INT64,
  job_grade                   STRING,
  employment_type             STRING,
  status                      STRING,
  eff_from_ts                 TIMESTAMP,
  eff_to_ts                   TIMESTAMP,
  is_current                  BOOL,
  -- former partition column eff_from_year INT → INT64
  eff_from_year               INT64
);

CREATE TABLE IF NOT EXISTS ods.ods_agent_skill_scd2 (
  agent_skill_history_id      STRING,
  agent_id                    INT64,
  skill_id                    INT64,
  skill_code                  STRING,
  proficiency                 INT64,
  certified                   BOOL,
  eff_from_ts                 TIMESTAMP,
  eff_to_ts                   TIMESTAMP,
  is_current                  BOOL,
  eff_from_year               INT64
);

CREATE TABLE IF NOT EXISTS ods.ods_agent_assignment_scd2 (
  assignment_history_id       STRING,
  agent_id                    INT64,
  program_id                  INT64,
  queue_id                    INT64,
  role_on_program             STRING,
  eff_from_ts                 TIMESTAMP,
  eff_to_ts                   TIMESTAMP,
  is_current                  BOOL,
  eff_from_year               INT64
);
