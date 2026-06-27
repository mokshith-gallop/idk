-- ----------------------------------------------------------------------------
-- 03-staging-delta-feeds: staging: CDC delta feeds, pipe-delimited (8)
-- Translated from: hive/ddl/03-staging-delta-feeds.hql
-- Hive constructs dropped: EXTERNAL, ROW FORMAT DELIMITED, STORED AS TEXTFILE,
--   LOCATION. Partition extract_ts STRING kept as STRING (not date-like).
-- Type map: BIGINT→INT64, INT→INT64, BOOLEAN→BOOL, DECIMAL(p,s)→NUMERIC(p,s).
-- All source COMMENTs carried as OPTIONS(description=...).
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS staging.stg_fin_timesheet_delta (
  timesheet_id                INT64,
  agent_id                    INT64,
  work_date                   STRING,
  program_id                  INT64,
  billable_minutes            INT64,
  nonbillable_minutes         INT64,
  approved_flag               BOOL,
  op                          STRING,
  change_ms                   INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  extract_ts                  STRING
);

CREATE TABLE IF NOT EXISTS staging.stg_fin_payroll_adj_delta (
  adjustment_id               INT64,
  agent_id                    INT64,
  period_month                STRING,
  adj_type                    STRING,
  amount                      NUMERIC(12, 2),
  op                          STRING,
  change_ms                   INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  extract_ts                  STRING
);

CREATE TABLE IF NOT EXISTS staging.stg_crm_sla_credit_delta (
  sla_credit_id               INT64,
  program_id                  INT64,
  sla_target_id               INT64,
  period_month                STRING,
  credit_amount               NUMERIC(12, 2),
  reason                      STRING,
  op                          STRING,
  change_ms                   INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  extract_ts                  STRING
);

CREATE TABLE IF NOT EXISTS staging.stg_tel_callback_request_delta (
  callback_id                 INT64,
  call_id                     INT64,
  queue_id                    INT64,
  requested_epoch             INT64 OPTIONS (description = 'epoch SECONDS (legacy)'),
  scheduled_epoch             INT64 OPTIONS (description = 'epoch SECONDS (legacy)'),
  completed_flag              BOOL,
  op                          STRING,
  change_ms                   INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  extract_ts                  STRING
);

CREATE TABLE IF NOT EXISTS staging.stg_wfm_shift_swap_delta (
  swap_id                     INT64,
  requesting_agent_id         INT64,
  accepting_agent_id          INT64,
  schedule_id                 INT64,
  swap_date                   STRING,
  status                      STRING,
  op                          STRING,
  change_ms                   INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  extract_ts                  STRING
);

CREATE TABLE IF NOT EXISTS staging.stg_tkt_worklog_delta (
  worklog_id                  INT64,
  ticket_id                   INT64,
  agent_id                    INT64,
  minutes_logged              INT64,
  log_ms                      INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  note                        STRING,
  op                          STRING,
  change_ms                   INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  extract_ts                  STRING
);

CREATE TABLE IF NOT EXISTS staging.stg_hr_attrition_event_delta (
  attrition_event_id          INT64,
  agent_id                    INT64,
  notice_epoch                INT64 OPTIONS (description = 'epoch SECONDS (legacy)'),
  last_day                    STRING,
  attrition_type              STRING,
  reason_code                 STRING,
  regrettable_flag            BOOL,
  op                          STRING,
  change_ms                   INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  extract_ts                  STRING
);

CREATE TABLE IF NOT EXISTS staging.stg_fin_rate_card_change_delta (
  rate_change_id              INT64,
  rate_card_id                INT64,
  old_rate                    NUMERIC(12, 4),
  new_rate                    NUMERIC(12, 4),
  change_reason               STRING,
  op                          STRING,
  change_ms                   INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  extract_ts                  STRING
);
