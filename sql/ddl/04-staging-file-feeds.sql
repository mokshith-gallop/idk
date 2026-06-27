-- ----------------------------------------------------------------------------
-- 04-staging-file-feeds.sql  — staging: SFTP/file-landed client feeds (10)
-- Migrated from: hive/ddl/04-staging-file-feeds.hql
-- Source: NBCS CDH 6.3.4 legacy warehouse → BigQuery
--
-- Type mappings applied:
--   BIGINT → INT64, INT → INT64, STRING → STRING, BOOLEAN → BOOL,
--   DECIMAL(p,s) → NUMERIC(p,s), DOUBLE → FLOAT64
--
-- Complex type mappings:
--   ARRAY<STRUCT<...>>  → ARRAY<STRUCT<...>>  (REPEATED RECORD)
--   MAP<STRING,STRING>  → JSON
--   ARRAY<STRING>       → ARRAY<STRING>       (REPEATED STRING)
--
-- Hive constructs dropped:
--   EXTERNAL, ROW FORMAT DELIMITED / SERDE / WITH SERDEPROPERTIES,
--   STORED AS TEXTFILE / SEQUENCEFILE / RCFILE, LOCATION, TBLPROPERTIES
--   (skip.header.line.count, ignore.malformed.json)
--
-- Partition strategy (per locked Performance Optimization):
--   Hive multi-column partition (client_code STRING, feed_date STRING) →
--   BQ: PARTITION BY feed_date (promoted from STRING to DATE),
--       CLUSTER BY (client_code).
--   client_code stays as a regular STRING column appended after feed_date.
--   OPTIONS(partition_expiration_days=365) on all 10 tables.
--
-- Column descriptions:
--   All source COMMENTs carried verbatim via OPTIONS(description=...).
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS staging.stg_file_interaction_export (
  interaction_ref             STRING,
  channel                     STRING,
  client_interaction_id       STRING,
  agent_email                 STRING,
  start_ms                    INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  end_ms                      INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  outcome                     STRING,
  customer_ref                STRING,
  -- former partition columns appended at end; feed_date promoted to DATE
  feed_date                   DATE,
  client_code                 STRING
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(
  partition_expiration_days=365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_survey_csat (
  survey_id                   STRING,
  interaction_ref             STRING,
  survey_ms                   INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  csat_score                  INT64,
  nps_score                   INT64,
  fcr_claimed                 BOOL,
  verbatim                    STRING,
  feed_date                   DATE,
  client_code                 STRING
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(
  partition_expiration_days=365
);

-- Complex type: sections ARRAY<STRUCT<...>> (REPEATED RECORD with 3 sub-fields)
CREATE TABLE IF NOT EXISTS staging.stg_file_qa_forms (
  qa_form_id                  STRING,
  interaction_ref             STRING,
  evaluator_email             STRING,
  evaluated_ms                INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  form_version                STRING,
  sections                    ARRAY<STRUCT<section_code STRING, max_points INT64, scored_points INT64>>,
  auto_fail                   BOOL,
  overall_pct                 NUMERIC(5,2),
  feed_date                   DATE,
  client_code                 STRING
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(
  partition_expiration_days=365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_ivr_logs (
  event_ms                    INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  session_ref                 STRING,
  menu_path                   STRING,
  key_pressed                 STRING,
  raw_tail                    STRING,
  feed_date                   DATE,
  client_code                 STRING
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(
  partition_expiration_days=365
);

-- Complex types:
--   messages ARRAY<STRUCT<...>> (REPEATED RECORD with 3 sub-fields)
--   metadata MAP<STRING,STRING> → JSON
CREATE TABLE IF NOT EXISTS staging.stg_file_chat_transcripts (
  chat_ref                    STRING,
  queue_code                  STRING,
  agent_email                 STRING,
  started_ms                  INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  ended_ms                    INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  messages                    ARRAY<STRUCT<sender STRING, ts_ms INT64, text STRING>>,
  metadata                    JSON,
  feed_date                   DATE,
  client_code                 STRING
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(
  partition_expiration_days=365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_roster (
  employee_no                 STRING,
  agent_email                 STRING,
  client_login                STRING,
  role_on_program             STRING,
  active_flag                 BOOL,
  as_of_ms                    INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  feed_date                   DATE,
  client_code                 STRING
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(
  partition_expiration_days=365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_telco_invoice (
  telco_invoice_id            STRING,
  carrier                     STRING,
  circuit_id                  STRING,
  usage_minutes               INT64,
  charge_amount               NUMERIC(12,2),
  bill_period                 STRING,
  billed_ms                   INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  feed_date                   DATE,
  client_code                 STRING
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(
  partition_expiration_days=365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_dialer_result (
  attempt_id                  STRING,
  campaign_code               STRING,
  phone_hash                  STRING,
  agent_id                    INT64,
  attempt_ms                  INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  result_code                 STRING,
  talk_seconds                INT64,
  feed_date                   DATE,
  client_code                 STRING
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(
  partition_expiration_days=365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_email_interaction (
  email_ref                   STRING,
  mailbox                     STRING,
  agent_email                 STRING,
  received_ms                 INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  first_reply_ms              INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  resolved_ms                 INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  subject_category            STRING,
  feed_date                   DATE,
  client_code                 STRING
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(
  partition_expiration_days=365
);

-- Complex type: keywords ARRAY<STRING> (REPEATED STRING)
CREATE TABLE IF NOT EXISTS staging.stg_file_speech_analytics (
  recording_id                STRING,
  call_ref                    STRING,
  analyzed_ms                 INT64 OPTIONS(description='epoch MILLISECONDS (legacy)'),
  sentiment_score             FLOAT64,
  silence_pct                 FLOAT64,
  talk_over_count             INT64,
  keywords                    ARRAY<STRING>,
  feed_date                   DATE,
  client_code                 STRING
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS(
  partition_expiration_days=365
);
