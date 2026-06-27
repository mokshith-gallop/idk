-- ----------------------------------------------------------------------------
-- 04-staging-file-feeds: staging: SFTP/file-landed client feeds (10)
-- Translated from: hive/ddl/04-staging-file-feeds.hql
-- Hive constructs dropped: EXTERNAL, ROW FORMAT, SERDE, WITH SERDEPROPERTIES,
--   STORED AS, LOCATION, TBLPROPERTIES.
-- Multi-column Hive partition (client_code STRING, feed_date STRING) →
--   BQ: PARTITION BY feed_date (DATE), CLUSTER BY client_code.
-- All 10 file-feed tables get OPTIONS(partition_expiration_days=365).
-- Complex types: ARRAY<STRUCT<...>>, MAP<STRING,STRING>→JSON,
--   ARRAY<STRING>→REPEATED STRING.
-- Type map: BIGINT→INT64, INT→INT64, STRING→STRING, BOOLEAN→BOOL,
--   DOUBLE→FLOAT64, DECIMAL(p,s)→NUMERIC(p,s).
-- All source COMMENTs carried as OPTIONS(description=...).
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS staging.stg_file_interaction_export (
  interaction_ref             STRING,
  channel                     STRING,
  client_interaction_id       STRING,
  agent_email                 STRING,
  start_ms                    INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  end_ms                      INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  outcome                     STRING,
  customer_ref                STRING,
  client_code                 STRING,
  feed_date                   DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS (
  partition_expiration_days = 365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_survey_csat (
  survey_id                   STRING,
  interaction_ref             STRING,
  survey_ms                   INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  csat_score                  INT64,
  nps_score                   INT64,
  fcr_claimed                 BOOL,
  verbatim                    STRING,
  client_code                 STRING,
  feed_date                   DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS (
  partition_expiration_days = 365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_qa_forms (
  qa_form_id                  STRING,
  interaction_ref             STRING,
  evaluator_email             STRING,
  evaluated_ms                INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  form_version                STRING,
  sections                    ARRAY<STRUCT<section_code STRING, max_points INT64, scored_points INT64>>,
  auto_fail                   BOOL,
  overall_pct                 NUMERIC(5, 2),
  client_code                 STRING,
  feed_date                   DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS (
  partition_expiration_days = 365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_ivr_logs (
  event_ms                    INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  session_ref                 STRING,
  menu_path                   STRING,
  key_pressed                 STRING,
  raw_tail                    STRING,
  client_code                 STRING,
  feed_date                   DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS (
  partition_expiration_days = 365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_chat_transcripts (
  chat_ref                    STRING,
  queue_code                  STRING,
  agent_email                 STRING,
  started_ms                  INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  ended_ms                    INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  messages                    ARRAY<STRUCT<sender STRING, ts_ms INT64, text STRING>>,
  metadata                    JSON,
  client_code                 STRING,
  feed_date                   DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS (
  partition_expiration_days = 365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_roster (
  employee_no                 STRING,
  agent_email                 STRING,
  client_login                STRING,
  role_on_program             STRING,
  active_flag                 BOOL,
  as_of_ms                    INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  client_code                 STRING,
  feed_date                   DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS (
  partition_expiration_days = 365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_telco_invoice (
  telco_invoice_id            STRING,
  carrier                     STRING,
  circuit_id                  STRING,
  usage_minutes               INT64,
  charge_amount               NUMERIC(12, 2),
  bill_period                 STRING,
  billed_ms                   INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  client_code                 STRING,
  feed_date                   DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS (
  partition_expiration_days = 365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_dialer_result (
  attempt_id                  STRING,
  campaign_code               STRING,
  phone_hash                  STRING,
  agent_id                    INT64,
  attempt_ms                  INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  result_code                 STRING,
  talk_seconds                INT64,
  client_code                 STRING,
  feed_date                   DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS (
  partition_expiration_days = 365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_email_interaction (
  email_ref                   STRING,
  mailbox                     STRING,
  agent_email                 STRING,
  received_ms                 INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  first_reply_ms              INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  resolved_ms                 INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  subject_category            STRING,
  client_code                 STRING,
  feed_date                   DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS (
  partition_expiration_days = 365
);

CREATE TABLE IF NOT EXISTS staging.stg_file_speech_analytics (
  recording_id                STRING,
  call_ref                    STRING,
  analyzed_ms                 INT64 OPTIONS (description = 'epoch MILLISECONDS (legacy)'),
  sentiment_score             FLOAT64,
  silence_pct                 FLOAT64,
  talk_over_count             INT64,
  keywords                    ARRAY<STRING>,
  client_code                 STRING,
  feed_date                   DATE
)
PARTITION BY feed_date
CLUSTER BY client_code
OPTIONS (
  partition_expiration_days = 365
);
