-- ----------------------------------------------------------------------------
-- 09-dm-views.sql  — dm: 15 analyst-facing views (BigQuery SQL dialect)
-- Migrated from: hive/ddl/09-dm-views.hql (HAND-WRITTEN)
-- Source: NBCS CDH 6.3.4 legacy warehouse → BigQuery
--
-- Dialect translations applied:
--   NDV()                  → APPROX_COUNT_DISTINCT()
--   RLIKE                  → REGEXP_CONTAINS()
--   regexp_extract(s,p,1)  → REGEXP_EXTRACT(s,p)
--   GROUPING__ID           → bit-encoded from GROUPING() calls
--   GROUP BY ... WITH ROLLUP → GROUP BY ROLLUP(...)
--   unix_timestamp(ts)     → UNIX_SECONDS(ts)
--   from_unixtime(x)       → TIMESTAMP_SECONDS(x)
--   date_add(ts, n)        → TIMESTAMP_ADD(ts, INTERVAL n DAY)
--   from_unixtime(unix_timestamp(CAST(...), fmt), fmt2)
--                          → FORMAT_DATE / PARSE_DATE
--   LIKE 'AUX%'            → STARTS_WITH (or LIKE — both work in BQ)
--
-- Cross-dataset references:
--   Several views read staging.* and ods.* tables (layer-skip reads).
--   These are preserved as-is with qualified dataset names.
--
-- MV references:
--   vw_attrition_risk references dm.mv_agent_weekly (the MV that replaces
--   the former agg_agent_weekly table).
-- ----------------------------------------------------------------------------

-- 1. Org hierarchy (recursive CTE — BQ-native, no change needed)
CREATE VIEW IF NOT EXISTS dm.vw_org_hierarchy AS
WITH RECURSIVE org_tree AS (
  SELECT o.org_unit_id, o.unit_code, o.unit_name, o.unit_type,
         o.site_code, o.org_unit_id AS root_unit_id, 0 AS depth,
         CAST(o.unit_name AS STRING) AS path_names
  FROM   ods.ods_org_unit o
  WHERE  o.parent_unit_id IS NULL
  UNION ALL
  SELECT c.org_unit_id, c.unit_code, c.unit_name, c.unit_type,
         c.site_code, p.root_unit_id, p.depth + 1,
         CONCAT(p.path_names, ' > ', c.unit_name)
  FROM   ods.ods_org_unit c
  JOIN   org_tree p ON c.parent_unit_id = p.org_unit_id
  WHERE  p.depth < 6
)
SELECT org_unit_id, unit_code, unit_name, unit_type, site_code,
       root_unit_id, depth, path_names
FROM   org_tree;

-- 2. Active-agent panel (NDV() → APPROX_COUNT_DISTINCT())
CREATE VIEW IF NOT EXISTS dm.vw_active_agents_ndv AS
SELECT f.date_key,
       a.site_code,
       APPROX_COUNT_DISTINCT(f.agent_sk)                AS approx_active_agents,
       APPROX_COUNT_DISTINCT(CONCAT(CAST(f.agent_sk AS STRING), '|', f.channel)) AS approx_agent_channel_pairs,
       COUNT(*)                                          AS interactions
FROM   dm.fact_interaction f
JOIN   dm.dim_agent a ON a.agent_sk = f.agent_sk
GROUP  BY f.date_key, a.site_code;

-- 3. CSAT rollup
--    GROUP BY ... WITH ROLLUP → GROUP BY ROLLUP(...)
--    GROUPING__ID → bit-encoded from GROUPING() calls
--    Hive GROUPING__ID bit order: rightmost col = LSB.
--    BQ GROUPING() returns 1 when the column is aggregated (rolled up).
--    Reconstruct the same integer: GROUPING(program_code)*1 + GROUPING(client_id)*2
CREATE VIEW IF NOT EXISTS dm.vw_csat_rollup AS
SELECT p.client_id,
       p.program_code,
       COUNT(*)                       AS surveys,
       AVG(s.csat_score)              AS avg_csat,
       SUM(CASE WHEN s.nps_score >= 9 THEN 1 ELSE 0 END) / COUNT(*) * 100 AS pct_promoters,
       GROUPING(p.program_code) + GROUPING(p.client_id) * 2 AS grouping_level
FROM   dm.fact_csat_survey s
JOIN   dm.dim_program p ON p.program_sk = s.program_sk
GROUP  BY ROLLUP(p.client_id, p.program_code);

-- 4. Call-driver classification
--    RLIKE → REGEXP_CONTAINS()
--    regexp_extract(s, p, 1) → REGEXP_EXTRACT(s, p)
--    (BQ REGEXP_EXTRACT returns the first capturing group by default)
CREATE VIEW IF NOT EXISTS dm.vw_call_driver_regex AS
SELECT c.call_date,
       c.queue_id,
       CASE
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(bill|invoice|charge|refund)')    THEN 'BILLING'
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(password|login|locked|reset)')   THEN 'ACCESS'
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(cancel|churn|retention)')        THEN 'RETENTION'
         WHEN REGEXP_EXTRACT(d.disposition_desc, r'^\[([A-Z]{2,5})\]') IS NOT NULL
              THEN REGEXP_EXTRACT(d.disposition_desc, r'^\[([A-Z]{2,5})\]')
         ELSE 'OTHER'
       END                                              AS call_driver,
       REGEXP_EXTRACT(d.disposition_desc, r'ref#(\d+)') AS embedded_ref_no,
       COUNT(*)                                         AS calls,
       AVG(c.talk_seconds)                              AS avg_talk_seconds
FROM   ods.ods_call c
JOIN   dm.dim_disposition d ON d.disposition_code = c.disposition_code
GROUP  BY c.call_date, c.queue_id,
       CASE
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(bill|invoice|charge|refund)')    THEN 'BILLING'
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(password|login|locked|reset)')   THEN 'ACCESS'
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(cancel|churn|retention)')        THEN 'RETENTION'
         WHEN REGEXP_EXTRACT(d.disposition_desc, r'^\[([A-Z]{2,5})\]') IS NOT NULL
              THEN REGEXP_EXTRACT(d.disposition_desc, r'^\[([A-Z]{2,5})\]')
         ELSE 'OTHER'
       END,
       REGEXP_EXTRACT(d.disposition_desc, r'ref#(\d+)');

-- 5. Repeat contacts within 72h
--    unix_timestamp(ts) → UNIX_SECONDS(ts)
CREATE VIEW IF NOT EXISTS dm.vw_repeat_contact_window AS
SELECT i.interaction_id,
       i.customer_ref,
       i.channel,
       i.start_ts,
       LAG(i.start_ts) OVER (PARTITION BY i.customer_ref ORDER BY i.start_ts) AS prev_contact_ts,
       CASE WHEN UNIX_SECONDS(i.start_ts)
               - UNIX_SECONDS(LAG(i.start_ts) OVER (PARTITION BY i.customer_ref
                                                      ORDER BY i.start_ts)) <= 259200
            THEN 1 ELSE 0 END                            AS repeat_within_72h
FROM   ods.ods_interaction i
WHERE  i.customer_ref IS NOT NULL AND i.customer_ref <> '';

-- 6. Billing reconciliation (layer-skip: reads staging.stg_fin_invoice)
--    from_unixtime(CAST(x/1000 AS BIGINT)) → TIMESTAMP_MILLIS(x)
--    unix_timestamp(ts) → UNIX_SECONDS(ts)
CREATE VIEW IF NOT EXISTS dm.vw_billing_reconciliation AS
SELECT s.invoice_no,
       s.total_amount                                    AS staged_amount,
       a.total_amount                                    AS ods_amount,
       TIMESTAMP_MILLIS(s.issued_ts_sec)                 AS staged_issued_ts,
       a.issued_ts                                       AS ods_issued_ts,
       (UNIX_SECONDS(a.issued_ts) - CAST(s.issued_ts_sec / 1000 AS INT64)) AS drift_seconds,
       CASE WHEN ABS(s.total_amount - a.total_amount) > 0.01 THEN 'AMOUNT_MISMATCH'
            WHEN UNIX_SECONDS(a.issued_ts) <> CAST(s.issued_ts_sec / 1000 AS INT64) THEN 'TS_MISMATCH'
            ELSE 'OK' END                                AS recon_status
FROM   staging.stg_fin_invoice s
JOIN   ods.ods_invoice_acid a ON a.invoice_id = s.invoice_id;

-- 7. Current agent roster (latest SCD-2 slice via ROW_NUMBER)
CREATE VIEW IF NOT EXISTS dm.vw_agent_roster_current AS
SELECT latest.agent_id, latest.employee_no, latest.org_unit_id, latest.job_grade,
       latest.employment_type, latest.status, latest.eff_from_ts,
       asg.program_id AS current_program_id,
       asg.queue_id   AS current_queue_id,
       asg.role_on_program
FROM (
  SELECT h.*,
         ROW_NUMBER() OVER (PARTITION BY h.agent_id ORDER BY h.eff_from_ts DESC) AS rn
  FROM   ods.ods_agent_scd2 h
) latest
LEFT   JOIN ods.ods_agent_assignment_scd2 asg
       ON asg.agent_id = latest.agent_id AND asg.is_current = TRUE
WHERE  latest.rn = 1;

-- 8. Agent scorecard (PERCENT_RANK + NTILE — BQ-native, no change needed)
CREATE VIEW IF NOT EXISTS dm.vw_agent_scorecard AS
WITH perf AS (
  SELECT d.agent_sk,
         AVG(d.avg_handle_seconds)  AS aht,
         AVG(d.adherence_pct)       AS adherence,
         SUM(d.interactions_handled) AS volume
  FROM   dm.agg_agent_daily d
  GROUP  BY d.agent_sk
),
qa AS (
  SELECT q.agent_sk, AVG(q.overall_pct) AS avg_qa_pct
  FROM   dm.fact_qa_evaluation q
  GROUP  BY q.agent_sk
),
skills AS (
  SELECT s.agent_id, COUNT(*) AS certified_skills
  FROM   ods.ods_agent_skill_scd2 s
  WHERE  s.is_current = TRUE AND s.certified = TRUE
  GROUP  BY s.agent_id
)
SELECT a.agent_sk, a.full_name, a.site_code, a.job_grade,
       p.aht, p.adherence, p.volume, q.avg_qa_pct,
       COALESCE(sk.certified_skills, 0) AS certified_skills,
       PERCENT_RANK() OVER (ORDER BY p.aht ASC)          AS aht_pctile,
       NTILE(4)       OVER (ORDER BY q.avg_qa_pct DESC)  AS qa_quartile
FROM   dm.dim_agent a
JOIN   perf p ON p.agent_sk = a.agent_sk
LEFT   JOIN qa q ON q.agent_sk = a.agent_sk
LEFT   JOIN skills sk ON sk.agent_id = a.agent_id
WHERE  a.is_current = TRUE;

-- 9. Attrition risk (nested CTEs + NTILE banding)
--    References dm.mv_agent_weekly (MV replacing former agg_agent_weekly)
CREATE VIEW IF NOT EXISTS dm.vw_attrition_risk AS
WITH adh AS (
  SELECT f.agent_sk, AVG(f.adherence_pct) AS adherence_90d
  FROM   dm.fact_adherence_daily f
  GROUP  BY f.agent_sk
),
notice AS (
  SELECT e.agent_id, COUNT(*) AS notice_events
  FROM   ods.ods_attrition_event e
  GROUP  BY e.agent_id
),
wk AS (
  SELECT w.agent_sk, AVG(w.interactions_handled) AS weekly_volume
  FROM   dm.mv_agent_weekly w
  GROUP  BY w.agent_sk
),
banded AS (
  SELECT a.agent_sk, a.agent_id, a.full_name, a.site_code,
         adh.adherence_90d,
         COALESCE(n.notice_events, 0) AS notice_events,
         NTILE(5) OVER (ORDER BY adh.adherence_90d ASC) AS adherence_band
  FROM   dm.dim_agent a
  JOIN   adh    ON adh.agent_sk = a.agent_sk
  LEFT   JOIN notice n ON n.agent_id = a.agent_id
  LEFT   JOIN wk ON wk.agent_sk = a.agent_sk
  WHERE  a.is_current = TRUE AND a.status = 'ACTIVE'
)
SELECT agent_sk, agent_id, full_name, site_code, adherence_90d, notice_events,
       CASE WHEN adherence_band = 1 OR notice_events > 0 THEN 'HIGH'
            WHEN adherence_band = 2 THEN 'MEDIUM' ELSE 'LOW' END AS attrition_risk
FROM   banded;

-- 10. Queue SLA attainment (layer-skip: reads staging.stg_crm_sla_target)
CREATE VIEW IF NOT EXISTS dm.vw_queue_sla_attainment AS
SELECT q.queue_code,
       q.media_type,
       f.date_key,
       SUM(f.answered_in_sl) / NULLIF(SUM(f.answered), 0) * 100 AS sl_pct,
       MAX(t.target_value)                                       AS sl_target,
       CASE WHEN SUM(f.answered_in_sl) / NULLIF(SUM(f.answered), 0) * 100
                 >= MAX(t.target_value) THEN 'MET' ELSE 'MISSED' END AS attainment
FROM   dm.fact_queue_interval f
JOIN   dm.dim_queue q          ON q.queue_sk = f.queue_sk
LEFT   JOIN staging.stg_crm_sla_target t
       ON t.queue_id = q.queue_id AND t.metric_code = 'SL_20_80'
GROUP  BY q.queue_code, q.media_type, f.date_key;

-- 11. First-contact resolution
--     date_add(f.end_ts, 7) → TIMESTAMP_ADD(f.end_ts, INTERVAL 7 DAY)
CREATE VIEW IF NOT EXISTS dm.vw_first_contact_resolution AS
SELECT f.date_key,
       f.program_sk,
       COUNT(*)                                          AS resolved_interactions,
       SUM(CASE WHEN rpt.interaction_id IS NULL THEN 1 ELSE 0 END) AS fcr_count,
       SUM(CASE WHEN rpt.interaction_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100 AS fcr_pct
FROM   dm.fact_interaction f
LEFT   JOIN dm.fact_interaction rpt
       ON  rpt.customer_ref = f.customer_ref
       AND rpt.start_ts > f.end_ts
       AND rpt.start_ts <= TIMESTAMP_ADD(f.end_ts, INTERVAL 7 DAY)
WHERE  f.resolved_flag = TRUE
GROUP  BY f.date_key, f.program_sk;

-- 12. Occupancy / utilization (pivot of agent-state seconds)
--     LIKE 'AUX%' works in BQ as-is
CREATE VIEW IF NOT EXISTS dm.vw_occupancy_utilization AS
SELECT f.date_key,
       a.site_code,
       f.agent_sk,
       SUM(CASE WHEN f.state_code IN ('TALK','HOLD','ACW') THEN f.state_seconds ELSE 0 END) AS handle_seconds,
       SUM(CASE WHEN f.state_code = 'READY'                THEN f.state_seconds ELSE 0 END) AS ready_seconds,
       SUM(CASE WHEN f.state_code LIKE 'AUX%'              THEN f.state_seconds ELSE 0 END) AS aux_seconds,
       SUM(CASE WHEN f.state_code IN ('TALK','HOLD','ACW') THEN f.state_seconds ELSE 0 END)
         / NULLIF(SUM(CASE WHEN f.state_code IN ('TALK','HOLD','ACW','READY')
                           THEN f.state_seconds ELSE 0 END), 0) * 100 AS occupancy_pct
FROM   dm.fact_agent_activity f
JOIN   dm.dim_agent a ON a.agent_sk = f.agent_sk
GROUP  BY f.date_key, a.site_code, f.agent_sk;

-- 13. Shrinkage analysis
--     from_unixtime(unix_timestamp(CAST(f.date_key AS STRING),'yyyyMMdd'),'yyyy-MM-dd')
--       → FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', CAST(f.date_key AS STRING)))
CREATE VIEW IF NOT EXISTS dm.vw_shrinkage_analysis AS
SELECT f.date_key,
       a.site_code,
       SUM(f.scheduled_minutes)                          AS scheduled_minutes,
       SUM(f.worked_minutes)                             AS worked_minutes,
       SUM(f.exception_minutes + f.timeoff_minutes)      AS shrinkage_minutes,
       SUM(f.exception_minutes + f.timeoff_minutes)
         / NULLIF(SUM(f.scheduled_minutes), 0) * 100     AS shrinkage_pct,
       COUNT(DISTINCT s.schedule_id)                     AS schedules,
       COUNT(DISTINCT sh.shift_sk)                       AS overnight_shifts
FROM   dm.fact_adherence_daily f
JOIN   dm.dim_agent a ON a.agent_sk = f.agent_sk
LEFT   JOIN ods.ods_schedule s
       ON s.agent_id = a.agent_id
      AND s.sched_date = PARSE_DATE('%Y%m%d', CAST(f.date_key AS STRING))
LEFT   JOIN dm.dim_shift sh ON sh.shift_id = s.shift_id AND sh.overnight_flag = TRUE
GROUP  BY f.date_key, a.site_code;

-- 14. Program margin (revenue minus labor cost proxy)
--     No dialect traps — standard SQL CTEs and joins.
--     Cross join wart (ON 1 = 1) preserved from legacy.
CREATE VIEW IF NOT EXISTS dm.vw_program_margin AS
SELECT b.period_month,
       b.client_sk,
       b.program_sk,
       b.billed_amount,
       b.net_revenue,
       lab.billable_cost_minutes / 60.0 * 18.50          AS est_labor_cost,
       adj.total_adjustments,
       b.net_revenue - (lab.billable_cost_minutes / 60.0 * 18.50)
                     - COALESCE(adj.total_adjustments, 0) AS est_margin,
       cmt.committed_min
FROM   dm.agg_billing_monthly b
LEFT   JOIN (
  SELECT t.program_id, t.work_month, SUM(t.billable_minutes) AS billable_cost_minutes
  FROM   ods.ods_timesheet t GROUP BY t.program_id, t.work_month
) lab ON lab.work_month = b.period_month
LEFT   JOIN (
  SELECT p.period_month, SUM(p.amount) AS total_adjustments
  FROM   ods.ods_payroll_adjustment p GROUP BY p.period_month
) adj ON adj.period_month = b.period_month
LEFT   JOIN (
  SELECT cl.contract_id, SUM(cl.min_commit) AS committed_min
  FROM   ods.ods_contract_line cl GROUP BY cl.contract_id
) cmt ON 1 = 1;

-- 15. Client executive summary (HUB view — wide multi-fact join)
CREATE VIEW IF NOT EXISTS dm.vw_client_executive_summary AS
SELECT c.client_code,
       c.client_name,
       pm.period_month,
       pr.program_code,
       pm.interactions,
       pm.avg_handle_seconds,
       pm.avg_csat,
       cs.pct_promoters,
       cs.pct_detractors,
       bm.billed_amount,
       bm.sla_credit_amount,
       bm.net_revenue,
       tk.open_tickets,
       tk.sla_breached_tickets
FROM   dm.dim_client c
JOIN   dm.dim_program pr            ON pr.client_id = c.client_id
JOIN   dm.agg_program_monthly pm    ON pm.program_sk = pr.program_sk AND pm.grouping_level = 0
LEFT   JOIN dm.agg_csat_rollup_monthly cs
       ON cs.program_sk = pr.program_sk AND cs.period_month = pm.period_month
LEFT   JOIN dm.agg_billing_monthly bm
       ON bm.program_sk = pr.program_sk AND bm.period_month = pm.period_month
LEFT   JOIN (
  SELECT t.program_sk,
         SUM(CASE WHEN t.status IN ('OPEN','PENDING') THEN 1 ELSE 0 END) AS open_tickets,
         SUM(CASE WHEN t.sla_breached_flag THEN 1 ELSE 0 END)            AS sla_breached_tickets
  FROM   dm.fact_ticket t GROUP BY t.program_sk
) tk ON tk.program_sk = pr.program_sk;
