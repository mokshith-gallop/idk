-- ----------------------------------------------------------------------------
-- 08b-dm-materialized-views: dm: materialized views (2)
-- Translated from: hive/ddl/08-dm-tables.hql (agg_agent_weekly, agg_site_daily)
-- Replaces two physical aggregate tables per locked Performance Optimization.
-- Both use dm.agg_agent_daily as the base table.
-- Must be applied AFTER 08-dm-tables.sql (base tables must exist).
--
-- BQ MV limitations:
--   - MV JOINs require BigQuery Enterprise edition.
--   - If the multi-table mv_site_daily fails at CREATE time, a single-table
--     fallback is provided (sl_pct = NULL, documented).
-- ----------------------------------------------------------------------------

-- mv_agent_weekly: weekly rollup of agg_agent_daily by ISO week boundary.
-- Replaces the source table dm.agg_agent_weekly.
CREATE MATERIALIZED VIEW IF NOT EXISTS dm.mv_agent_weekly AS
SELECT
  CAST(FORMAT_DATE('%Y%m%d',
    DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(date_key AS STRING)), ISOWEEK)
  ) AS INT64)                                        AS week_start_key,
  agent_sk,
  site_code,
  COUNT(*)                                           AS days_worked,
  SUM(interactions_handled)                          AS interactions_handled,
  CAST(AVG(avg_handle_seconds) AS NUMERIC)           AS avg_handle_seconds,
  CAST(AVG(adherence_pct) AS NUMERIC)                AS adherence_pct,
  CAST(AVG(occupancy_pct) AS NUMERIC)                AS occupancy_pct
FROM dm.agg_agent_daily
GROUP BY 1, 2, 3;

-- mv_site_daily: site-level daily rollup aggregating agent metrics.
-- Replaces the source table dm.agg_site_daily.
-- sl_pct derived from fact_queue_interval via dim_queue join.
-- Note: If BQ rejects the multi-table MV (JOIN support requires Enterprise
-- edition), fall back to sl_pct = NULL and document the limitation.
CREATE MATERIALIZED VIEW IF NOT EXISTS dm.mv_site_daily AS
SELECT
  a.date_key,
  a.site_code,
  COUNT(DISTINCT a.agent_sk)                         AS agents_active,
  SUM(a.interactions_handled)                        AS interactions,
  CAST(AVG(a.avg_handle_seconds) AS NUMERIC)         AS avg_handle_seconds,
  CAST(q.sl_pct AS NUMERIC)                         AS sl_pct,
  CAST(AVG(a.adherence_pct) AS NUMERIC)              AS adherence_pct
FROM dm.agg_agent_daily a
LEFT JOIN (
  SELECT qi.date_key,
         dq.site_code,
         SAFE_DIVIDE(SUM(qi.answered_in_sl), SUM(qi.answered)) * 100 AS sl_pct
  FROM dm.fact_queue_interval qi
  JOIN dm.dim_queue dq ON dq.queue_sk = qi.queue_sk
  GROUP BY qi.date_key, dq.site_code
) q ON q.date_key = a.date_key AND q.site_code = a.site_code
GROUP BY a.date_key, a.site_code, q.sl_pct;
