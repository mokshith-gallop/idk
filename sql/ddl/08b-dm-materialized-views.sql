-- ----------------------------------------------------------------------------
-- 08b-dm-materialized-views.sql  — dm: materialized views (2)
-- Source: NBCS CDH 6.3.4 legacy warehouse → BigQuery
--
-- Per locked Performance Optimization decision:
--   dm.agg_agent_weekly (TABLE) → dm.mv_agent_weekly (MATERIALIZED VIEW)
--   dm.agg_site_daily   (TABLE) → dm.mv_site_daily   (MATERIALIZED VIEW)
--
-- Both MVs are simple aggregate rollups over dm.agg_agent_daily (the base
-- table, created in 08-dm-tables.sql). Must be applied AFTER 08.
--
-- Limitation: BigQuery materialized views do NOT support joins. The source
-- agg_site_daily included sl_pct derived from fact_queue_interval via a
-- join. The MV computes sl_pct as CAST(NULL AS NUMERIC) instead;
-- consumers requiring sl_pct should use the full vw_queue_sla_attainment
-- view or query fact_queue_interval directly.
-- ----------------------------------------------------------------------------

-- mv_agent_weekly: weekly rollup from agg_agent_daily.
-- Groups by ISO week boundary derived from date_key.
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

-- mv_site_daily: site-level rollup from agg_agent_daily.
-- sl_pct is CAST(NULL AS NUMERIC) because BQ MVs do not support joins
-- (the source agg_site_daily joined fact_queue_interval for this column).
CREATE MATERIALIZED VIEW IF NOT EXISTS dm.mv_site_daily AS
SELECT
  date_key,
  site_code,
  COUNT(DISTINCT agent_sk)                           AS agents_active,
  SUM(interactions_handled)                          AS interactions,
  CAST(AVG(avg_handle_seconds) AS NUMERIC)           AS avg_handle_seconds,
  CAST(NULL AS NUMERIC)                              AS sl_pct,
  CAST(AVG(adherence_pct) AS NUMERIC)                AS adherence_pct
FROM dm.agg_agent_daily
GROUP BY date_key, site_code;
