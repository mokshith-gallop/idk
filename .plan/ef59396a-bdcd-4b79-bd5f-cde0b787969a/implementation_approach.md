# Implementation Approach

## BigQuery Physical Schema DDL — Implementation Approach

### DDL File Organization (Mirror Hive Layout)

10 SQL files under `sql/ddl/`, numbered to match the source Hive layout for easy diffing:

| File | Contents | Objects |
|---|---|---|
| `01-create-datasets.sql` | `CREATE SCHEMA IF NOT EXISTS` for staging, ods, dm (with descriptions) | 3 datasets |
| `02-staging-sqoop-mirrors.sql` | 27 Sqoop mirror tables (Datastream-replicated) | 27 tables |
| `03-staging-delta-feeds.sql` | 8 delta CDC staging tables | 8 tables |
| `04-staging-file-feeds.sql` | 10 file-feed staging tables (partitioned + clustered + expiring) | 10 tables |
| `05-ods-cleanse.sql` | 15 ODS cleanse tables | 15 tables |
| `06-ods-delta-scd2.sql` | 8 delta-merge + 3 SCD-2 history tables | 11 tables |
| `07-ods-acid.sql` | 4 former ACID tables as native BQ tables (2 with clustering) | 4 tables |
| `08-dm-tables.sql` | 9 dims + 9 facts + 5 physical aggs | 23 tables |
| `08b-dm-materialized-views.sql` | 2 materialized views (must run AFTER 08) | 2 MVs |
| `09-dm-views.sql` | 15 analyst-facing views (BQ dialect) | 15 views |
| **Total** | | **115 + 3 datasets** |

File `08b` is separated because MVs reference base tables from `08` and must be applied after them. The harness applies files in lexicographic order.

### Hive Constructs Dropped (no BQ equivalent)

Every source DDL artifact below is eliminated — BQ native tables need none of them:

| Hive Construct | Action |
|---|---|
| `EXTERNAL TABLE` | Drop — all BQ tables are native managed |
| `STORED AS PARQUET / ORC / TEXTFILE / SEQUENCEFILE / RCFILE` | Drop — BQ manages storage format |
| `LOCATION 'hdfs://...'` | Drop — no external location |
| `TBLPROPERTIES ('parquet.compression'='SNAPPY')` | Drop — BQ auto-compresses |
| `TBLPROPERTIES ('transactional'='true')` | Drop — BQ DML is always transactional |
| `ROW FORMAT SERDE / DELIMITED / WITH SERDEPROPERTIES` | Drop — parsing happens at load time, not DDL |
| `CLUSTERED BY ... INTO N BUCKETS` | Replaced by `CLUSTER BY` (no bucket count) |

### Clustering Strategy (12 Tables)

Per locked Performance Optimization + Schema bucketing translation:

| Table | Cluster Columns | Source |
|---|---|---|
| `fact_interaction` | `agent_sk, channel, client_sk, customer_ref` | Perf Opt (3 cols) + query optimization (+customer_ref) |
| `fact_agent_activity` | `agent_sk, state_code` | Perf Opt |
| `fact_queue_interval` | `queue_sk` | Perf Opt |
| `fact_csat_survey` | `program_sk, agent_sk` | Perf Opt |
| `fact_billing_line` | `client_sk, program_sk` | Perf Opt |
| `fact_adherence_daily` | `agent_sk` | Perf Opt |
| `fact_ticket` | `program_sk, assigned_agent_sk` | Perf Opt |
| `agg_agent_daily` | `agent_sk, site_code` | Perf Opt |
| `agg_queue_hourly` | `queue_sk` | Perf Opt |
| `ods_interaction` | `agent_id, client_code` | Perf Opt |
| `ods_agent_acid` | `agent_id` | Schema bucketing translation |
| `ods_ticket_acid` | `ticket_id` | Schema bucketing translation |

`ods_client_acid` and `ods_invoice_acid` — no clustering (tiny tables: 10 and 290 rows).

### TABLE OPTIONS

| Option | Tables | Value |
|---|---|---|
| `require_partition_filter = true` | `fact_interaction`, `fact_agent_activity`, `fact_queue_interval` | Prevents accidental full scans |
| `partition_expiration_days = 365` | All 10 `stg_file_*` tables | Replaces housekeeping script 86 |

### Column Descriptions (70 total)

All 68 epoch-annotation COMMENTs from source Hive DDL carried verbatim to BQ column `OPTIONS(description=...)`:
- `epoch SECONDS (legacy)` — telephony, WFM, HR, CRM columns
- `epoch MILLISECONDS (legacy)` — ticketing, file feed, CDC `change_ms` columns
- `Oracle string YYYYMMDDHH24MISS (legacy)` — CRM contract date strings

Plus 2 lie-column descriptions (per EPOCH-POLICY.md):
- `stg_fin_invoice.issued_ts_sec` → `"!! name says seconds, VALUES ARE MILLIS !!"`
- `stg_fin_invoice.due_ts_sec` → `"!! name says seconds, VALUES ARE MILLIS !!"`

### Materialized View Definitions

**`dm.mv_agent_weekly`** (replaces `agg_agent_weekly`, base: `agg_agent_daily`):

Weekly rollup grouping by ISO week boundary derived from `date_key`:

```sql
CREATE MATERIALIZED VIEW dm.mv_agent_weekly AS
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
```

**`dm.mv_site_daily`** (replaces `agg_site_daily`, base: `agg_agent_daily`):

Site-level rollup aggregating agent metrics. `sl_pct` requires `fact_queue_interval` — BQ MVs support joins, so the MV LEFT JOINs a queue-interval subaggregate. If BQ rejects the multi-fact join at CREATE time, fall back to computing `sl_pct` via a separate subquery or as NULL and document the limitation.

```sql
CREATE MATERIALIZED VIEW dm.mv_site_daily AS
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
```

### View Dialect Translation (15 Views)

All 15 views translated per locked SQL Dialect Translation rules. Key per-view translations:

| View | Translation Applied |
|---|---|
| `vw_org_hierarchy` | `WITH RECURSIVE` — BQ-native, no change needed |
| `vw_active_agents_ndv` | `NDV()` → `APPROX_COUNT_DISTINCT()` |
| `vw_csat_rollup` | `GROUP BY ... WITH ROLLUP` → `GROUP BY ROLLUP(...)`; `GROUPING__ID` → `GROUPING(client_id, program_code)` with bit-order reversal |
| `vw_call_driver_regex` | `RLIKE` → `REGEXP_CONTAINS()`; `regexp_extract(s, p, 1)` → `REGEXP_EXTRACT(s, p)` |
| `vw_repeat_contact_window` | `unix_timestamp(ts)` → `UNIX_SECONDS(ts)` |
| `vw_billing_reconciliation` | `from_unixtime(CAST(x/1000 AS BIGINT))` → `TIMESTAMP_MILLIS(x)`; `unix_timestamp()` → `UNIX_SECONDS()` |
| `vw_first_contact_resolution` | `date_add(f.end_ts, 7)` → `TIMESTAMP_ADD(f.end_ts, INTERVAL 7 DAY)` |
| `vw_shrinkage_analysis` | `from_unixtime(unix_timestamp(CAST(...),'yyyyMMdd'),'yyyy-MM-dd')` → `FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', CAST(f.date_key AS STRING)))` |
| `vw_queue_sla_attainment` | Layer-skip read of `staging.stg_crm_sla_target` — requires authorized view on `staging` dataset |
| `vw_billing_reconciliation` | Layer-skip read of `staging.stg_fin_invoice` — requires authorized view on `staging` dataset |
| All other views | Standard translations (window functions, CTEs, JOINs all BQ-compatible) |

### Harness Integration (Mode-2 Build-and-Verify)

The DDL files are supplied as `migration.steps` of `kind: ddl` in the MVS YAML. The harness:
1. Creates an ephemeral build dataset (or uses `isolate: true` for parallel runs)
2. Applies each DDL file in order (01→09) via `BigQuery.query()` — verbatim, no normalization
3. Reads back from `INFORMATION_SCHEMA` to verify against source ground truth (`manifests/tables.yaml` + `hive/ddl/*.hql`)
4. Tears down the ephemeral dataset

Per-object error attribution: each `CREATE` statement is a separate SQL statement within the file. The harness splits on `;` and executes individually, catching errors per-object. Any failure reports the object name + BQ error message (AC#1 HARD FAIL requirement).
