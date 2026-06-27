# Coverage matrix

Tracks which validation patterns (SPEC §5) have a golden `lib/` module + a green
golden test + a negative twin, proven against the live fixture migration.

Legend: ✅ green on live infra · 🚧 in progress · ⬜ not started

## P0 spine (SPEC §13)

| Order | Item | Patterns | lib module | Golden spec | Negative twin | Status |
|---|---|---|---|---|---|---|
| P0-1 | Schema/DDL conformance | 7 | `lib/schema.py` | `tests/schema/schema_conformance.mvs.yaml` | ✅ | ✅ |
| P0-2 | Bulk load + row-count parity | 14, 1 | `lib/parity.py`, `lib/bqload.py` | `tests/parity/rowcount_parity.mvs.yaml` | ✅ | ✅ |
| P0-3 | Encoding: epoch + DECIMAL | 5, 6 | `lib/epoch.py` | `tests/encoding/encoding.mvs.yaml` | ✅ | ✅ |
| P0-4 | Aggregate + fingerprint parity | 2, 3 | `lib/parity.py` | `tests/parity/value_parity.mvs.yaml` | ✅ | ✅ |
| P0-5 | SCD-2 + MERGE + FK orphan | 8, 9, 10 | `lib/scd2.py`, `lib/merge.py`, `lib/fk.py` | `tests/transform/transform.mvs.yaml` | ✅ | ✅ |
| P0-6 | Query/view parity | 4 | `lib/parity.py` | `tests/query/query_parity.mvs.yaml` | ✅ | ✅ |
| P0-7 | Egress parity | 16 | `lib/egress.py` | `tests/egress/egress.mvs.yaml` | ✅ | ✅ |
| P0-8 | End-to-end reconcile + sign-off | 1–10 | `lib/reconcile.py` | `tests/reconcile/test_signoff.py` | ✅ | ✅ |

## Flow group → required patterns (SPEC §6)

CI asserts every pattern has ≥1 green golden test, and every flow group's required
patterns are present. (Wired up in P0-8 / `ci/run-meta-validation.sh`.)

| Flow group | Required patterns | Covered |
|---|---|---|
| Target Schema | 5, 7 | 5, 7 |
| Transform | 1, 2, 3, 5, 8, 9, 10 | 1,2,3,5,8,9,10 |
| Extract-Load | 6, 12, 13, 14, 15 | 14 |
| Egress | 15, 16 | 16 |
| Consumer Migration | 4 | 4 |
| Historical Backfill | 1, 2, 3, 5, 8, 10 | 1,2,3,5,8,10 |

(High/Low-tier patterns — 11, 12, 13, 15, 17–21 — are deferred per SPEC §13.)

## Beyond the P0 spec (added this iteration)

| Capability | Module | Test | Status |
|---|---|---|---|
| BigQuery→BigQuery in-warehouse transform validation | `lib/harness.py` (engine-agnostic source) | `tests/bq_to_bq/` | ✅ |
| TABLE_OPTIONS (retention / require-filter / labels) | `lib/schema.py` | `tests/schema/` | ✅ |
| Scale: in-warehouse SQL-pushdown parity (no egress), cross-engine | `lib/parity.py` (`scale: pushdown`) | `tests/parity/`, `tests/bq_to_bq/` | ✅ |
| Scale: smart-diff localization (segmented digest → differing keys) | `lib/parity.py` (`_localize_pushdown`) | `tests/parity/value_parity_negative` | ✅ |
| Query performance — measure / assert / compare (A-vs-B) / regression modes (BigQuery Job-API + Impala runtime profile) | `lib/perf.py` (`query_performance`) | `tests/perf/` | ✅ |
| Synthetic large-data generator (in-BQ GENERATE_ARRAY, scale-tiered, auto-expiry) | `lib/synth.py` | `tests/perf/test_synth.py` | ✅ |
| Composer/Airflow DAG validation (real env via `gcloud composer run`) | `lib/dag.py` (`dag_structure`) + `ComposerEngine` | `tests/orchestration/test_dag.py` + live | ✅ |
| Read-only / no-seed mode (safe against real envs; blocks mutating patterns) | `lib/harness.py` (`read_only`) + registry `mutates` | `tests/test_readonly.py`, `examples/assessment_readonly.mvs.yaml` | ✅ |
| Mode-2 build-and-verify (apply CUT artifacts to a clean build dataset, then verify; guarded reset+teardown) | `lib/build.py` + `lib/harness.py` (`migration`) | `tests/build/`, `tests/test_build_guard.py` | ✅ |
| ELT transform unit tier (`given`→T→`expect`, hermetic BQ, canonicalized rows + property asserts) | `lib/transform_unit.py` | `tests/transform_unit/` (golden + negative) | ✅ |
| Cross-engine declarative seeder (`given` → tables in BOTH source HS2 + dest BQ; no Python loader) | `lib/build.py` (`seed_given`) | exercised by `tests/transform_diff/` | ✅ |
| Transform equivalence — `transform_diff` (same `given` → legacy T on Impala vs migrated T on BQ; legacy = oracle) | `lib/transform_diff.py` | `tests/transform_diff/` (golden + negative) | ✅ |

## BigQuery Physical Schema DDL — Acceptance Criteria Coverage

Full DDL validation for 115 objects (98 tables + 2 MVs + 15 views) across 3 datasets
(staging, ods, dm). Run via `ci/run_ddl_validation.sh`.

### AC → Test Mapping

| AC | What It Verifies | Test Module | Key Checks | Count |
|---|---|---|---|---|
| **AC1** | 0 CREATE errors | `test_ddl_full_e2e::TestAC1_DDLApplication` | Apply 10 DDL files, split on `;`, per-object error attribution | ≥118 stmts |
| **AC2** | 916 columns match source | `test_ddl_full_e2e::TestAC2_Columns` | Column presence/count, descriptions (70: 68 epoch + 2 lie), 4 complex columns (sections, messages, metadata, keywords) with recursive sub-field verification via COLUMN_FIELD_PATHS, DECIMAL precision/scale | 916 cols |
| **AC3** | Object types correct | `test_ddl_full_e2e::TestAC3_ObjectTypes` + `test_object_types` | 98 BASE TABLE + 2 MV + 15 VIEW; no silent type flips; 2 intentional MV replacements | 115 objects |
| **AC4** | Partition/cluster/options | `test_ddl_full_e2e::TestAC4_PartitionClusterOptions` | 12 clustered tables (exact column lists), 10 RANGE_BUCKET + 4 DATE_TRUNC partitions, 3 require_partition_filter, 10 file-feed expiration (365d), 4 ACID tables native | ~40 checks |
| **AC5** | FK type consistency | `test_ddl_full_e2e::TestAC5_FKConsistency` + `test_fk_consistency` | 56 FK→PK paths all INT64↔INT64; 4 surrogate-key chains; self-FK; cross-dataset paths | 56 paths |
| **AC6** | SELECT * succeeds | `test_ddl_full_e2e::TestAC6_Queryability` + `test_queryability` | SELECT * LIMIT 0 on 115 objects; 3 tier-specific join queries | 118 queries |
| **AC7** | Catalog read-back | `test_ddl_full_e2e::TestAC7_CatalogPresence` + `test_catalog_presence` | INFORMATION_SCHEMA.TABLES all 115 objects present + correct type | 115 objects |
| **AC8** | Live execution proof | *meta-constraint* | Every check queries live BQ INFORMATION_SCHEMA — no offline parse, dry-run, or DDL-vs-DDL comparison | all |
| **AC9** | Scan reduction | `test_scan_reduction::TestScanReduction` | 10 cluster-filtered vs unfiltered (Job API bytes_processed); 3 partition-filter rejections; 3 partition pruning | 16 benchmarks |

### Source Ground Truth

All comparisons use:
- **Source side**: `manifests/tables.yaml` + `hive/ddl/*.hql` + `docs/EPOCH-POLICY.md`
- **Target side**: Live BQ `INFORMATION_SCHEMA.COLUMNS`, `INFORMATION_SCHEMA.TABLES`,
  `INFORMATION_SCHEMA.TABLE_OPTIONS`, `INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`

No offline parse, dry-run, or DDL-vs-DDL comparison counts as a pass (AC8).

### Execution

```
# AC1–AC8 (no data needed):
ci/run_ddl_validation.sh

# AC1–AC9 (seeds synthetic data for scan benchmarks):
ci/run_ddl_validation.sh --with-perf
```

### DDL File Inventory

| File | Objects | Count |
|---|---|---|
| `sql/ddl/01-create-datasets.sql` | CREATE SCHEMA for staging, ods, dm | 3 |
| `sql/ddl/02-staging-sqoop-mirrors.sql` | 27 Sqoop mirror tables | 27 |
| `sql/ddl/03-staging-delta-feeds.sql` | 8 delta CDC tables | 8 |
| `sql/ddl/04-staging-file-feeds.sql` | 10 file-feed tables (partitioned + clustered + expiring) | 10 |
| `sql/ddl/05-ods-cleanse.sql` | 15 ODS cleanse tables | 15 |
| `sql/ddl/06-ods-delta-scd2.sql` | 8 delta-merge + 3 SCD-2 tables | 11 |
| `sql/ddl/07-ods-acid.sql` | 4 former ACID tables (2 clustered) | 4 |
| `sql/ddl/08-dm-tables.sql` | 9 dims + 9 facts + 5 aggs (partitioned + clustered) | 23 |
| `sql/ddl/08b-dm-materialized-views.sql` | 2 materialized views | 2 |
| `sql/ddl/09-dm-views.sql` | 15 analyst-facing views (Hive→BQ dialect translated) | 15 |
| **Total** | | **118 stmts** |

