# Validation

## BigQuery Physical Schema DDL — Validation Strategy

### Validation Architecture

All 9 ACs require **live BQ catalog verification** — no offline parsing, DDL-vs-DDL comparison, or dry-run counts as a pass. The validation uses the project's Mode-2 build-and-verify harness:

1. **Apply**: Harness executes all 10 DDL files (01→09) against an ephemeral BQ dataset
2. **Verify**: Harness reads back from `INFORMATION_SCHEMA` and compares against source ground truth
3. **Teardown**: Ephemeral dataset destroyed regardless of pass/fail

Source ground truth comes from two authoritative files:
- `manifests/tables.yaml` — column names, types, tags (pk, fk, epoch annotations), partition columns, row counts
- `hive/ddl/*.hql` — COMMENTs, TBLPROPERTIES, partition/bucketing declarations

### AC-to-Pattern Mapping

| AC | What It Verifies | Harness Pattern | Data Source |
|---|---|---|---|
| **AC1** — 0 CREATE errors | All 115 DDL objects apply cleanly | `ddl_apply` — execute each CREATE individually, catch per-object errors | DDL files (01-09) |
| **AC2** — 916 columns match | Column name, type, ordinal, precision, nullability, description | `schema_conformance` — read `INFORMATION_SCHEMA.COLUMNS` + `COLUMN_FIELD_PATHS` | tables.yaml + hive/ddl COMMENTs |
| **AC3** — Object types correct | 98 TABLE + 2 MV + 15 VIEW | `object_type_check` — read `INFORMATION_SCHEMA.TABLES` | tables.yaml |
| **AC4** — Partition/cluster/options | Partition keys, cluster columns, filter requirements, expiration | `partition_conformance` — read `TABLE_OPTIONS` | Performance Opt + Schema decisions |
| **AC5** — FK type consistency | 56 FK→PK pairs same BQ type | `fk_consistency` — cross-join column types from `INFORMATION_SCHEMA.COLUMNS` | tables.yaml fk= annotations |
| **AC6** — SELECT * succeeds | 115 objects queryable + 3 tier queries | `queryability` — `SELECT * LIMIT 0` on each object + parameterized tier queries | DDL objects |
| **AC7** — Catalog read-back | Every object present in catalog | `catalog_presence` — `INFORMATION_SCHEMA.TABLES` + `INFORMATION_SCHEMA.VIEWS` | DDL objects |
| **AC8** — Live execution proof | All checks against live catalog | Meta-constraint: harness runs against real BQ, not parsed DDL | — |
| **AC9** — Scan reduction | Cluster-filtered < unfiltered | `perf_scan` — BQ job statistics comparison with loaded fixture data | datagen/ fixtures |

### Per-AC Verification Details

**AC1 — DDL Application (0 errors)**
- Each file split on `;` into individual statements
- Each statement executed via `BigQuery.query()`
- On failure: report object name (extracted from `CREATE TABLE/VIEW/MATERIALIZED VIEW <name>`) + BQ error message
- Pass: 115/115 CREATE + 3 CREATE SCHEMA = 118/118 statements succeed

**AC2 — Column Verification (916 columns)**
- For each of 100 data objects: query `INFORMATION_SCHEMA.COLUMNS WHERE table_name = '<table>'`
- Compare each column's: `column_name`, `ordinal_position`, `data_type`, `is_nullable`, `column_description`
- For DECIMAL/NUMERIC: verify `numeric_precision` and `numeric_scale` match source `DECIMAL(p,s)`
- For complex types: query `INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` to verify nested sub-field names, types, and depth
- For the 2 lie columns: verify `column_description` contains the warning text
- Partition columns verified at ordinal positions after all data columns (appended at end)
- Pass: 916/916 columns match on all attributes

**AC3 — Object Type Verification**
- Query `INFORMATION_SCHEMA.TABLES` for all objects
- Assert 98 objects have `table_type = 'BASE TABLE'`
- Assert 2 objects (`mv_site_daily`, `mv_agent_weekly`) have `table_type = 'MATERIALIZED VIEW'`
- Query `INFORMATION_SCHEMA.VIEWS` for 15 view objects — confirm presence
- Any mismatch names the object and its actual vs expected type

**AC4 — Partition/Cluster/Options**
- For each partitioned table: read `INFORMATION_SCHEMA.TABLE_OPTIONS` for partition config
- For each of 12 clustered tables: read clustering_columns from catalog, compare against the exact column list matrix
- For 3 tables: verify `require_partition_filter = true`
- For 10 file-feed tables: verify `partition_expiration_days = 365`
- For 4 ACID tables: confirm no transactional TBLPROPERTIES (BQ tables never have them — absence is the assertion)

**AC5 — FK Type Consistency (56 paths)**
- Parse all `fk=<entity>` annotations from tables.yaml
- For each FK column: read its BQ type from `INFORMATION_SCHEMA.COLUMNS`
- For each referenced PK column: read its BQ type from `INFORMATION_SCHEMA.COLUMNS`
- Assert FK type = PK type (both INT64 for all surrogate/natural keys)
- Cross-dataset paths verified explicitly (staging→ods, staging→dm, ods→dm)

**AC6 — Queryability**
- Execute `SELECT * FROM <dataset>.<object> LIMIT 0` on all 115 objects (98 tables + 2 MVs + 15 views)
- Execute 3 tier-specific join queries (staging, ODS, DM) with `LIMIT 1`
- Any query error names the object and BQ's error

**AC7 — Catalog Completeness**
- Query `INFORMATION_SCHEMA.TABLES` in each of 3 datasets
- Assert every expected object name is present
- An object that EXISTS but fails `SELECT *` is still a HARD FAIL (AC6 catches this)

**AC9 — Scan Reduction (requires fixture data)**
- Prerequisite: load synthetic data from `datagen/` fixtures into the 10 hot-path clustered tables
- For each table: run a cluster-filtered query and an unfiltered query
- Compare `total_bytes_processed` from BQ job statistics
- Assert filtered < unfiltered for all 10 tables
- For 3 `require_partition_filter` tables: confirm unfiltered query is rejected by BQ

### Edge Cases and HARD FAIL Conditions

| Condition | HARD FAIL? | Action |
|---|---|---|
| CREATE statement fails | Yes | Report object name + BQ error |
| Column missing or extra | Yes | Report table + column name |
| Column type mismatch | Yes | Report table + column + expected vs actual type |
| DECIMAL precision/scale mismatch | Yes | Report column + expected vs actual p/s |
| Complex type sub-field mismatch | Yes | Report table + column + sub-field path |
| Missing column description | Yes | Report table + column (for the 70 required descriptions) |
| Object type flip (TABLE↔VIEW) | Yes | Report object + expected vs actual type |
| Partition/cluster mismatch | Yes | Report table + expected vs actual config |
| FK type inconsistency | Yes | Report FK column, PK column, both types |
| SELECT * fails | Yes | Report object + BQ error |
| Catalog read-back absent | Yes | Report object name |
| Two empty sides treated as match | Yes | Absence of an expected object is never a pass |

### Test Execution Order

1. **DDL application** (AC1) — must succeed before any other check
2. **Catalog completeness** (AC7) — confirm all objects exist
3. **Object type verification** (AC3) — confirm TABLE vs VIEW vs MV
4. **Column verification** (AC2) — detailed schema comparison
5. **Partition/cluster verification** (AC4) — structural configuration
6. **FK consistency** (AC5) — cross-table type alignment
7. **Queryability** (AC6) — functional smoke tests
8. **Scan reduction** (AC9) — performance validation (requires fixture data load step)

Steps 2-7 can run in parallel after step 1. Step 8 runs last because it requires data loading.
