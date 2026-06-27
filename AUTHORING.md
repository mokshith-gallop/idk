# Authoring guide — writing MVS specs

The execution agent emits a **Migration Validation Spec (MVS)**; the fixed harness runs
it (SPEC §4.1). The agent never writes assertion / canonicalization / dialect-SQL /
hashing code — it only declares inputs, expected, and which patterns to run. This guide
is the reference for what to emit.

A worked end-to-end example lives in [`examples/reference_migration/`](examples/reference_migration/).

## Pick a mode

| You want to… | Mode | How |
|---|---|---|
| Prove the migration **code** produces the right target, from a clean slate | **build-and-verify** (default) | add a `migration:` block |
| Certify an **already-deployed** target (incl. prod), changing nothing | **read-only assessment** | set `read_only: true` |

`read_only: true` blocks every mutating pattern (it can't seed/build), so it is safe
against production. `read_only` + `migration:` is rejected (a contradiction).

## Envelope

```yaml
name: <spec name>
read_only: false              # default; true = assessment mode
connections:
  source: { engine: impala }  # impala | hive | bigquery
  target: { engine: bigquery }
migration: { ... }            # optional; presence => build-and-verify
suites: [ ... ]               # the validations to run
```

Env vars interpolate as `${VAR}` or `${VAR:-default}` (SPEC §8 — never hardcode
dataset/host names). `${BUILD_DATASET}` resolves to the clean build dataset.

## The build-and-verify block

```yaml
migration:
  source_map:                 # ground truth: legacy -> migrated (expectations anchor here)
    - { source: "${SOURCE_DATABASE}.ods_invoice_acid", target: ods_invoice }
  build_dataset: dmt_build    # optional; default dmt_build (reset at START of each run)
  isolate: false              # optional; true = unique per-run dataset for parallel lanes
  steps:                      # applied VERBATIM, in order; abort on first error
    - { kind: ddl,       target: ods_invoice,     sql: sql/ddl/ods_invoice.sql }
    - { kind: load,      target: ods_invoice_raw, from: "gs://…/*", format: PARQUET }
    - { kind: transform, target: ods_invoice,     sql: sql/transform/ods_invoice.sql }
```

- `kind`: `ddl` | `load` | `transform` | `external` (external = non-SQL ETL, not yet
  implemented — use SQL or the E2E adapters when built).
- Unqualified table names in the SQL resolve to the build dataset (default-dataset
  redirection — do **not** hardcode the dataset in your SQL).

## SQL inputs — the one convention

Everywhere a pattern takes SQL, it comes in two forms:

| Form | Key |
|---|---|
| **path** to a `.sql` file (the CUT's artifact — preferred) | `sql:` |
| **inline** SQL | `sql_text:` |

Role-based patterns use a prefix: `transform_diff` → `legacy_sql`/`legacy_transform`
(inline/path) and `migrated_sql`/`migrated_transform`; `query_parity` → `source_sql`/
`source_sql_path` and `target_sql`/`target_sql_path`. Prefer the **path** form so the
spec validates the CUT's actual checked-in SQL.

## Declaring data with `given`

`given` stands up tables — in BigQuery for unit tests, in **both** engines for
`transform_diff`. No Python loader needed.

```yaml
given:
  ods_invoice_raw:
    columns:
      - { name: invoice_id, type: INT64 }
      - { name: amount,     type: NUMERIC, scale: 2 }   # scale matters for decimals
      - { name: op,         type: STRING }
    rows:
      - { invoice_id: 5001, amount: "100.00", op: "I" }
```
Types: `INT64`, `NUMERIC` (+ `scale`), `TIMESTAMP`, `DATE`, `STRING`, `BOOL`, `FLOAT64`.
Decimals/timestamps may be written as strings; they canonicalize correctly on compare.

## Choosing a validation tier

| Tier | Pattern | When | Cost |
|---|---|---|---|
| **Unit** | `transform_unit` | test one transform's logic on controlled input | BQ only, ¢ |
| **Equivalence** | `transform_diff` | prove a rewrite matches the legacy transform | both engines, ¢ |
| **Integration** | `rowcount_parity`, `aggregate_parity`, `fingerprint_parity`, `query_parity`, `scd2_continuity`, `merge_idempotency`, `fk_orphan`, `epoch_conversion`, `decimal_roundtrip` | compare migrated vs legacy on real data | both engines |
| **Schema** | `schema_conformance` | target types/partition/cluster/options correct | BQ |
| **Egress / Orchestration / Perf** | `egress_parity`, `dag_structure`, `query_performance` | exports, Composer DAGs, performance | varies |

Prefer **many unit tests** (cheap, precise) + **diff** for rewrites; reserve full
parity for real-data acceptance.

### `transform_unit` — exact rows or properties

```yaml
- pattern: transform_unit
  sql: sql/transform/ods_invoice.sql      # the CUT's artifact (reuse it!)
  given: { ods_invoice_raw: { columns: [...], rows: [...] } }
  expect:
    table: ods_invoice
    rows: [ { invoice_id: 5001, op: "I", issued_ts: "2026-06-01T00:00:00Z" } ]  # canonicalized set-equality
    assert: [ { rowcount: 2 }, { unique: [invoice_id] }, { no_nulls: [issued_ts] } ]
```
`expect.rows` authored from *requirements* = a correctness test; a captured snapshot =
a regression test. Use `transform_diff` to avoid hand-authoring the answer entirely.

### `transform_diff` — equivalence (legacy = oracle)

```yaml
- pattern: transform_diff
  given: { ods_invoice_raw: { columns: [...], rows: [...] } }
  legacy_sql:   "SELECT op, COUNT(*) n, SUM(amount) total FROM ods_invoice_raw GROUP BY op"
  migrated_sql: "SELECT op, COUNT(*) n, SUM(amount) total FROM ods_invoice_raw GROUP BY op"
```
Both transforms must be **SELECTs** (the pattern compares result sets). Keep them
dialect-safe where engines differ (e.g. avoid epoch/timezone functions in a diff).

## The invariants (what keeps a green meaningful)

1. The harness **applies** the CUT's SQL; it never **authors** it.
2. Apply **verbatim** — no normalization (that would mask defects).
3. Expectations anchor to **source ground truth** (`source_map` / legacy T), never the DDL.
4. **Clean slate + guarded build dataset** (can't touch a real/named dataset).
5. **Multi-axis** — schema *and* data, so an empty/faked target fails.

## Output

Every run produces a `Report` (PASS / FAIL / ERROR per check) that flattens to the
platform's `cuj_validation_results` shape (SPEC §11). No SKIP: unsupported paths are
removed, a missing env var errors (fail-fast).
