"""Mode-2 build-and-verify: the harness applies the CUT's artifacts against a CLEAN
build dataset, then the suites verify the result, then (optionally) teardown.

Design: DESIGN-MODE2.md. The line that keeps this honest is **apply != author** — the
harness runs the CUT's DDL/ELT SQL *verbatim*; it never writes it, and the suite
expectations are anchored to source ground truth, never to the DDL.

Clean slate is required; a unique name is not. Default = a fixed, well-known dataset
`dmt_build` reset at the START of each run (so a crashed prior run can't poison this
one). `--isolate` (migration.isolate) uses a unique `dmt_build_<id>` for parallel lanes.

A guard makes it structurally impossible to reset/drop anything that isn't a build
dataset (name must be `dmt_build` or `dmt_build_*` AND carry the dmt_ephemeral label).
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from .mvs import expand_env

BUILD_DS_DEFAULT = "dmt_build"
LABEL_KEY = "dmt_ephemeral"
LABEL_VAL = "true"

# GCS bulk-load source formats accepted in a `kind: load` step.
_LOAD_FORMATS = {"CSV": "CSV", "PARQUET": "PARQUET", "JSON": "NEWLINE_DELIMITED_JSON",
                 "AVRO": "AVRO", "ORC": "ORC"}


class BuildError(RuntimeError):
    pass


class BuildGuardError(BuildError):
    """Raised when something asks to reset/drop a dataset that is not a build dataset."""


def _guard(name: str) -> None:
    if name != BUILD_DS_DEFAULT and not name.startswith(BUILD_DS_DEFAULT + "_"):
        raise BuildGuardError(
            f"refusing to create/reset/drop '{name}': build-and-verify only owns "
            f"'{BUILD_DS_DEFAULT}' or '{BUILD_DS_DEFAULT}_*' datasets (it must never "
            f"build into or wipe a real/named target)")


def _ref(bq, name: str):
    return bq._bq.DatasetReference(bq.cfg.project, name)


def provision_build_dataset(bq, name: str = BUILD_DS_DEFAULT) -> str:
    """Reset-at-start: drop the build dataset if present, recreate it empty + labeled.
    Refuses to touch a same-named dataset that lacks the ephemeral label (it could be a
    real dataset someone happened to name `dmt_build`)."""
    from google.api_core.exceptions import NotFound

    _guard(name)
    ref = _ref(bq, name)
    try:
        existing = bq.client.get_dataset(ref)
        if (existing.labels or {}).get(LABEL_KEY) != LABEL_VAL:
            raise BuildGuardError(
                f"refusing to reset '{name}': it exists without the {LABEL_KEY}={LABEL_VAL} "
                f"label, so it is not a build dataset (it may be real). Drop it manually "
                f"or pick another build dataset name.")
        bq.client.delete_dataset(ref, delete_contents=True, not_found_ok=True)
    except NotFound:
        pass
    ds = bq._bq.Dataset(ref)
    ds.location = bq.cfg.location
    ds.labels = {LABEL_KEY: LABEL_VAL}
    bq.client.create_dataset(ds, exists_ok=False)
    return name


def teardown(bq, name: str) -> None:
    """Drop a build dataset. Guarded by name; no-op-safe if already gone."""
    _guard(name)
    bq.client.delete_dataset(_ref(bq, name), delete_contents=True, not_found_ok=True)


def run_sql(bq, build_ds: str, sql: str):
    """Run SQL with default dataset = build_ds, so the CUT's unqualified table refs
    redirect to the build dataset (ref-redirection by default-dataset, NOT by rewriting
    FROM clauses)."""
    job_cfg = bq._bq.QueryJobConfig(default_dataset=_ref(bq, build_ds))
    return bq.client.query(sql, job_config=job_cfg).result()


def query_sql(bq, build_ds: str, sql: str) -> list[dict]:
    """run_sql but return rows (for a SELECT whose results we compare)."""
    job_cfg = bq._bq.QueryJobConfig(default_dataset=_ref(bq, build_ds))
    return [dict(r.items()) for r in bq.client.query(sql, job_config=job_cfg).result()]


# ---------------------------------------------------------------------------
# Declarative cross-engine seeding: one `given` (columns + rows) -> tables in
# either BigQuery (target) or an HS2 engine (Impala/Hive source). This is what
# lets a spec stand up source AND destination tables itself (no Python loader).
# ---------------------------------------------------------------------------

_BQ_TYPE = {"INT64": "INTEGER", "FLOAT64": "FLOAT", "BOOL": "BOOLEAN"}
_HS2_TYPE = {"INT64": "BIGINT", "FLOAT64": "DOUBLE", "BOOL": "BOOLEAN",
             "STRING": "STRING", "TIMESTAMP": "TIMESTAMP", "DATE": "DATE", "INT": "INT"}
_DECIMALS = {"NUMERIC", "DECIMAL", "BIGNUMERIC"}


def _hs2_type(col: dict) -> str:
    t = col["type"].upper()
    if t in _DECIMALS:
        return f"DECIMAL(38,{col.get('scale', 9)})"
    return _HS2_TYPE.get(t, t)


def _hs2_lit(v, coltype: str) -> str:
    if v is None:
        return "NULL"
    t = coltype.upper()
    if t in ("BOOL", "BOOLEAN"):
        return "true" if v else "false"
    if t in _DECIMALS:
        return str(Decimal(str(v)))
    if t == "TIMESTAMP":
        s = str(v).replace("T", " ").replace("Z", "").split("+")[0].strip()
        return f"CAST('{s}' AS TIMESTAMP)"
    if t == "DATE":
        return f"CAST('{v}' AS DATE)"
    if t in ("INT64", "INT", "BIGINT", "FLOAT64", "DOUBLE"):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def seed_bigquery(bq, dataset: str, name: str, table: dict) -> None:
    fields = [bq._bq.SchemaField(c["name"], _BQ_TYPE.get(c["type"].upper(), c["type"].upper()),
                                 mode=c.get("mode", "NULLABLE")) for c in table["columns"]]
    ref = _ref(bq, dataset).table(name)
    job_cfg = bq._bq.LoadJobConfig(schema=fields, write_disposition="WRITE_TRUNCATE")
    bq.client.load_table_from_json(table["rows"], ref, job_config=job_cfg).result()


def seed_hs2(hs2, db: str, name: str, table: dict) -> None:
    cols = table["columns"]
    ddl = ", ".join(f"{c['name']} {_hs2_type(c)}" for c in cols)
    types = {c["name"]: c["type"] for c in cols}
    stmts = [f"DROP TABLE IF EXISTS {db}.{name}",
             f"CREATE TABLE {db}.{name} ({ddl}) STORED AS PARQUET"]
    if table["rows"]:
        vals = ",\n".join("(" + ",".join(_hs2_lit(r.get(c["name"]), types[c["name"]]) for c in cols) + ")"
                          for r in table["rows"])
        stmts.append(f"INSERT INTO {db}.{name} VALUES\n{vals}")
    hs2.execute(*stmts)


def seed_given(engine, dataset: str, given: dict) -> None:
    """Seed every table in `given` into `dataset` on `engine` (BigQuery or HS2)."""
    for name, table in given.items():
        if getattr(engine, "name", "") == "bigquery":
            seed_bigquery(engine, dataset, name, table)
        else:
            seed_hs2(engine, dataset, name, table)


def sql_from(spec: dict, path_key: str, text_key: str, base_dir: str = ".") -> str | None:
    """Canonical SQL resolution shared by every pattern: a `<text_key>` inline string,
    or a `<path_key>` path to a .sql file (the CUT's artifact). env-expanded. Returns
    None if neither is set. Keeps SQL-passing uniform across patterns."""
    if spec.get(text_key):
        return expand_env(spec[text_key])
    if spec.get(path_key):
        return expand_env((Path(base_dir) / spec[path_key]).read_text())
    return None


def _step_sql(step: dict, base_dir: str) -> str:
    sql = sql_from(step, "sql", "sql_text", base_dir)
    if sql is None:
        raise BuildError(f"{step.get('kind')} step needs 'sql' (path) or 'sql_text' (inline)")
    return sql


def _load(bq, build_ds: str, step: dict) -> None:
    fmt = str(step.get("format", "CSV")).upper()
    if fmt not in _LOAD_FORMATS:
        raise BuildError(f"unsupported load format '{fmt}' (one of {sorted(_LOAD_FORMATS)})")
    job_cfg = bq._bq.LoadJobConfig(
        source_format=getattr(bq._bq.SourceFormat, _LOAD_FORMATS[fmt]),
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    ref = _ref(bq, build_ds).table(step["target"])
    bq.client.load_table_from_uri(step["from"], ref, job_config=job_cfg).result()


def apply_step(bq, build_ds: str, step: dict, base_dir: str = ".") -> None:
    """Apply one migration step VERBATIM. Any error propagates (the orchestrator aborts
    the remaining steps — a half-built target must not be judged as complete)."""
    kind = step.get("kind")
    if kind in ("ddl", "transform"):
        run_sql(bq, build_ds, _step_sql(step, base_dir))
    elif kind == "load":
        _load(bq, build_ds, step)
    elif kind == "external":
        raise BuildError("kind 'external' (non-SQL/ETL transforms) is not supported yet "
                         "— use the E2E adapters when built (see DESIGN-MODE2 non-goals)")
    else:
        raise BuildError(f"unknown migration step kind: {kind!r}")
