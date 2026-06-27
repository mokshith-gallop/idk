"""AC5 — FK / PK column type consistency across datasets.

For each of 56 documented FK relationships, the FK column's BQ type must match
the PK column's BQ type in the referenced entity's table — both sides read from
the live INFORMATION_SCHEMA.COLUMNS catalog after DDL application.

Any mismatch is a HARD FAIL naming both columns and both types.
"""
from __future__ import annotations

import pytest

# ── FK→PK join paths (56 total) ───────────────────────────────────────────
# Each entry: (fk_dataset, fk_table, fk_column, pk_dataset, pk_table, pk_column)
#
# Derived from manifests/tables.yaml fk=<entity> annotations + the locked
# star-schema ER diagram. Grouped by relationship chain.
#
# Surrogate-key chains verified:
#   agent_sk:  dim_agent ↔ fact_interaction/agent_activity/csat_survey/qa_evaluation/
#              adherence_daily/agg_agent_daily (6 paths)
#   client_sk: dim_client ↔ fact_interaction/billing_line/agg_billing_monthly/
#              agg_program_monthly/agg_csat_rollup_monthly (5 paths)
#   program_sk: dim_program ↔ fact_interaction/billing_line/csat_survey/ticket/
#               agg_program_monthly (5 paths)
#   queue_sk:  dim_queue ↔ fact_queue_interval/agg_queue_hourly (2 paths)

FK_PATHS = [
    # ── staging: CRM FKs ──
    ("staging", "stg_crm_client_contact", "client_id", "staging", "stg_crm_client", "client_id"),
    ("staging", "stg_crm_program", "client_id", "staging", "stg_crm_client", "client_id"),
    ("staging", "stg_crm_contract", "client_id", "staging", "stg_crm_client", "client_id"),
    ("staging", "stg_crm_contract", "program_id", "staging", "stg_crm_program", "program_id"),
    ("staging", "stg_crm_contract_line", "contract_id", "staging", "stg_crm_contract", "contract_id"),
    ("staging", "stg_crm_sla_target", "program_id", "staging", "stg_crm_program", "program_id"),
    ("staging", "stg_crm_sla_target", "queue_id", "staging", "stg_tel_queue", "queue_id"),
    # ── staging: HR FKs ──
    ("staging", "stg_hr_agent", "org_unit_id", "staging", "stg_hr_org_unit", "org_unit_id"),
    ("staging", "stg_hr_employment_event", "agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_hr_employment_event", "from_org_unit_id", "staging", "stg_hr_org_unit", "org_unit_id"),
    ("staging", "stg_hr_employment_event", "to_org_unit_id", "staging", "stg_hr_org_unit", "org_unit_id"),
    ("staging", "stg_hr_agent_skill", "agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_hr_agent_skill", "skill_id", "staging", "stg_hr_skill", "skill_id"),
    # ── staging: WFM FKs ──
    ("staging", "stg_wfm_schedule", "agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_wfm_schedule", "shift_id", "staging", "stg_wfm_shift", "shift_id"),
    ("staging", "stg_wfm_adherence_event", "agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_wfm_adherence_event", "schedule_id", "staging", "stg_wfm_schedule", "schedule_id"),
    ("staging", "stg_wfm_forecast", "queue_id", "staging", "stg_tel_queue", "queue_id"),
    ("staging", "stg_wfm_timeoff_request", "agent_id", "staging", "stg_hr_agent", "agent_id"),
    # ── staging: Telephony FKs ──
    ("staging", "stg_tel_call", "queue_id", "staging", "stg_tel_queue", "queue_id"),
    ("staging", "stg_tel_call", "agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_tel_call", "program_id", "staging", "stg_crm_program", "program_id"),
    ("staging", "stg_tel_call_segment", "call_id", "staging", "stg_tel_call", "call_id"),
    ("staging", "stg_tel_call_segment", "agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_tel_agent_state_event", "agent_id", "staging", "stg_hr_agent", "agent_id"),
    # ── staging: Ticketing FKs ──
    ("staging", "stg_tkt_ticket", "program_id", "staging", "stg_crm_program", "program_id"),
    ("staging", "stg_tkt_ticket", "category_id", "staging", "stg_tkt_category", "category_id"),
    ("staging", "stg_tkt_ticket", "opened_by_agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_tkt_ticket", "assigned_agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_tkt_ticket_event", "ticket_id", "staging", "stg_tkt_ticket", "ticket_id"),
    ("staging", "stg_tkt_ticket_event", "actor_agent_id", "staging", "stg_hr_agent", "agent_id"),
    # ── staging: Finance FKs ──
    ("staging", "stg_fin_invoice", "client_id", "staging", "stg_crm_client", "client_id"),
    ("staging", "stg_fin_invoice", "program_id", "staging", "stg_crm_program", "program_id"),
    ("staging", "stg_fin_invoice_line", "invoice_id", "staging", "stg_fin_invoice", "invoice_id"),
    ("staging", "stg_fin_invoice_line", "contract_line_id", "staging", "stg_crm_contract_line", "contract_line_id"),
    ("staging", "stg_fin_rate_card", "program_id", "staging", "stg_crm_program", "program_id"),
    # ── staging: Delta FKs ──
    ("staging", "stg_fin_timesheet_delta", "agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_fin_timesheet_delta", "program_id", "staging", "stg_crm_program", "program_id"),
    ("staging", "stg_fin_payroll_adj_delta", "agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_crm_sla_credit_delta", "program_id", "staging", "stg_crm_program", "program_id"),
    ("staging", "stg_tel_callback_request_delta", "call_id", "staging", "stg_tel_call", "call_id"),
    ("staging", "stg_tel_callback_request_delta", "queue_id", "staging", "stg_tel_queue", "queue_id"),
    ("staging", "stg_wfm_shift_swap_delta", "requesting_agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_wfm_shift_swap_delta", "schedule_id", "staging", "stg_wfm_schedule", "schedule_id"),
    ("staging", "stg_tkt_worklog_delta", "ticket_id", "staging", "stg_tkt_ticket", "ticket_id"),
    ("staging", "stg_tkt_worklog_delta", "agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_hr_attrition_event_delta", "agent_id", "staging", "stg_hr_agent", "agent_id"),
    ("staging", "stg_fin_rate_card_change_delta", "rate_card_id", "staging", "stg_fin_rate_card", "rate_card_id"),
    # ── self-FK ──
    ("ods", "ods_org_unit", "parent_unit_id", "ods", "ods_org_unit", "org_unit_id"),
    # ── cross-dataset layer-skip: staging → ods ──
    ("staging", "stg_fin_invoice", "invoice_id", "ods", "ods_invoice_acid", "invoice_id"),
    # ── cross-dataset layer-skip: staging → dm ──
    ("staging", "stg_crm_sla_target", "queue_id", "dm", "dim_queue", "queue_id"),
    # ── DM surrogate-key chains ──
    ("dm", "fact_interaction", "agent_sk", "dm", "dim_agent", "agent_sk"),
    ("dm", "fact_interaction", "client_sk", "dm", "dim_client", "client_sk"),
    ("dm", "fact_interaction", "program_sk", "dm", "dim_program", "program_sk"),
    ("dm", "fact_interaction", "queue_sk", "dm", "dim_queue", "queue_sk"),
    ("dm", "fact_billing_line", "client_sk", "dm", "dim_client", "client_sk"),
]

assert len(FK_PATHS) == 56, f"Expected 56 FK paths, got {len(FK_PATHS)}"


def _column_type(bq_engine, dataset: str, table: str, column: str) -> str | None:
    """Read a column's data_type from INFORMATION_SCHEMA.COLUMNS."""
    sql = (
        f"SELECT data_type "
        f"FROM `{bq_engine.cfg.project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
        f"WHERE table_name = '{table}' AND column_name = '{column}'"
    )
    rows = bq_engine.query(sql)
    if not rows:
        return None
    return rows[0]["data_type"]


@pytest.mark.live_bq
class TestFKConsistency:
    """AC5: 56 FK→PK pairs type-consistent, 0 mismatches."""

    def test_all_56_fk_pk_pairs(self, bq_engine):
        """Every FK column's BQ type matches its referenced PK column's BQ type."""
        failures = []
        for fk_ds, fk_tbl, fk_col, pk_ds, pk_tbl, pk_col in FK_PATHS:
            fk_type = _column_type(bq_engine, fk_ds, fk_tbl, fk_col)
            pk_type = _column_type(bq_engine, pk_ds, pk_tbl, pk_col)

            label = f"{fk_ds}.{fk_tbl}.{fk_col} -> {pk_ds}.{pk_tbl}.{pk_col}"

            if fk_type is None:
                failures.append(f"FK column NOT FOUND: {label}")
                continue
            if pk_type is None:
                failures.append(f"PK column NOT FOUND: {label}")
                continue
            if fk_type != pk_type:
                failures.append(
                    f"TYPE MISMATCH: {label}: FK={fk_type}, PK={pk_type}"
                )

        assert not failures, (
            f"{len(failures)} FK/PK type consistency failures:\n"
            + "\n".join(failures)
        )

    def test_agent_sk_chain(self, bq_engine):
        """agent_sk INT64 across dim_agent and all fact/agg tables."""
        agent_sk_tables = [
            ("dm", "dim_agent"),
            ("dm", "fact_interaction"),
            ("dm", "fact_agent_activity"),
            ("dm", "fact_csat_survey"),
            ("dm", "fact_qa_evaluation"),
            ("dm", "fact_adherence_daily"),
            ("dm", "agg_agent_daily"),
        ]
        types = {}
        for ds, tbl in agent_sk_tables:
            t = _column_type(bq_engine, ds, tbl, "agent_sk")
            types[f"{ds}.{tbl}"] = t
        unique_types = set(types.values())
        assert unique_types == {"INT64"}, f"agent_sk type mismatch: {types}"

    def test_client_sk_chain(self, bq_engine):
        """client_sk INT64 across dim_client and all fact/agg tables."""
        tables = [
            ("dm", "dim_client"),
            ("dm", "fact_interaction"),
            ("dm", "fact_billing_line"),
            ("dm", "agg_billing_monthly"),
            ("dm", "agg_program_monthly"),
            ("dm", "agg_csat_rollup_monthly"),
        ]
        types = {}
        for ds, tbl in tables:
            t = _column_type(bq_engine, ds, tbl, "client_sk")
            types[f"{ds}.{tbl}"] = t
        unique_types = set(types.values())
        assert unique_types == {"INT64"}, f"client_sk type mismatch: {types}"

    def test_program_sk_chain(self, bq_engine):
        """program_sk INT64 across dim_program and all fact/agg tables."""
        tables = [
            ("dm", "dim_program"),
            ("dm", "fact_interaction"),
            ("dm", "fact_billing_line"),
            ("dm", "fact_csat_survey"),
            ("dm", "fact_ticket"),
            ("dm", "agg_program_monthly"),
        ]
        types = {}
        for ds, tbl in tables:
            t = _column_type(bq_engine, ds, tbl, "program_sk")
            types[f"{ds}.{tbl}"] = t
        unique_types = set(types.values())
        assert unique_types == {"INT64"}, f"program_sk type mismatch: {types}"

    def test_queue_sk_chain(self, bq_engine):
        """queue_sk INT64 across dim_queue and fact/agg tables."""
        tables = [
            ("dm", "dim_queue"),
            ("dm", "fact_queue_interval"),
            ("dm", "agg_queue_hourly"),
        ]
        types = {}
        for ds, tbl in tables:
            t = _column_type(bq_engine, ds, tbl, "queue_sk")
            types[f"{ds}.{tbl}"] = t
        unique_types = set(types.values())
        assert unique_types == {"INT64"}, f"queue_sk type mismatch: {types}"

    def test_self_fk_org_unit(self, bq_engine):
        """ods_org_unit.parent_unit_id ↔ ods_org_unit.org_unit_id — same type."""
        parent = _column_type(bq_engine, "ods", "ods_org_unit", "parent_unit_id")
        pk = _column_type(bq_engine, "ods", "ods_org_unit", "org_unit_id")
        assert parent == pk == "INT64", f"self-FK mismatch: parent={parent}, pk={pk}"
