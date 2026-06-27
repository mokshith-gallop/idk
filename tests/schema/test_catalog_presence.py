"""AC7 — Catalog completeness: every created object is present and readable.

After DDL application, queries INFORMATION_SCHEMA.TABLES in all 3 datasets,
asserts all 115 expected objects are present with correct table_type.
Any missing object is a HARD FAIL (never 'skipped').
Two empty/missing sides are never treated as a match.
"""
from __future__ import annotations

import pytest

# ── Expected object inventory ──────────────────────────────────────────────

STAGING_TABLES = [
    # sqoop mirrors (27)
    "stg_crm_client", "stg_crm_client_contact", "stg_crm_program",
    "stg_crm_contract", "stg_crm_contract_line", "stg_crm_sla_target",
    "stg_hr_agent", "stg_hr_org_unit", "stg_hr_employment_event",
    "stg_hr_skill", "stg_hr_agent_skill",
    "stg_wfm_shift", "stg_wfm_schedule", "stg_wfm_adherence_event",
    "stg_wfm_forecast", "stg_wfm_timeoff_request",
    "stg_tel_call", "stg_tel_call_segment", "stg_tel_queue",
    "stg_tel_agent_state_event", "stg_tel_disposition_code",
    "stg_tkt_ticket", "stg_tkt_ticket_event", "stg_tkt_category",
    "stg_fin_invoice", "stg_fin_invoice_line", "stg_fin_rate_card",
    # delta feeds (8)
    "stg_fin_timesheet_delta", "stg_fin_payroll_adj_delta",
    "stg_crm_sla_credit_delta", "stg_tel_callback_request_delta",
    "stg_wfm_shift_swap_delta", "stg_tkt_worklog_delta",
    "stg_hr_attrition_event_delta", "stg_fin_rate_card_change_delta",
    # file feeds (10)
    "stg_file_interaction_export", "stg_file_survey_csat",
    "stg_file_qa_forms", "stg_file_ivr_logs", "stg_file_chat_transcripts",
    "stg_file_roster", "stg_file_telco_invoice", "stg_file_dialer_result",
    "stg_file_email_interaction", "stg_file_speech_analytics",
]

ODS_TABLES = [
    # cleanse (15)
    "ods_program", "ods_contract", "ods_contract_line", "ods_org_unit",
    "ods_queue", "ods_schedule", "ods_adherence_event", "ods_call",
    "ods_ivr_session", "ods_chat_session", "ods_email_interaction",
    "ods_survey_response", "ods_qa_evaluation", "ods_interaction",
    "ods_dialer_attempt",
    # delta-merge (8)
    "ods_timesheet", "ods_payroll_adjustment", "ods_sla_credit",
    "ods_callback_request", "ods_shift_swap", "ods_ticket_worklog",
    "ods_attrition_event", "ods_rate_card",
    # SCD-2 (3)
    "ods_agent_scd2", "ods_agent_skill_scd2", "ods_agent_assignment_scd2",
    # ACID (4)
    "ods_client_acid", "ods_agent_acid", "ods_ticket_acid", "ods_invoice_acid",
]

DM_TABLES = [
    # dimensions (9)
    "dim_date", "dim_agent", "dim_client", "dim_program", "dim_queue",
    "dim_site", "dim_shift", "dim_org", "dim_disposition",
    # facts (9)
    "fact_interaction", "fact_agent_activity", "fact_queue_interval",
    "fact_csat_survey", "fact_qa_evaluation", "fact_billing_line",
    "fact_adherence_daily", "fact_ticket", "fact_ivr_path",
    # physical aggs (5)
    "agg_agent_daily", "agg_program_monthly", "agg_queue_hourly",
    "agg_csat_rollup_monthly", "agg_billing_monthly",
]

DM_MATERIALIZED_VIEWS = ["mv_agent_weekly", "mv_site_daily"]

DM_VIEWS = [
    "vw_org_hierarchy", "vw_active_agents_ndv", "vw_csat_rollup",
    "vw_call_driver_regex", "vw_repeat_contact_window",
    "vw_billing_reconciliation", "vw_agent_roster_current",
    "vw_agent_scorecard", "vw_attrition_risk", "vw_queue_sla_attainment",
    "vw_first_contact_resolution", "vw_occupancy_utilization",
    "vw_shrinkage_analysis", "vw_program_margin",
    "vw_client_executive_summary",
]

# Sanity: 45 + 30 + 23 + 2 + 15 = 115
assert len(STAGING_TABLES) == 45
assert len(ODS_TABLES) == 30
assert len(DM_TABLES) == 23
assert len(DM_MATERIALIZED_VIEWS) == 2
assert len(DM_VIEWS) == 15

ALL_TABLES = (
    [(t, "staging") for t in STAGING_TABLES]
    + [(t, "ods") for t in ODS_TABLES]
    + [(t, "dm") for t in DM_TABLES]
)  # 98 BASE TABLEs

ALL_OBJECTS = (
    [(t, ds, "BASE TABLE") for t, ds in ALL_TABLES]
    + [(mv, "dm", "MATERIALIZED VIEW") for mv in DM_MATERIALIZED_VIEWS]
    + [(v, "dm", "VIEW") for v in DM_VIEWS]
)  # 115 total


def _query_catalog(bq_engine, dataset: str) -> dict[str, str]:
    """Read INFORMATION_SCHEMA.TABLES for a dataset → {table_name: table_type}."""
    sql = (
        f"SELECT table_name, table_type "
        f"FROM `{bq_engine.cfg.project}.{dataset}.INFORMATION_SCHEMA.TABLES`"
    )
    rows = bq_engine.query(sql)
    return {r["table_name"]: r["table_type"] for r in rows}


@pytest.mark.live_bq
class TestCatalogPresence:
    """AC7: every DDL object confirmed present and readable from live catalog."""

    def test_all_115_objects_present(self, bq_engine):
        """Verify every expected object exists in the catalog with correct type."""
        catalogs = {}
        for ds in ("staging", "ods", "dm"):
            catalogs[ds] = _query_catalog(bq_engine, ds)

        failures = []
        for name, dataset, expected_type in ALL_OBJECTS:
            actual_type = catalogs.get(dataset, {}).get(name)
            if actual_type is None:
                failures.append(f"MISSING: {dataset}.{name} (expected {expected_type})")
            elif actual_type != expected_type:
                failures.append(
                    f"TYPE FLIP: {dataset}.{name}: expected {expected_type}, "
                    f"got {actual_type}"
                )

        assert not failures, (
            f"{len(failures)} catalog presence failures:\n"
            + "\n".join(failures)
        )

    def test_staging_count(self, bq_engine):
        catalog = _query_catalog(bq_engine, "staging")
        tables = {k for k, v in catalog.items() if v == "BASE TABLE"}
        assert len(tables) == 45, f"staging table count {len(tables)}, expected 45"

    def test_ods_count(self, bq_engine):
        catalog = _query_catalog(bq_engine, "ods")
        tables = {k for k, v in catalog.items() if v == "BASE TABLE"}
        assert len(tables) == 30, f"ods table count {len(tables)}, expected 30"

    def test_dm_table_count(self, bq_engine):
        catalog = _query_catalog(bq_engine, "dm")
        tables = {k for k, v in catalog.items() if v == "BASE TABLE"}
        assert len(tables) == 23, f"dm table count {len(tables)}, expected 23"

    def test_dm_mv_count(self, bq_engine):
        catalog = _query_catalog(bq_engine, "dm")
        mvs = {k for k, v in catalog.items() if v == "MATERIALIZED VIEW"}
        assert mvs == set(DM_MATERIALIZED_VIEWS), f"dm MVs: {mvs}"

    def test_dm_view_count(self, bq_engine):
        catalog = _query_catalog(bq_engine, "dm")
        views = {k for k, v in catalog.items() if v == "VIEW"}
        assert views == set(DM_VIEWS), f"dm views: {views}"
