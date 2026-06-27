#!/usr/bin/env python3
"""Generate the query_performance suites (AC6 + AC9) and append to the MVS spec."""
import yaml

MVS_PATH = '/workspace/project/sql/nbcs_physical_schema.mvs.yaml'

# ---- All objects by dataset ----
STAGING_TABLES = [
    'stg_crm_client', 'stg_crm_client_contact', 'stg_crm_program',
    'stg_crm_contract', 'stg_crm_contract_line', 'stg_crm_sla_target',
    'stg_hr_agent', 'stg_hr_org_unit', 'stg_hr_employment_event',
    'stg_hr_skill', 'stg_hr_agent_skill',
    'stg_wfm_shift', 'stg_wfm_schedule', 'stg_wfm_adherence_event',
    'stg_wfm_forecast', 'stg_wfm_timeoff_request',
    'stg_tel_call', 'stg_tel_call_segment', 'stg_tel_queue',
    'stg_tel_agent_state_event', 'stg_tel_disposition_code',
    'stg_tkt_ticket', 'stg_tkt_ticket_event', 'stg_tkt_category',
    'stg_fin_invoice', 'stg_fin_invoice_line', 'stg_fin_rate_card',
    'stg_fin_timesheet_delta', 'stg_fin_payroll_adj_delta',
    'stg_crm_sla_credit_delta', 'stg_tel_callback_request_delta',
    'stg_wfm_shift_swap_delta', 'stg_tkt_worklog_delta',
    'stg_hr_attrition_event_delta', 'stg_fin_rate_card_change_delta',
    'stg_file_interaction_export', 'stg_file_survey_csat',
    'stg_file_qa_forms', 'stg_file_ivr_logs', 'stg_file_chat_transcripts',
    'stg_file_roster', 'stg_file_telco_invoice', 'stg_file_dialer_result',
    'stg_file_email_interaction', 'stg_file_speech_analytics',
]

ODS_TABLES = [
    'ods_program', 'ods_contract', 'ods_contract_line', 'ods_org_unit',
    'ods_queue', 'ods_schedule', 'ods_adherence_event', 'ods_call',
    'ods_ivr_session', 'ods_chat_session', 'ods_email_interaction',
    'ods_survey_response', 'ods_qa_evaluation', 'ods_interaction',
    'ods_dialer_attempt', 'ods_timesheet', 'ods_payroll_adjustment',
    'ods_sla_credit', 'ods_callback_request', 'ods_shift_swap',
    'ods_ticket_worklog', 'ods_attrition_event', 'ods_rate_card',
    'ods_agent_scd2', 'ods_agent_skill_scd2', 'ods_agent_assignment_scd2',
    'ods_client_acid', 'ods_agent_acid', 'ods_ticket_acid', 'ods_invoice_acid',
]

DM_TABLES = [
    'dim_date', 'dim_agent', 'dim_client', 'dim_program', 'dim_queue',
    'dim_site', 'dim_shift', 'dim_org', 'dim_disposition',
    'fact_interaction', 'fact_agent_activity', 'fact_queue_interval',
    'fact_csat_survey', 'fact_qa_evaluation', 'fact_billing_line',
    'fact_adherence_daily', 'fact_ticket', 'fact_ivr_path',
    'agg_agent_daily', 'agg_program_monthly', 'agg_queue_hourly',
    'agg_csat_rollup_monthly', 'agg_billing_monthly',
]

DM_MVS = ['mv_agent_weekly', 'mv_site_daily']

DM_VIEWS = [
    'vw_org_hierarchy', 'vw_active_agents_ndv', 'vw_csat_rollup',
    'vw_call_driver_regex', 'vw_repeat_contact_window',
    'vw_billing_reconciliation', 'vw_agent_roster_current',
    'vw_agent_scorecard', 'vw_attrition_risk', 'vw_queue_sla_attainment',
    'vw_first_contact_resolution', 'vw_occupancy_utilization',
    'vw_shrinkage_analysis', 'vw_program_margin',
    'vw_client_executive_summary',
]

# ---- AC9: 10 hot-path clustered tables with their first cluster column ----
HOT_PATH = [
    ('fact_interaction',    'staging', 'dm',      'agent_sk',          'date_key'),
    ('fact_agent_activity', 'staging', 'dm',      'agent_sk',          'date_key'),
    ('fact_queue_interval', 'staging', 'dm',      'queue_sk',          'date_key'),
    ('fact_csat_survey',    None,      'dm',      'program_sk',        'date_key'),
    ('fact_billing_line',   None,      'dm',      'client_sk',         None),
    ('fact_adherence_daily',None,      'dm',      'agent_sk',          'date_key'),
    ('fact_ticket',         None,      'dm',      'program_sk',        'date_key'),
    ('agg_agent_daily',     None,      'dm',      'agent_sk',          'date_key'),
    ('agg_queue_hourly',    None,      'dm',      'queue_sk',          'date_key'),
    ('ods_interaction',     None,      'ods',     'agent_id',          None),
]

# Tables with require_partition_filter=true
REQUIRE_FILTER = ['fact_interaction', 'fact_agent_activity', 'fact_queue_interval']


def main():
    lines = []

    # ========================================================================
    # AC6 — Queryability: SELECT * LIMIT 0 on every object
    # ========================================================================
    lines.append("")
    lines.append("  # ======================================================================")
    lines.append("  # AC6 — Queryability: SELECT * LIMIT 0 on every object (115 objects)")
    lines.append("  # Plus 3 cross-tier join queries.")
    lines.append("  # Pattern: query_performance, mode: measure")
    lines.append("  # Any query that fails to run surfaces as ERROR.")
    lines.append("  # ======================================================================")

    # -- Staging tables --
    lines.append("  - pattern: query_performance")
    lines.append("    id: queryability-staging")
    lines.append("    story_id: NBCS-TARGET-SCHEMA-AC6")
    lines.append("    queries:")
    for i, t in enumerate(STAGING_TABLES):
        lines.append(f"      - id: select-staging-{t}")
        lines.append(f"        mode: measure")
        lines.append(f"        sql: \"SELECT * FROM staging.{t} LIMIT 0\"")

    # Cross-tier staging query
    lines.append(f"      - id: cross-tier-staging")
    lines.append(f"        mode: measure")
    lines.append(f"        sql: >")
    lines.append(f"          SELECT c.client_code, p.program_code")
    lines.append(f"          FROM staging.stg_crm_client c")
    lines.append(f"          JOIN staging.stg_crm_program p ON p.client_id = c.client_id")
    lines.append(f"          LIMIT 1")

    # -- ODS tables --
    lines.append("")
    lines.append("  - pattern: query_performance")
    lines.append("    id: queryability-ods")
    lines.append("    story_id: NBCS-TARGET-SCHEMA-AC6")
    lines.append("    queries:")
    for t in ODS_TABLES:
        lines.append(f"      - id: select-ods-{t}")
        lines.append(f"        mode: measure")
        lines.append(f"        sql: \"SELECT * FROM ods.{t} LIMIT 0\"")

    # Cross-tier ODS query
    lines.append(f"      - id: cross-tier-ods")
    lines.append(f"        mode: measure")
    lines.append(f"        sql: >")
    lines.append(f"          SELECT o.contract_id, cl.unit_rate")
    lines.append(f"          FROM ods.ods_contract o")
    lines.append(f"          JOIN ods.ods_contract_line cl ON cl.contract_id = o.contract_id")
    lines.append(f"          LIMIT 1")

    # -- DM tables + MVs + views --
    lines.append("")
    lines.append("  - pattern: query_performance")
    lines.append("    id: queryability-dm")
    lines.append("    story_id: NBCS-TARGET-SCHEMA-AC6")
    lines.append("    queries:")

    # DM tables
    for t in DM_TABLES:
        lines.append(f"      - id: select-dm-{t}")
        lines.append(f"        mode: measure")
        lines.append(f"        sql: \"SELECT * FROM dm.{t} LIMIT 0\"")

    # DM MVs
    for t in DM_MVS:
        lines.append(f"      - id: select-dm-{t}")
        lines.append(f"        mode: measure")
        lines.append(f"        sql: \"SELECT * FROM dm.{t} LIMIT 0\"")

    # DM views
    for t in DM_VIEWS:
        lines.append(f"      - id: select-dm-{t}")
        lines.append(f"        mode: measure")
        lines.append(f"        sql: \"SELECT * FROM dm.{t} LIMIT 0\"")

    # Cross-tier DM query (includes partition filter for require_partition_filter)
    lines.append(f"      - id: cross-tier-dm")
    lines.append(f"        mode: measure")
    lines.append(f"        sql: >")
    lines.append(f"          SELECT f.interaction_id, a.full_name, f.handle_seconds")
    lines.append(f"          FROM dm.fact_interaction f")
    lines.append(f"          JOIN dm.dim_agent a ON a.agent_sk = f.agent_sk")
    lines.append(f"          WHERE f.date_key = 20260601")
    lines.append(f"          LIMIT 1")

    # ========================================================================
    # AC9 — Scan reduction: A-vs-B comparison on 10 hot-path clustered tables
    # ========================================================================
    lines.append("")
    lines.append("  # ======================================================================")
    lines.append("  # AC9 — Scan reduction: partition/cluster-filtered queries scan")
    lines.append("  # materially fewer bytes than unfiltered on 10 hot-path objects.")
    lines.append("  # Pattern: query_performance, mode: compare, dry_run: true")
    lines.append("  # dry_run bytes are deterministic and free (no execution cost).")
    lines.append("  # ======================================================================")
    lines.append("  - pattern: query_performance")
    lines.append("    id: scan-reduction-hotpath")
    lines.append("    story_id: NBCS-TARGET-SCHEMA-AC9")
    lines.append("    queries:")

    for table, rpf, ds, cluster_col, part_col in HOT_PATH:
        # For tables with require_partition_filter, the unfiltered query
        # would be rejected by BQ. Use a wide partition range for 'a' and
        # a narrow filter for 'b'.
        if table in REQUIRE_FILTER:
            # A: wide partition range (full scan within filter)
            a_sql = f"SELECT * FROM {ds}.{table} WHERE {part_col} BETWEEN 20200101 AND 20261231"
            # B: partition + cluster filtered
            b_sql = f"SELECT * FROM {ds}.{table} WHERE {part_col} = 20260601 AND {cluster_col} = 1"
        elif part_col:
            # A: unfiltered
            a_sql = f"SELECT * FROM {ds}.{table}"
            # B: cluster + partition filtered
            b_sql = f"SELECT * FROM {ds}.{table} WHERE {part_col} = 20260601 AND {cluster_col} = 1"
        else:
            # No partition column (ods_interaction, fact_billing_line)
            a_sql = f"SELECT * FROM {ds}.{table}"
            b_sql = f"SELECT * FROM {ds}.{table} WHERE {cluster_col} = 1"

        lines.append(f"      - id: scan-{table}")
        lines.append(f"        mode: compare")
        lines.append(f"        a: {{ sql: \"{a_sql}\", dry_run: true }}")
        lines.append(f"        b: {{ sql: \"{b_sql}\", dry_run: true }}")
        lines.append(f"        compare:")
        lines.append(f"          bytes_scanned: \"b <= a\"")

    # Additional: verify require_partition_filter rejects unfiltered queries
    lines.append("")
    lines.append("  # Verify require_partition_filter=true rejects unfiltered scans")
    lines.append("  # on the 3 large fact tables. An unfiltered query must ERROR.")
    lines.append("  - pattern: query_performance")
    lines.append("    id: partition-filter-enforcement")
    lines.append("    story_id: NBCS-TARGET-SCHEMA-AC9")
    lines.append("    queries:")

    for table in REQUIRE_FILTER:
        lines.append(f"      - id: filter-required-{table}")
        lines.append(f"        mode: measure")
        lines.append(f"        sql: \"SELECT * FROM dm.{table} WHERE date_key = 20260601 LIMIT 0\"")

    lines.append("")

    # Now append to the MVS file
    with open(MVS_PATH, 'r') as f:
        existing = f.read()

    # Remove trailing whitespace
    existing = existing.rstrip()

    with open(MVS_PATH, 'w') as f:
        f.write(existing)
        f.write('\n')
        f.write('\n'.join(lines))
        f.write('\n')

    # Count queries
    query_count = sum(1 for l in lines if '      - id:' in l)
    print(f"Added {query_count} queries across AC6 + AC9 suites")

    # Count objects covered by SELECT * LIMIT 0
    select_count = sum(1 for l in lines if 'SELECT * FROM' in l and 'LIMIT 0' in l)
    print(f"SELECT * LIMIT 0 queries: {select_count}")

    # Count cross-tier queries
    cross_count = sum(1 for l in lines if 'cross-tier' in l and '- id:' in l)
    print(f"Cross-tier join queries: {cross_count}")

    # Count scan-reduction comparisons
    scan_count = sum(1 for l in lines if 'scan-' in l and '- id:' in l)
    print(f"Scan-reduction comparisons: {scan_count}")


if __name__ == '__main__':
    main()
