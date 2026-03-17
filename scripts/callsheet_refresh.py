#!/usr/bin/env python3
"""
Call Sheet Data Refresh — Queries Azure SQL for recall, overdue, and
unscheduled treatment data and writes data/callsheets.json for the
PSPD Interactive Call Sheet Dashboard.

Data sources (existing Azure SQL views — these are the SAME views
consumed by the Power BI dashboards and represent TRUTH data):

  Recall (Due Next 30 Days):
    vw_RecallHouseholdDueNext30Days_PBI
      → RPID, PrimaryPhone, Email, KidsDueCount, KidsDueList,
        LastHygieneOfficeName, LastProviderName, MostFrequentOfficeName,
        SuggestedFamilyDate

  Overdue Recall:
    vw_RecallHouseholdDue
      → RPID, PrimaryPhone, Email, KidsDueCount, KidsDueList,
        LastHygieneOfficeName, MostRecentHygieneDate, OldestHygieneDate,
        HouseholdLastVisitAnyDate, HouseholdLastVisitAnyOfficeName

  Unscheduled Treatment:
    vw_TxAction_Unscheduled_Current_Scheduler_v2
      → PATID, FNAME, LNAME, AgeYears, FormattedCellPhone, EMAIL,
        OfficeName, TxSummaryFormatted, TotalFee, BookingSlot,
        DiagnosingProvider, PrimaryCarrierName, RiskTag,
        DaysSinceLastPlanActivity, NextApptDate, NextApptType

  Data freshness:
    vw_LastUpdateStamp  → OperationalDate
    vw_DataLastUpdate   → LastDataUTC

NOTE: These views already exist in Azure SQL. This script does NOT
create or alter any views. It only reads from them.

Environment variables:
  AZURE_SQL_CONN_STR — Full ODBC connection string for Denticon Azure SQL
"""

import os
import sys
import json
from datetime import datetime, date
from decimal import Decimal

OUTPUT_FILE  = "data/callsheets.json"

# PSPD Office canonical names (used for display normalization)
OFFICE_CANONICAL = {
    "EVERETT":      "EVERETT",
    "LAKE STEVENS": "LAKE STEVENS",
    "MARYSVILLE":   "MARYSVILLE",
    "MONROE":       "MONROE",
    "STANWOOD":     "STANWOOD",
}


def json_serial(obj):
    """JSON serializer for date/datetime/Decimal objects."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not JSON serializable")


def connect():
    """Connect to Azure SQL Server via ODBC (same as payroll_refresh.py)."""
    try:
        import pyodbc
    except ImportError:
        print("ERROR: pyodbc required. Install with: pip install pyodbc")
        sys.exit(1)

    conn_str = os.environ.get("AZURE_SQL_CONN_STR", "")
    if not conn_str:
        print("ERROR: AZURE_SQL_CONN_STR environment variable not set.")
        sys.exit(1)

    drivers = pyodbc.drivers()
    print(f"  Available ODBC drivers: {drivers}")

    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        print("  ✓ Connected to Azure SQL")
        return conn
    except pyodbc.Error as e:
        print(f"ERROR: Failed to connect to Azure SQL: {e}")
        sys.exit(1)


# ── Recall (Due Next 30 Days) ──────────────────────────────────────────────
# Source: vw_RecallHouseholdDueNext30Days_PBI
# This is the same view used by the "Due Next 30 Days" page in
# PSPD_Recall_CallLists.pbix

def rows_to_dicts(cursor):
    """Convert pyodbc cursor results to list of dicts (like pymssql as_dict=True)."""
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def query_recall(conn):
    """Pull hygiene recall households due in the next 30 days.

    Tries to exclude households that have already completed their
    recall visit by checking for recent appointments in production data.
    """
    cursor = conn.cursor()

    # First try: query with NOT EXISTS against vw_RecallAnchor_LastRecallAppt
    # RecallAnchor has PATID (patient-level), not RPID (household-level).
    # We need to find patient tables to bridge PATID→RPID for the join.
    # If that's too complex, we rely on post-processing filter instead.
    try:
        cursor.execute("SELECT TOP 0 * FROM vw_RecallAnchor_LastRecallAppt")
        anchor_cols = [col[0] for col in cursor.description]
        rpid_col = next((c for c in anchor_cols if 'RPID' in c.upper()), None)
        pid_col = next((c for c in anchor_cols if 'PATID' in c.upper() or
                        (c.upper().startswith('PAT') and 'ID' in c.upper())), None)
        date_col = next((c for c in anchor_cols if 'DATE' in c.upper() or 'RECALL' in c.upper()), None)

        if rpid_col and date_col:
            # Best case: RecallAnchor has RPID directly
            cursor.execute(f"""
                SELECT
                    r.RPID                        AS household_id,
                    r.RPID                        AS household,
                    r.PrimaryPhone                AS phone,
                    r.Email                       AS email,
                    r.KidsDueCount                AS kids_due_count,
                    r.KidsDueList                 AS kids_due_names,
                    r.SuggestedFamilyDate         AS suggest_date,
                    r.LastHygieneOfficeName       AS last_office,
                    r.MostFrequentOfficeName      AS most_frequent_office,
                    r.LastProviderName            AS last_provider
                FROM vw_RecallHouseholdDueNext30Days_PBI r
                WHERE NOT EXISTS (
                    SELECT 1 FROM vw_RecallAnchor_LastRecallAppt a
                    WHERE a.[{rpid_col}] = r.RPID
                      AND a.[{date_col}] >= DATEADD(day, -30, GETDATE())
                )
                ORDER BY r.KidsDueCount DESC, r.SuggestedFamilyDate ASC
            """)
            rows = rows_to_dicts(cursor)
            print(f"  Recall (next 30 days): {len(rows)} households "
                  f"(filtered via RecallAnchor.RPID — excludes recently seen)")
            return rows
        elif pid_col and date_col:
            # RecallAnchor has PATID, not RPID. Use a patient table to bridge.
            # Try to find a table with both PATID and RPID for the subquery join.
            bridge_tbl = None
            bridge_pid = None
            bridge_rpid = None
            for tbl in ['PATHDR', 'PATD', 'Patient', 'PAT']:
                try:
                    cursor.execute(f"SELECT TOP 0 * FROM {tbl}")
                    tcols = [c[0] for c in cursor.description]
                    t_rpid = next((c for c in tcols if 'RPID' in c.upper()), None)
                    t_pid = next((c for c in tcols if c.upper() == 'PATID'), None)
                    if not t_pid:
                        t_pid = next((c for c in tcols if 'PAT' in c.upper() and 'ID' in c.upper()), None)
                    if t_rpid and t_pid:
                        bridge_tbl = tbl
                        bridge_pid = t_pid
                        bridge_rpid = t_rpid
                        print(f"  Found PATID→RPID bridge: {tbl}.{t_pid} → {tbl}.{t_rpid}")
                        break
                except Exception:
                    continue

            if bridge_tbl:
                cursor.execute(f"""
                    SELECT
                        r.RPID                        AS household_id,
                        r.RPID                        AS household,
                        r.PrimaryPhone                AS phone,
                        r.Email                       AS email,
                        r.KidsDueCount                AS kids_due_count,
                        r.KidsDueList                 AS kids_due_names,
                        r.SuggestedFamilyDate         AS suggest_date,
                        r.LastHygieneOfficeName       AS last_office,
                        r.MostFrequentOfficeName      AS most_frequent_office,
                        r.LastProviderName            AS last_provider
                    FROM vw_RecallHouseholdDueNext30Days_PBI r
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM vw_RecallAnchor_LastRecallAppt a
                        JOIN {bridge_tbl} p ON p.[{bridge_pid}] = a.[{pid_col}]
                        WHERE p.[{bridge_rpid}] = r.RPID
                          AND a.[{date_col}] >= DATEADD(day, -30, GETDATE())
                    )
                    ORDER BY r.KidsDueCount DESC, r.SuggestedFamilyDate ASC
                """)
                rows = rows_to_dicts(cursor)
                print(f"  Recall (next 30 days): {len(rows)} households "
                      f"(filtered via RecallAnchor.PATID→{bridge_tbl}.RPID)")
                return rows
            else:
                print(f"  ⚠ RecallAnchor has PATID but no bridge table found — falling back")
        else:
            print(f"  ⚠ RecallAnchor columns not suitable: {anchor_cols}")
    except Exception as e:
        print(f"  ⚠ Recall filtered query failed ({e}), falling back to unfiltered...")

    # Fallback: original unfiltered query (post-processing will still catch some)
    try:
        cursor.execute("""
            SELECT
                RPID                        AS household_id,
                RPID                        AS household,
                PrimaryPhone                AS phone,
                Email                       AS email,
                KidsDueCount                AS kids_due_count,
                KidsDueList                 AS kids_due_names,
                SuggestedFamilyDate         AS suggest_date,
                LastHygieneOfficeName       AS last_office,
                MostFrequentOfficeName      AS most_frequent_office,
                LastProviderName            AS last_provider
            FROM vw_RecallHouseholdDueNext30Days_PBI
            ORDER BY KidsDueCount DESC, SuggestedFamilyDate ASC
        """)
        rows = rows_to_dicts(cursor)
        print(f"  Recall (next 30 days): {len(rows)} households (UNFILTERED — will post-process)")
        return rows
    except Exception as e:
        print(f"  ✗ Recall query failed: {e}")
        return []


# ── Overdue Recall ──────────────────────────────────────────────────────────
# Source: vw_RecallHouseholdDue
# This is the same view used by the "Household Due" page in
# PSPD_Recall_CallLists.pbix

def query_overdue(conn):
    """Pull overdue recall households (past due for hygiene)."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT
                RPID                              AS household_id,
                RPID                              AS household,
                PrimaryPhone                      AS phone,
                Email                             AS email,
                KidsDueCount                      AS kids_due_count,
                KidsDueList                       AS kids_due_names,
                LastHygieneOfficeName             AS last_office,
                MostRecentHygieneDate             AS most_recent_hygiene,
                OldestHygieneDate                 AS oldest_hygiene,
                HouseholdLastVisitAnyDate         AS last_visit_date,
                HouseholdLastVisitAnyOfficeName   AS last_visit_office,
                -- Calculate days overdue from oldest hygiene date
                -- Standard recall interval is ~6 months (180 days)
                CASE
                    WHEN OldestHygieneDate IS NOT NULL
                    THEN DATEDIFF(day, OldestHygieneDate, GETDATE())
                    ELSE NULL
                END                               AS days_overdue
            FROM vw_RecallHouseholdDue
            WHERE HouseholdLastVisitAnyDate >= DATEADD(year, -1, GETDATE())
              -- Exclude households whose most recent hygiene is within the
              -- last 30 days — they've already been seen and are NOT overdue
              AND (MostRecentHygieneDate IS NULL
                   OR MostRecentHygieneDate < DATEADD(day, -30, GETDATE()))
            ORDER BY OldestHygieneDate ASC, KidsDueCount DESC
        """)
        rows = rows_to_dicts(cursor)
        print(f"  Overdue: {len(rows)} households")
        return rows
    except Exception as e:
        print(f"  ✗ Overdue query failed: {e}")
        return []


# ── Unscheduled Treatment ──────────────────────────────────────────────────
# Source: vw_TxAction_Unscheduled_Current_Scheduler_v2
# This is the same view used by Treatment - Unscheduled.pbix

def query_treatment(conn):
    """Pull unscheduled treatment patients.

    Note: RiskTag column was removed — it does not exist in the view.
    The view columns can be checked with:
      SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = 'vw_TxAction_Unscheduled_Current_Scheduler_v2'
    """
    cursor = conn.cursor()

    # First, try to discover available columns so we can include risk if it exists
    risk_col = "NULL AS caries_risk"
    try:
        cursor.execute("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'vw_TxAction_Unscheduled_Current_Scheduler_v2'
              AND COLUMN_NAME IN ('RiskTag', 'CariesRisk', 'caries_risk', 'Risk', 'RiskLevel')
        """)
        risk_rows = [row[0] for row in cursor.fetchall()]
        if risk_rows:
            risk_col = f"{risk_rows[0]} AS caries_risk"
            print(f"  Found risk column: {risk_rows[0]}")
        else:
            print(f"  No risk column found in treatment view — will use NULL")
    except Exception:
        pass

    try:
        cursor.execute(f"""
            SELECT
                PATID                       AS patient_id,
                FNAME                       AS fname,
                LNAME                       AS lname,
                CONCAT(LNAME, ', ', FNAME)  AS name,
                AgeYears                    AS age,
                FormattedCellPhone          AS phone,
                EMAIL                       AS email,
                OfficeName                  AS last_office,
                TxSummaryFormatted          AS tx_summary,
                TotalFee                    AS total_fee,
                BookingSlot                 AS booking_slot,
                DiagnosingProvider          AS diagnosing_provider,
                PrimaryCarrierName          AS insurance,
                {risk_col},
                DaysSinceLastPlanActivity   AS days_since_plan,
                NextApptDate                AS next_appt_date,
                NextApptType                AS next_appt_type
            FROM vw_TxAction_Unscheduled_Current_Scheduler_v2
            ORDER BY TotalFee DESC
        """)
        rows = rows_to_dicts(cursor)
        print(f"  Treatment: {len(rows)} patients")
        return rows
    except Exception as e:
        print(f"  ✗ Treatment query failed: {e}")
        # Fallback: try minimal query without problematic columns
        try:
            print(f"  Retrying with minimal columns...")
            cursor.execute("""
                SELECT
                    PATID                       AS patient_id,
                    FNAME                       AS fname,
                    LNAME                       AS lname,
                    CONCAT(LNAME, ', ', FNAME)  AS name,
                    AgeYears                    AS age,
                    FormattedCellPhone          AS phone,
                    EMAIL                       AS email,
                    OfficeName                  AS last_office,
                    TotalFee                    AS total_fee,
                    BookingSlot                 AS booking_slot,
                    NULL                        AS caries_risk
                FROM vw_TxAction_Unscheduled_Current_Scheduler_v2
                ORDER BY TotalFee DESC
            """)
            rows = rows_to_dicts(cursor)
            print(f"  Treatment (minimal): {len(rows)} patients")
            return rows
        except Exception as e2:
            print(f"  ✗ Treatment fallback also failed: {e2}")
            return []


# ── Data Freshness ──────────────────────────────────────────────────────────
# Check when the underlying data was last refreshed

def query_data_freshness(conn):
    """Get the last data refresh timestamp from the update stamp views."""
    cursor = conn.cursor()
    timestamps = {}

    # Try vw_LastUpdateStamp (used by Recall model)
    try:
        cursor.execute("SELECT TOP 1 OperationalDate FROM vw_LastUpdateStamp")
        row = cursor.fetchone()
        if row:
            timestamps['recall_as_of'] = row[0]
            print(f"  Recall data as of: {row[0]}")
    except Exception:
        pass

    # Try vw_DataLastUpdate (used by Treatment model)
    try:
        cursor.execute("SELECT TOP 1 LastDataUTC FROM vw_DataLastUpdate")
        row = cursor.fetchone()
        if row:
            timestamps['treatment_as_of'] = row[0]
            print(f"  Treatment data as of: {row[0]}")
    except Exception:
        pass

    return timestamps


# ── Contact Log & Z-Codes ──────────────────────────────────────────────────
# These are custom tables maintained by the call sheet system itself,
# not from Denticon views. They may not exist on first run.

def query_contact_log(conn):
    """Pull SMS/contact log from the callsheet_log table (if it exists)."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT row_id, phone, name, type, status, date, message, error
            FROM rpt.callsheet_contact_log
            WHERE date >= DATEADD(day, -30, GETDATE())
            ORDER BY date DESC
        """)
        rows = rows_to_dicts(cursor)
        print(f"  Contact log: {len(rows)} entries (last 30 days)")
        return rows
    except Exception:
        print("  ⚠ No contact log table found (rpt.callsheet_contact_log)")
        print("    Contact history will be tracked in-browser until table is created.")
        return []


def query_bad_phones(conn):
    """Pull known bad phone numbers from the contact log."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT phone
            FROM rpt.callsheet_contact_log
            WHERE status IN ('failed', 'undelivered')
              AND (error LIKE '%211%' OR error LIKE '%614%' OR error LIKE '%608%')
        """)
        rows = rows_to_dicts(cursor)
        phones = [r['phone'] for r in rows if r.get('phone')]
        print(f"  Bad phones: {len(phones)} numbers flagged")
        return phones
    except Exception:
        print("  ⚠ Could not query bad phones (no log table)")
        return []


def query_zcode_status(conn):
    """Check for Z-code entries marking patients as 'contacted this cycle'.
    Z-codes are typically posted as procedures in the Denticon ledger."""
    cursor = conn.cursor()
    queries = [
        # Pattern 1: Ledger entry with Z-code procedure
        """SELECT DISTINCT patient_id, MAX(service_date) AS z_code_date
           FROM dbo.ledger
           WHERE procedure_code LIKE 'Z%'
             AND service_date >= DATEADD(day, -90, GETDATE())
           GROUP BY patient_id""",
        # Pattern 2: Custom tracking table
        """SELECT DISTINCT patient_id, MAX(contact_date) AS z_code_date
           FROM rpt.callsheet_zcode_log
           WHERE contact_date >= DATEADD(day, -90, GETDATE())
           GROUP BY patient_id""",
    ]

    for i, sql in enumerate(queries):
        try:
            cursor.execute(sql)
            rows = rows_to_dicts(cursor)
            if rows:
                print(f"  Z-codes: {len(rows)} patients contacted (pattern {i+1})")
                return {r['patient_id']: r.get('z_code_date', '') for r in rows}
        except Exception:
            continue

    print("  ⚠ No Z-code data found (table may not exist yet)")
    return {}


# ── Doctor Leaderboard KPIs ────────────────────────────────────────────────
# Aggregates production and treatment metrics per doctor for the
# leaderboard dashboard. Does NOT disclose pay/compensation.

def query_doctor_leaderboard(conn, col_map=None):
    """Build doctor-level KPIs from treatment and production views.
    col_map: optional dict mapping logical names → actual column names."""
    cursor = conn.cursor()
    leaderboard = {}

    # KPI 1: Unscheduled treatment count + value per diagnosing doctor
    try:
        cursor.execute("""
            SELECT
                DiagnosingProvider          AS provider,
                COUNT(DISTINCT PATID)       AS unscheduled_patients,
                SUM(TotalFee)               AS unscheduled_value,
                AVG(DaysSinceLastPlanActivity) AS avg_days_pending
            FROM vw_TxAction_Unscheduled_Current_Scheduler_v2
            WHERE DiagnosingProvider IS NOT NULL
              AND DiagnosingProvider <> ''
            GROUP BY DiagnosingProvider
            ORDER BY SUM(TotalFee) DESC
        """)
        for row in rows_to_dicts(cursor):
            prov = str(row.get('provider', '')).strip()
            if not prov:
                continue
            leaderboard[prov] = {
                'provider': prov,
                'unscheduled_patients': int(row.get('unscheduled_patients', 0)),
                'unscheduled_value': round(float(row.get('unscheduled_value', 0) or 0), 2),
                'avg_days_pending': round(float(row.get('avg_days_pending', 0) or 0), 0),
            }
        print(f"  Leaderboard: {len(leaderboard)} providers with unscheduled tx")
    except Exception as e:
        print(f"  ⚠ Unscheduled tx aggregation failed: {e}")

    # KPI 2: Try to get production data (completed procedures)
    # Discover production-related views first
    production_views = []
    try:
        cursor.execute("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_NAME LIKE '%prod%'
               OR TABLE_NAME LIKE '%income%'
               OR TABLE_NAME LIKE '%ledger%'
               OR TABLE_NAME LIKE '%complete%'
            ORDER BY TABLE_NAME
        """)
        production_views = [row[0] for row in cursor.fetchall()]
        print(f"  Discovered production views: {production_views}")
    except Exception as e:
        print(f"  ⚠ View discovery failed: {e}")

    # KPI 3: Try vw_income_allocation (same view used by payroll)
    # Use col_map if available — provider may be numeric ID, not a string name
    prov_col = col_map.get('provider_name') or col_map.get('provider_id') if col_map else None
    if col_map and col_map.get('service_date') and prov_col:
        svc = col_map['service_date']
        prov_is_id = col_map.get('_provider_is_id', False)
        provider_names = col_map.get('_provider_names', {})
        gross_col = col_map.get('gross_production')
        net_col = col_map.get('net_production')
        coll_col = col_map.get('net_collections')
        pid_col = col_map.get('patient_id')

        try:
            select_parts = [f"[{prov_col}] AS provider"]
            if gross_col: select_parts.append(f"SUM(CAST([{gross_col}] AS DECIMAL(18,2))) AS mtd_production")
            if coll_col: select_parts.append(f"SUM(CAST([{coll_col}] AS DECIMAL(18,2))) AS mtd_collections")
            if pid_col: select_parts.append(f"COUNT(DISTINCT [{pid_col}]) AS patients_seen")

            sort = f"SUM(CAST([{gross_col}] AS DECIMAL(18,2)))" if gross_col else f"[{prov_col}]"

            # No string comparison (<> '') on numeric provider IDs
            cursor.execute(f"""
                SELECT {', '.join(select_parts)}
                FROM rpt.vw_income_allocation
                WHERE [{svc}] >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                  AND [{prov_col}] IS NOT NULL
                GROUP BY [{prov_col}]
                ORDER BY {sort} DESC
            """)
            for row in rows_to_dicts(cursor):
                raw_prov = row.get('provider')
                if raw_prov is None:
                    continue
                # Resolve provider ID to name if needed
                prov_key = str(int(raw_prov)) if isinstance(raw_prov, (int, float)) else str(raw_prov).strip()
                if prov_is_id and prov_key in provider_names:
                    prov_display = provider_names[prov_key]
                else:
                    prov_display = prov_key
                if not prov_display:
                    continue
                if prov_display not in leaderboard:
                    leaderboard[prov_display] = {'provider': prov_display}
                leaderboard[prov_display]['mtd_production'] = round(float(row.get('mtd_production', 0) or 0), 2)
                leaderboard[prov_display]['mtd_collections'] = round(float(row.get('mtd_collections', 0) or 0), 2)
                leaderboard[prov_display]['patients_seen_mtd'] = int(row.get('patients_seen', 0))
            print(f"  MTD production data loaded for {len(leaderboard)} providers")
        except Exception as e:
            print(f"  ⚠ MTD production query failed: {e}")
    else:
        print(f"  ⚠ No column mapping for income allocation — skipping MTD production KPIs")

    # KPI 4: Try to get treatment plan conversion (tx planned vs completed)
    try:
        cursor.execute("""
            SELECT
                DiagnosingProvider          AS provider,
                COUNT(DISTINCT PATID)       AS total_planned,
                SUM(CASE WHEN NextApptDate IS NOT NULL THEN 1 ELSE 0 END) AS scheduled_count
            FROM vw_TxAction_Unscheduled_Current_Scheduler_v2
            WHERE DiagnosingProvider IS NOT NULL
              AND DiagnosingProvider <> ''
            GROUP BY DiagnosingProvider
        """)
        for row in rows_to_dicts(cursor):
            prov = str(row.get('provider', '')).strip()
            if not prov or prov not in leaderboard:
                continue
            total = int(row.get('total_planned', 0))
            scheduled = int(row.get('scheduled_count', 0))
            leaderboard[prov]['total_tx_planned'] = total
            leaderboard[prov]['tx_scheduled'] = scheduled
            leaderboard[prov]['scheduling_rate'] = round(scheduled / total * 100, 1) if total > 0 else 0
        print(f"  Treatment scheduling rates loaded")
    except Exception as e:
        print(f"  ⚠ Tx conversion query failed: {e}")

    return {
        'providers': list(leaderboard.values()),
        'discovered_views': production_views,
    }


# ── Income Allocation Column Discovery ──────────────────────────────────────
# rpt.vw_income_allocation uses different column names than expected.
# This discovers actual column names and builds a mapping.

def _discover_office_names(conn):
    """Build a lookup dict mapping OID → office name."""
    cursor = conn.cursor()
    office_map = {}
    # Try common Denticon office/practice tables
    for tbl in ['OFFICEH', 'OFFICE', 'PRACTICEH', 'PRACTICE', 'BI_Office']:
        try:
            cursor.execute(f"SELECT TOP 0 * FROM {tbl}")
            tcols = [c[0] for c in cursor.description]
            oid_col = next((c for c in tcols if c.upper() == 'OID'), None)
            if not oid_col:
                oid_col = next((c for c in tcols if c.upper() in ('OFFICEID', 'ID', 'PGID')), None)
            name_col = next((c for c in tcols if 'NAME' in c.upper() and 'NICK' not in c.upper()), None)
            if not name_col:
                name_col = next((c for c in tcols if 'NAME' in c.upper()), None)
            if oid_col and name_col:
                cursor.execute(f"SELECT [{oid_col}], [{name_col}] FROM {tbl}")
                for row in cursor.fetchall():
                    if row[0] is not None and row[1]:
                        office_map[str(int(row[0])) if isinstance(row[0], (int, float)) else str(row[0])] = str(row[1]).strip()
                if office_map:
                    print(f"  Office name lookup: {len(office_map)} offices from {tbl} ({office_map})")
                    return office_map
        except Exception:
            continue
    print(f"  ⚠ No office name lookup table found")
    return office_map


def _discover_provider_names(conn):
    """Build a lookup dict mapping provider_id → provider name."""
    cursor = conn.cursor()
    prov_map = {}
    for tbl in ['PROVIDERH', 'PROVIDER', 'BI_Provider']:
        try:
            cursor.execute(f"SELECT TOP 0 * FROM {tbl}")
            tcols = [c[0] for c in cursor.description]
            pid_col = next((c for c in tcols if c.upper() in ('PROVIDERID', 'PGID', 'ID')), None)
            # Try to find name columns
            fname_col = next((c for c in tcols if c.upper() in ('FNAME', 'FIRSTNAME')), None)
            lname_col = next((c for c in tcols if c.upper() in ('LNAME', 'LASTNAME')), None)
            name_col = next((c for c in tcols if c.upper() in ('PROVIDERNAME', 'NAME', 'DISPLAYNAME')), None)
            if pid_col and (name_col or (fname_col and lname_col)):
                if name_col:
                    cursor.execute(f"SELECT [{pid_col}], [{name_col}] FROM {tbl}")
                    for row in cursor.fetchall():
                        if row[0] is not None and row[1]:
                            prov_map[str(int(row[0])) if isinstance(row[0], (int, float)) else str(row[0])] = str(row[1]).strip()
                else:
                    cursor.execute(f"SELECT [{pid_col}], [{lname_col}], [{fname_col}] FROM {tbl}")
                    for row in cursor.fetchall():
                        if row[0] is not None and (row[1] or row[2]):
                            lname = str(row[1] or '').strip()
                            fname = str(row[2] or '').strip()
                            prov_map[str(int(row[0])) if isinstance(row[0], (int, float)) else str(row[0])] = f"{lname}, {fname}" if lname else fname
                if prov_map:
                    print(f"  Provider name lookup: {len(prov_map)} providers from {tbl}")
                    return prov_map
        except Exception:
            continue
    print(f"  ⚠ No provider name lookup table found")
    return prov_map


def discover_income_allocation_columns(conn):
    """Discover actual column names in rpt.vw_income_allocation.
    Returns a dict mapping logical names to actual column names, or None if view unavailable.
    Also discovers office and provider name lookup tables."""
    cursor = conn.cursor()

    # First try views that might have human-readable provider/office names
    preferred_views = [
        'rpt.vw_doctor_production_with_provider_office',
        'rpt.vw_income_allocation_by_office',
    ]
    preferred_view = None
    preferred_cols = None
    for pv in preferred_views:
        try:
            cursor.execute(f"SELECT TOP 0 * FROM {pv}")
            preferred_cols = [col[0] for col in cursor.description]
            preferred_view = pv
            print(f"  Found preferred view: {pv} with columns: {preferred_cols}")
            break
        except Exception:
            continue

    try:
        cursor.execute("SELECT TOP 0 * FROM rpt.vw_income_allocation")
        cols = [col[0] for col in cursor.description]
        print(f"  Income allocation columns: {cols}")

        # Build mapping from expected names to actual discovered columns
        col_map = {}
        upper_cols = {c.upper(): c for c in cols}

        def find_col(*patterns):
            """Find first column matching any pattern (case-insensitive exact or substring)."""
            for pat in patterns:
                pat_upper = pat.upper()
                if pat_upper in upper_cols:
                    return upper_cols[pat_upper]
                for uc, orig in upper_cols.items():
                    if pat_upper in uc:
                        return orig
            return None

        # Date column — proc_dos_date is the procedure date of service
        col_map['service_date'] = find_col('proc_dos_date', 'service_date', 'ServiceDate',
                                            'TransDate', 'trans_date', 'ProcDate', 'proc_date',
                                            'EntryDate', 'entry_date', 'DOS', 'DateOfService')

        # Office — OID is the numeric office ID in this view
        col_map['office_id'] = find_col('OID')
        col_map['office_name'] = find_col('office_name', 'OfficeName', 'LocationName', 'FacilityName')

        # Production amounts — proc_amount is the procedure charge amount
        col_map['gross_production'] = find_col('proc_amount', 'gross_production', 'GrossProduction',
                                                'GrossProd', 'gross_prod', 'TotalFee', 'total_fee',
                                                'Fee', 'GrossCharges', 'Production')

        # Net production — may not exist; can derive from gross - adjustments
        col_map['net_production'] = find_col('net_production', 'NetProduction', 'NetProd',
                                              'net_prod', 'NetFee', 'AdjustedProduction')

        # Collections — alloc_amount is the payment allocation amount
        col_map['net_collections'] = find_col('alloc_amount', 'net_collections', 'NetCollections',
                                               'Collections', 'net_collect', 'Payments', 'NetPayments')

        # Adjustments
        col_map['adjustments'] = find_col('adjustments', 'Adjustments', 'Adjustment',
                                           'TotalAdj', 'adj', 'WriteOff')

        # Patient ID
        col_map['patient_id'] = find_col('PATID', 'patient_id', 'PatientID', 'PatID', 'pat_id')

        # Provider — alloc_provider_id and proc_provider_id are numeric IDs
        col_map['provider_id'] = find_col('proc_provider_id', 'alloc_provider_id',
                                           'PROVIDERID', 'provider_id')
        col_map['provider_name'] = find_col('provider_name', 'ProviderName', 'DoctorName')
        # Flag: is provider a numeric ID (not a name string)?
        col_map['_provider_is_id'] = col_map.get('provider_id') is not None and col_map.get('provider_name') is None

        # If preferred view has better columns, check for office/provider names there
        if preferred_view and preferred_cols:
            pref_upper = {c.upper(): c for c in preferred_cols}
            # Check for office name
            for pat in ['OFFICENAME', 'OFFICE_NAME', 'OFFICE']:
                if pat in pref_upper and not col_map.get('office_name'):
                    col_map['_preferred_view'] = preferred_view
                    col_map['_preferred_office_col'] = pref_upper[pat]
                    break
            # Check for provider name
            for pat in ['PROVIDERNAME', 'PROVIDER_NAME', 'PROVIDER', 'DOCTORNAME']:
                if pat in pref_upper and not col_map.get('provider_name'):
                    col_map['_preferred_provider_col'] = pref_upper.get(pat)
                    break

        # Discover office and provider name lookup tables
        col_map['_office_names'] = _discover_office_names(conn)
        col_map['_provider_names'] = _discover_provider_names(conn)

        # Log what we found
        public_map = {k: v for k, v in col_map.items() if v and not k.startswith('_')}
        missing = [k for k in ['service_date', 'office_id', 'gross_production', 'net_collections',
                                'patient_id', 'provider_id'] if not col_map.get(k)]
        print(f"  Column mapping: {public_map}")
        if col_map.get('_provider_is_id'):
            print(f"  Provider column is numeric ID (will use lookup table)")
        if col_map.get('office_id') and not col_map.get('office_name'):
            print(f"  Office column is numeric ID (OID, will use lookup table)")
        if missing:
            print(f"  ⚠ Missing mappings: {missing}")

        return col_map if col_map.get('service_date') else None

    except Exception as e:
        print(f"  ⚠ Cannot access rpt.vw_income_allocation: {e}")
        return None


# ── Daily Production by Location ────────────────────────────────────────────
# Provides daily Gross/Net production by office for the production dashboard.
# Uses rpt.vw_income_allocation with auto-discovered column names.

def query_daily_production(conn, col_map=None):
    """Pull daily production by office for current month + prior month.
    col_map: dict mapping logical names → actual column names in rpt.vw_income_allocation."""
    cursor = conn.cursor()
    result = {
        'by_date': [],
        'by_office_daily': [],
        'by_provider_mtd': [],
        'monthly_summary': [],
    }

    if not col_map or not col_map.get('service_date'):
        print(f"  ⚠ No column mapping for rpt.vw_income_allocation — skipping production queries")
        return result

    # Shorthand — use provider_id (numeric) if provider_name (string) unavailable
    svc = col_map['service_date']
    ofc_name = col_map.get('office_name')     # String office name (may be None)
    ofc_id = col_map.get('office_id')          # Numeric OID (fallback)
    ofc = ofc_name or ofc_id                   # Best available office column
    ofc_is_id = ofc == ofc_id and not ofc_name  # True if office column is numeric ID
    gross = col_map.get('gross_production')
    net = col_map.get('net_production')
    coll = col_map.get('net_collections')
    adj = col_map.get('adjustments')
    pid = col_map.get('patient_id')
    prov = col_map.get('provider_name') or col_map.get('provider_id')
    prov_is_id = col_map.get('_provider_is_id', False)
    office_names = col_map.get('_office_names', {})
    provider_names = col_map.get('_provider_names', {})

    def resolve_office(val):
        """Convert office ID or name to normalized office name."""
        if val is None:
            return ''
        s = str(int(val)) if isinstance(val, (int, float)) else str(val).strip()
        if ofc_is_id and s in office_names:
            return normalize_office(office_names[s])
        return normalize_office(s)

    def resolve_provider(val):
        """Convert provider ID to name if needed."""
        if val is None:
            return ''
        s = str(int(val)) if isinstance(val, (int, float)) else str(val).strip()
        if prov_is_id and s in provider_names:
            return provider_names[s]
        return s

    # Daily production by office — current month
    if ofc:
        try:
            select_parts = [f"CAST([{svc}] AS DATE) AS prod_date", f"[{ofc}] AS office"]
            if gross: select_parts.append(f"SUM(CAST([{gross}] AS DECIMAL(18,2))) AS gross_production")
            if net: select_parts.append(f"SUM(CAST([{net}] AS DECIMAL(18,2))) AS net_production")
            if coll: select_parts.append(f"SUM(CAST([{coll}] AS DECIMAL(18,2))) AS net_collections")
            if adj: select_parts.append(f"SUM(CAST([{adj}] AS DECIMAL(18,2))) AS adjustments")
            if pid: select_parts.append(f"COUNT(DISTINCT [{pid}]) AS patients_seen")

            cursor.execute(f"""
                SELECT {', '.join(select_parts)}
                FROM rpt.vw_income_allocation
                WHERE [{svc}] >= DATEADD(month, -1, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))
                  AND [{ofc}] IS NOT NULL
                GROUP BY CAST([{svc}] AS DATE), [{ofc}]
                ORDER BY CAST([{svc}] AS DATE), [{ofc}]
            """)
            for row in rows_to_dicts(cursor):
                result['by_office_daily'].append({
                    'date': str(row.get('prod_date', '')),
                    'office': resolve_office(row.get('office')),
                    'gross': round(float(row.get('gross_production', 0) or 0), 2),
                    'net': round(float(row.get('net_production', 0) or 0), 2),
                    'collections': round(float(row.get('net_collections', 0) or 0), 2),
                    'adjustments': round(float(row.get('adjustments', 0) or 0), 2),
                    'patients': int(row.get('patients_seen', 0)),
                })
            print(f"  Daily production: {len(result['by_office_daily'])} office-day records")
        except Exception as e:
            print(f"  ⚠ Daily production by office failed: {e}")

    # Daily production totals (all offices combined)
    try:
        select_parts = [f"CAST([{svc}] AS DATE) AS prod_date"]
        if gross: select_parts.append(f"SUM(CAST([{gross}] AS DECIMAL(18,2))) AS gross_production")
        if net: select_parts.append(f"SUM(CAST([{net}] AS DECIMAL(18,2))) AS net_production")
        if coll: select_parts.append(f"SUM(CAST([{coll}] AS DECIMAL(18,2))) AS net_collections")
        if adj: select_parts.append(f"SUM(CAST([{adj}] AS DECIMAL(18,2))) AS adjustments")
        if pid: select_parts.append(f"COUNT(DISTINCT [{pid}]) AS patients_seen")
        if prov: select_parts.append(f"COUNT(DISTINCT [{prov}]) AS providers_active")

        # For numeric provider IDs, don't compare with empty string
        where_extra = ""
        if prov:
            where_extra = f" AND [{prov}] IS NOT NULL"

        cursor.execute(f"""
            SELECT {', '.join(select_parts)}
            FROM rpt.vw_income_allocation
            WHERE [{svc}] >= DATEADD(month, -1, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))
              {where_extra}
            GROUP BY CAST([{svc}] AS DATE)
            ORDER BY CAST([{svc}] AS DATE)
        """)
        for row in rows_to_dicts(cursor):
            result['by_date'].append({
                'date': str(row.get('prod_date', '')),
                'gross': round(float(row.get('gross_production', 0) or 0), 2),
                'net': round(float(row.get('net_production', 0) or 0), 2),
                'collections': round(float(row.get('net_collections', 0) or 0), 2),
                'adjustments': round(float(row.get('adjustments', 0) or 0), 2),
                'patients': int(row.get('patients_seen', 0)),
                'providers': int(row.get('providers_active', 0)),
            })
        print(f"  Daily totals: {len(result['by_date'])} days")
    except Exception as e:
        print(f"  ⚠ Daily production totals failed: {e}")

    # MTD by provider (for provider ranking)
    if prov:
        try:
            select_parts = [f"[{prov}] AS provider"]
            if ofc: select_parts.append(f"[{ofc}] AS office")
            if gross: select_parts.append(f"SUM(CAST([{gross}] AS DECIMAL(18,2))) AS gross_production")
            if net: select_parts.append(f"SUM(CAST([{net}] AS DECIMAL(18,2))) AS net_production")
            if coll: select_parts.append(f"SUM(CAST([{coll}] AS DECIMAL(18,2))) AS net_collections")
            if adj: select_parts.append(f"SUM(CAST([{adj}] AS DECIMAL(18,2))) AS adjustments")
            if pid: select_parts.append(f"COUNT(DISTINCT [{pid}]) AS patients_seen")
            select_parts.append(f"COUNT(DISTINCT CAST([{svc}] AS DATE)) AS days_worked")

            group_parts = [f"[{prov}]"]
            if ofc: group_parts.append(f"[{ofc}]")

            sort_col = f"SUM(CAST([{gross}] AS DECIMAL(18,2)))" if gross else f"[{prov}]"

            # No string comparison on numeric columns
            cursor.execute(f"""
                SELECT {', '.join(select_parts)}
                FROM rpt.vw_income_allocation
                WHERE [{svc}] >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                  AND [{prov}] IS NOT NULL
                GROUP BY {', '.join(group_parts)}
                ORDER BY {sort_col} DESC
            """)
            for row in rows_to_dicts(cursor):
                result['by_provider_mtd'].append({
                    'provider': resolve_provider(row.get('provider')),
                    'office': resolve_office(row.get('office')),
                    'gross': round(float(row.get('gross_production', 0) or 0), 2),
                    'net': round(float(row.get('net_production', 0) or 0), 2),
                    'collections': round(float(row.get('net_collections', 0) or 0), 2),
                    'adjustments': round(float(row.get('adjustments', 0) or 0), 2),
                    'patients': int(row.get('patients_seen', 0)),
                    'days_worked': int(row.get('days_worked', 0)),
                })
            print(f"  MTD by provider: {len(result['by_provider_mtd'])} provider-office combos")
        except Exception as e:
            print(f"  ⚠ MTD by provider failed: {e}")

    # Monthly summary (last 12 months)
    try:
        select_parts = [f"YEAR([{svc}]) AS yr", f"MONTH([{svc}]) AS mo"]
        if gross: select_parts.append(f"SUM(CAST([{gross}] AS DECIMAL(18,2))) AS gross_production")
        if net: select_parts.append(f"SUM(CAST([{net}] AS DECIMAL(18,2))) AS net_production")
        if coll: select_parts.append(f"SUM(CAST([{coll}] AS DECIMAL(18,2))) AS net_collections")
        if adj: select_parts.append(f"SUM(CAST([{adj}] AS DECIMAL(18,2))) AS adjustments")
        if pid: select_parts.append(f"COUNT(DISTINCT [{pid}]) AS patients_seen")

        cursor.execute(f"""
            SELECT {', '.join(select_parts)}
            FROM rpt.vw_income_allocation
            WHERE [{svc}] >= DATEADD(month, -12, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))
            GROUP BY YEAR([{svc}]), MONTH([{svc}])
            ORDER BY YEAR([{svc}]), MONTH([{svc}])
        """)
        month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        for row in rows_to_dicts(cursor):
            yr = int(row.get('yr', 0))
            mo = int(row.get('mo', 0))
            result['monthly_summary'].append({
                'year': yr,
                'month': mo,
                'label': f"{month_names[mo]} {yr}" if 1 <= mo <= 12 else f"{mo}/{yr}",
                'gross': round(float(row.get('gross_production', 0) or 0), 2),
                'net': round(float(row.get('net_production', 0) or 0), 2),
                'collections': round(float(row.get('net_collections', 0) or 0), 2),
                'adjustments': round(float(row.get('adjustments', 0) or 0), 2),
                'patients': int(row.get('patients_seen', 0)),
            })
        print(f"  Monthly summary: {len(result['monthly_summary'])} months")
    except Exception as e:
        print(f"  ⚠ Monthly summary failed: {e}")

    return result


# ── Production by Patient Zip Code ──────────────────────────────────────────
# Aggregates production by patient zip code for geographic heatmap visualization.

def query_production_by_zip(conn, col_map=None):
    """Pull production aggregated by patient zip code for current month.
    col_map: dict mapping logical names → actual column names."""
    cursor = conn.cursor()
    result = []

    if not col_map or not col_map.get('service_date') or not col_map.get('patient_id'):
        print(f"  ⚠ No column mapping — skipping production by zip")
        return result

    svc = col_map['service_date']
    pid = col_map['patient_id']
    net = col_map.get('net_production')
    gross = col_map.get('gross_production')
    ofc = col_map.get('office_name') or col_map.get('office_id')
    prov = col_map.get('provider_name') or col_map.get('provider_id')

    # Try to find a patient table with Zip data
    patient_tables = ['PATHDR', 'PATD', 'Patient', 'PAT']
    pat_tbl = None
    pat_pid_col = None
    for tbl in patient_tables:
        try:
            cursor.execute(f"SELECT TOP 0 * FROM {tbl}")
            tcols = [c[0] for c in cursor.description]
            zip_col = next((c for c in tcols if 'ZIP' in c.upper()), None)
            t_pid = next((c for c in tcols if c.upper() == 'PATID'), None)
            if not t_pid:
                t_pid = next((c for c in tcols if 'PAT' in c.upper() and 'ID' in c.upper()), None)
            if zip_col and t_pid:
                pat_tbl = tbl
                pat_pid_col = t_pid
                print(f"  Found patient zip table: {tbl} (ZIP={zip_col}, PATID={t_pid})")
                break
        except Exception:
            continue

    if not pat_tbl:
        print(f"  ⚠ No patient table with Zip data found — skipping production by zip")
        return result

    try:
        select_parts = [
            f"LEFT(LTRIM(RTRIM(p.[{zip_col}])), 5) AS zip_code",
        ]
        # Try City/State columns
        city_col = next((c for c in tcols if 'CITY' in c.upper()), None)
        state_col = next((c for c in tcols if 'STATE' in c.upper()), None)
        if city_col: select_parts.append(f"p.[{city_col}] AS city")
        if state_col: select_parts.append(f"p.[{state_col}] AS state")
        if ofc: select_parts.append(f"ia.[{ofc}] AS office")
        if net: select_parts.append(f"SUM(CAST(ia.[{net}] AS DECIMAL(18,2))) AS net_production")
        if gross: select_parts.append(f"SUM(CAST(ia.[{gross}] AS DECIMAL(18,2))) AS gross_production")
        select_parts.append(f"COUNT(DISTINCT ia.[{pid}]) AS patient_count")

        group_parts = [f"LEFT(LTRIM(RTRIM(p.[{zip_col}])), 5)"]
        if city_col: group_parts.append(f"p.[{city_col}]")
        if state_col: group_parts.append(f"p.[{state_col}]")
        if ofc: group_parts.append(f"ia.[{ofc}]")

        having = f"HAVING SUM(CAST(ia.[{gross}] AS DECIMAL(18,2))) > 0" if gross else ""
        sort = f"ORDER BY SUM(CAST(ia.[{gross}] AS DECIMAL(18,2))) DESC" if gross else f"ORDER BY COUNT(DISTINCT ia.[{pid}]) DESC"

        prov_filter = f"AND ia.[{prov}] IS NOT NULL" if prov else ""

        cursor.execute(f"""
            SELECT {', '.join(select_parts)}
            FROM rpt.vw_income_allocation ia
            JOIN {pat_tbl} p ON ia.[{pid}] = p.[{pat_pid_col}]
            WHERE ia.[{svc}] >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
              AND p.[{zip_col}] IS NOT NULL
              AND LEN(LTRIM(RTRIM(p.[{zip_col}]))) >= 5
              {prov_filter}
            GROUP BY {', '.join(group_parts)}
            {having}
            {sort}
        """)
        for row in rows_to_dicts(cursor):
            result.append({
                'zip': str(row.get('zip_code', '')).strip(),
                'city': str(row.get('city', '')).strip(),
                'state': str(row.get('state', '')).strip(),
                'office': normalize_office(str(row.get('office', ''))),
                'net': round(float(row.get('net_production', 0) or 0), 2),
                'gross': round(float(row.get('gross_production', 0) or 0), 2),
                'patients': int(row.get('patient_count', 0)),
            })
        print(f"  Production by zip: {len(result)} zip codes")
    except Exception as e:
        print(f"  ⚠ Production by zip failed: {e}")

    return result


# ── Recently-Seen Patient Filtering ────────────────────────────────────────
# Patients who have had a hygiene/recall visit completed recently should
# NOT appear on the call sheet. The Denticon views may not auto-exclude
# them, so we build our own exclusion set.

def _map_patient_ids_to_rpids(cursor, patient_ids):
    """Map a set of patient IDs to household RPIDs via patient tables.
    Returns a set of household RPIDs."""
    household_rpids = set()
    if not patient_ids:
        return household_rpids

    patient_tables = ['PATHDR', 'PATD', 'tbl_patient', 'Patient', 'PAT']
    for tbl in patient_tables:
        try:
            cursor.execute(f"SELECT TOP 0 * FROM {tbl}")
            cols = [col[0] for col in cursor.description]
            rpid_col = next((c for c in cols if 'RPID' in c.upper() or 'RP' == c.upper()), None)
            pid_col = next((c for c in cols if c.upper() == 'PATID'), None)
            if not pid_col:
                pid_col = next((c for c in cols if 'PAT' in c.upper() and 'ID' in c.upper()), None)
            if rpid_col and pid_col:
                # Process in batches to avoid SQL length limits
                pid_list_all = [str(int(p)) for p in patient_ids
                                if str(p).replace('.', '').replace('-', '').isdigit()]
                for i in range(0, len(pid_list_all), 500):
                    batch = ",".join(pid_list_all[i:i+500])
                    if batch:
                        cursor.execute(f"""
                            SELECT DISTINCT [{rpid_col}]
                            FROM {tbl}
                            WHERE [{pid_col}] IN ({batch})
                              AND [{rpid_col}] IS NOT NULL
                        """)
                        for row in cursor.fetchall():
                            if row[0]:
                                try:
                                    household_rpids.add(int(row[0]))
                                except (ValueError, TypeError):
                                    pass
                print(f"  Mapped {len(patient_ids)} patients → "
                      f"{len(household_rpids)} households via {tbl}.{rpid_col}")
                return household_rpids
        except Exception:
            continue

    # Fallback: use patient IDs as RPIDs (some systems use same IDs)
    print(f"  ⚠ Could not map patient_ids to RPIDs — using patient_ids as RPIDs")
    for pid in patient_ids:
        try:
            household_rpids.add(int(pid))
        except (ValueError, TypeError):
            pass
    return household_rpids


def query_recently_seen_patients(conn):
    """Build exclusion sets of patients/households who have had a dental
    visit in the last 45 days.  Returns two things:
      - patient_ids: set of patient IDs seen recently
      - household_rpids: set of RPIDs where ANY family member was seen

    Tries multiple Denticon tables/views to find recent visits:
      1. vw_RecallAnchor_LastRecallAppt — has PATID + LastRecallDate
      2. BI_Appointments — has PATID + APPTDATE + APPTSTATUS (tinyint)
      3. APPTH (appointment header) — may have PATID + date
      4. rpt.vw_income_allocation — production data with patient + date
    """
    cursor = conn.cursor()
    patient_ids = set()
    household_rpids = set()

    # First, discover actual column names in key views for debugging
    for view in ['rpt.vw_income_allocation', 'vw_RecallAnchor_LastRecallAppt',
                 'BI_Appointments', 'APPTH']:
        try:
            cursor.execute("SELECT TOP 0 * FROM " + view)
            cols = [col[0] for col in cursor.description]
            print(f"  Columns in {view}: {cols}")
        except Exception:
            print(f"  ⚠ Cannot access {view}")

    # ── Strategy 1: vw_RecallAnchor_LastRecallAppt ─────────────────────
    # This view has: PATID, OID, LastRecallDate
    # PATID is patient-level (not household RPID), so we collect patient IDs
    # and map to household RPIDs afterward.
    try:
        cursor.execute("SELECT TOP 0 * FROM vw_RecallAnchor_LastRecallAppt")
        cols = [col[0] for col in cursor.description]
        # Look for patient ID column (PATID, PatientID, etc.)
        pid_col = next((c for c in cols if 'PATID' in c.upper() or
                        (c.upper().startswith('PAT') and 'ID' in c.upper())), None)
        # Look for RPID (household) — might be present in some configurations
        rpid_col = next((c for c in cols if 'RPID' in c.upper()), None)
        # Look for date column
        date_col = next((c for c in cols if 'DATE' in c.upper() or 'RECALL' in c.upper()), None)

        if rpid_col and date_col:
            # Best case: direct RPID available
            cursor.execute(f"""
                SELECT DISTINCT [{rpid_col}]
                FROM vw_RecallAnchor_LastRecallAppt
                WHERE [{date_col}] >= DATEADD(day, -45, GETDATE())
                  AND [{rpid_col}] IS NOT NULL
            """)
            for row in cursor.fetchall():
                if row[0]:
                    try:
                        household_rpids.add(int(row[0]))
                    except (ValueError, TypeError):
                        pass
            print(f"  RecallAnchor (RPID): {len(household_rpids)} households seen in last 45 days")
        elif pid_col and date_col:
            # Has PATID + date — collect patient IDs, map to RPIDs later
            cursor.execute(f"""
                SELECT DISTINCT [{pid_col}]
                FROM vw_RecallAnchor_LastRecallAppt
                WHERE [{date_col}] >= DATEADD(day, -45, GETDATE())
                  AND [{pid_col}] IS NOT NULL
            """)
            for row in cursor.fetchall():
                if row[0]:
                    patient_ids.add(row[0])
            print(f"  RecallAnchor (PATID): {len(patient_ids)} patients seen in last 45 days")
        else:
            print(f"  ⚠ RecallAnchor columns not suitable: {cols}")
    except Exception as e:
        print(f"  ⚠ RecallAnchor query failed: {e}")

    # ── Strategy 2: BI_Appointments ────────────────────────────────────
    # Columns: APPTID, PATID, PROVIDERID, APPTDATE, APPTSTATUS (tinyint!),
    #          APPTLENGTH, OPERATORYID, PRODTYPE
    # APPTSTATUS is tinyint — do NOT compare with varchar strings.
    # Just filter by APPTDATE (recent appointments = recently seen).
    if not patient_ids and not household_rpids:
        try:
            cursor.execute("SELECT TOP 0 * FROM BI_Appointments")
            cols = [col[0] for col in cursor.description]
            pid_col = next((c for c in cols if 'PAT' in c.upper() and 'ID' in c.upper()), None)
            date_col = next((c for c in cols if 'DATE' in c.upper()), None)
            if pid_col and date_col:
                # Date-only filter — APPTSTATUS is tinyint, safe to skip
                # (we just want to know IF they had an appointment recently)
                cursor.execute(f"""
                    SELECT DISTINCT [{pid_col}]
                    FROM BI_Appointments
                    WHERE [{date_col}] >= DATEADD(day, -45, GETDATE())
                      AND [{date_col}] <= GETDATE()
                      AND [{pid_col}] IS NOT NULL
                """)
                for row in cursor.fetchall():
                    if row[0]:
                        patient_ids.add(row[0])
                print(f"  BI_Appointments: {len(patient_ids)} patients with appts in last 45 days")
        except Exception as e:
            print(f"  ⚠ BI_Appointments query failed: {e}")

    # ── Strategy 3: APPTH (appointment header — may have PATID + date) ─
    if not patient_ids and not household_rpids:
        try:
            cursor.execute("SELECT TOP 0 * FROM APPTH")
            cols = [col[0] for col in cursor.description]
            pid_col = next((c for c in cols if 'PAT' in c.upper() and 'ID' in c.upper()), None)
            if not pid_col:
                pid_col = next((c for c in cols if 'PAT' in c.upper()), None)
            date_col = next((c for c in cols if 'DATE' in c.upper()), None)
            if pid_col and date_col:
                cursor.execute(f"""
                    SELECT DISTINCT [{pid_col}]
                    FROM APPTH
                    WHERE [{date_col}] >= DATEADD(day, -45, GETDATE())
                      AND [{date_col}] <= GETDATE()
                      AND [{pid_col}] IS NOT NULL
                """)
                for row in cursor.fetchall():
                    if row[0]:
                        patient_ids.add(row[0])
                print(f"  APPTH: {len(patient_ids)} patients with appts in last 45 days")
        except Exception as e:
            print(f"  ⚠ APPTH query failed: {e}")

    # ── Strategy 4: rpt.vw_income_allocation (with schema prefix!) ────
    if not patient_ids and not household_rpids:
        try:
            cursor.execute("SELECT TOP 0 * FROM rpt.vw_income_allocation")
            cols = [col[0] for col in cursor.description]
            pid_col = next((c for c in cols if 'PAT' in c.upper() and 'ID' in c.upper()), None)
            date_col = next((c for c in cols if 'DATE' in c.upper() or 'SERVICE' in c.upper()), None)
            if pid_col and date_col:
                cursor.execute(f"""
                    SELECT DISTINCT [{pid_col}]
                    FROM rpt.vw_income_allocation
                    WHERE [{date_col}] >= DATEADD(day, -45, GETDATE())
                      AND [{pid_col}] IS NOT NULL
                """)
                for row in cursor.fetchall():
                    if row[0]:
                        patient_ids.add(row[0])
                print(f"  income_allocation: {len(patient_ids)} patients (via {pid_col}/{date_col})")
        except Exception as e:
            print(f"  ⚠ income_allocation auto-discover failed: {e}")

    # ── Map patient_ids → household RPIDs ──────────────────────────────
    if patient_ids and not household_rpids:
        household_rpids = _map_patient_ids_to_rpids(cursor, patient_ids)

    print(f"  Summary: {len(patient_ids)} patient IDs, {len(household_rpids)} household RPIDs")
    return patient_ids, household_rpids


def discover_view_columns(conn, view_name):
    """Discover columns available in a view for debugging."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, (view_name,))
        cols = [(row[0], row[1]) for row in cursor.fetchall()]
        if cols:
            print(f"  Columns in {view_name}:")
            for name, dtype in cols:
                print(f"    - {name} ({dtype})")
        return cols
    except Exception as e:
        print(f"  ⚠ Could not discover columns for {view_name}: {e}")
        return []


def filter_recently_seen(rows, household_rpids, id_field="household_id"):
    """Remove rows whose household RPID is in the recently-seen set.
    Returns (filtered_rows, removed_count)."""
    if not household_rpids:
        return rows, 0

    filtered = []
    removed = 0
    for row in rows:
        rpid_val = row.get(id_field)
        try:
            rpid_int = int(rpid_val) if rpid_val else None
        except (ValueError, TypeError):
            rpid_int = None

        if rpid_int and rpid_int in household_rpids:
            removed += 1
        else:
            filtered.append(row)

    return filtered, removed


# ── Data Processing ─────────────────────────────────────────────────────────

def normalize_office(name):
    """Normalize office names to PSPD canonical uppercase form."""
    if not name:
        return ""
    upper = str(name).upper().strip()
    for canonical in OFFICE_CANONICAL:
        if canonical in upper:
            return canonical
    return upper


def normalize_phone(phone):
    """Format phone number as (XXX) XXX-XXXX."""
    if not phone:
        return ""
    # Strip everything except digits
    digits = ''.join(c for c in str(phone) if c.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    # Already formatted or international — return as-is
    return str(phone).strip()


def clean_row(row):
    """Normalize a result row for JSON serialization."""
    cleaned = {}
    for k, v in row.items():
        if isinstance(v, (datetime, date)):
            cleaned[k] = v.strftime("%Y-%m-%d")
        elif isinstance(v, bytes):
            cleaned[k] = v.decode("utf-8", errors="replace")
        elif v is None:
            cleaned[k] = ""
        else:
            cleaned[k] = v

    # Normalize office names
    for key in ("last_office", "most_frequent_office", "last_visit_office"):
        if key in cleaned and cleaned[key]:
            cleaned[key] = normalize_office(cleaned[key])

    # Normalize phone
    if "phone" in cleaned:
        cleaned["phone"] = normalize_phone(cleaned["phone"])

    # Ensure numeric fields
    for key in ("kids_due_count", "days_overdue", "days_since_plan"):
        if key in cleaned:
            try:
                cleaned[key] = int(cleaned[key]) if cleaned[key] else 0
            except (ValueError, TypeError):
                cleaned[key] = 0

    for key in ("total_fee",):
        if key in cleaned:
            try:
                cleaned[key] = round(float(cleaned[key]), 2) if cleaned[key] else 0
            except (ValueError, TypeError):
                cleaned[key] = 0

    # Ensure household has a display name (not just RPID number)
    hh = str(cleaned.get("household", "")).strip()
    if not hh or hh.isdigit():
        # Fallback: use kids_due_names or household_id
        kids = str(cleaned.get("kids_due_names", "")).strip()
        if kids:
            cleaned["household"] = kids + " Family"
        else:
            cleaned["household"] = f"Household {hh}" if hh else "Unknown"

    # Normalize risk tags for consistent display
    risk = str(cleaned.get("caries_risk", "")).strip()
    if risk.lower() in ("high", "3"):
        cleaned["caries_risk"] = "High"
    elif risk.lower() in ("medium", "moderate", "med", "2"):
        cleaned["caries_risk"] = "Medium"
    elif risk.lower() in ("low", "1"):
        cleaned["caries_risk"] = "Low"

    return cleaned


def enrich_treatment_with_zcode(rows, zcode_map):
    """Add Z-code dates to treatment rows where available."""
    for row in rows:
        pid = row.get("patient_id")
        if pid and pid in zcode_map:
            zdate = zcode_map[pid]
            row["z_code_date"] = (
                zdate.strftime("%Y-%m-%d") if hasattr(zdate, "strftime") else str(zdate)
            )
    return rows


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("PSPD Call Sheet Data Refresh")
    print(f"Started: {datetime.utcnow().isoformat()}Z")
    print("=" * 60)
    print()
    print("Views used (same as Power BI dashboards):")
    print("  • vw_RecallHouseholdDueNext30Days_PBI  (Recall)")
    print("  • vw_RecallHouseholdDue                (Overdue)")
    print("  • vw_TxAction_Unscheduled_Current_Scheduler_v2 (Treatment)")
    print()

    conn = connect()
    print("✓ Connected to Azure SQL")

    # Check data freshness
    print("\nData freshness...")
    freshness = query_data_freshness(conn)

    # Discover view columns (helps debug filtering issues)
    print("\nDiscovering view schemas...")
    discover_view_columns(conn, "vw_RecallHouseholdDueNext30Days_PBI")
    discover_view_columns(conn, "vw_RecallHouseholdDue")

    # Build recently-seen patient exclusion set BEFORE querying call sheets
    print("\nBuilding recently-seen patient exclusion list...")
    recent_patient_ids, recent_household_rpids = query_recently_seen_patients(conn)

    # Pull all three datasets from the SAME views Power BI uses
    print("\nQuerying call sheet data...")
    recall_rows = query_recall(conn)
    overdue_rows = query_overdue(conn)
    treatment_rows = query_treatment(conn)

    # Pull supplementary data (contact log, bad phones, Z-codes)
    print("\nQuerying supplementary data...")
    contact_log = query_contact_log(conn)
    bad_phones = query_bad_phones(conn)
    zcode_map = query_zcode_status(conn)

    # Discover income allocation column names for production queries
    print("\nDiscovering income allocation columns...")
    ia_col_map = discover_income_allocation_columns(conn)

    # Pull doctor leaderboard KPIs
    print("\nQuerying doctor leaderboard...")
    leaderboard = query_doctor_leaderboard(conn, col_map=ia_col_map)

    # Pull daily production by location for production dashboard
    print("\nQuerying daily production by location...")
    daily_production = query_daily_production(conn, col_map=ia_col_map)

    # Pull production by patient zip code for geographic heatmap
    print("\nQuerying production by zip code...")
    production_by_zip = query_production_by_zip(conn, col_map=ia_col_map)

    conn.close()
    print("\n✓ Connection closed")

    # Enrich treatment rows with Z-code data
    enrich_treatment_with_zcode(treatment_rows, zcode_map)

    # Clean and serialize all rows
    recall = [clean_row(r) for r in recall_rows]
    overdue = [clean_row(r) for r in overdue_rows]
    treatment = [clean_row(r) for r in treatment_rows]
    contact_log_clean = [clean_row(r) for r in contact_log]

    # ── Post-process: remove recently-seen households ─────────────────────
    # Even if the SQL-level filtering caught most, this catches stragglers
    # (e.g., if the NOT EXISTS subquery failed and we fell back to unfiltered)
    recall_before = len(recall)
    recall, recall_removed = filter_recently_seen(recall, recent_household_rpids, "household_id")
    if recall_removed:
        print(f"  ✓ Recall: removed {recall_removed} recently-seen households "
              f"({recall_before} → {len(recall)})")

    overdue_before = len(overdue)
    overdue, overdue_removed = filter_recently_seen(overdue, recent_household_rpids, "household_id")
    if overdue_removed:
        print(f"  ✓ Overdue: removed {overdue_removed} recently-seen households "
              f"({overdue_before} → {len(overdue)})")

    # Also filter treatment: if a patient has been seen recently AND has
    # a next appointment, they likely don't need to be called
    treatment_before = len(treatment)
    treatment_filtered = []
    tx_removed = 0
    for row in treatment:
        pid = row.get("patient_id")
        next_appt = row.get("next_appt_date")
        try:
            pid_int = int(pid) if pid else None
        except (ValueError, TypeError):
            pid_int = None
        # Only remove from treatment if they were seen recently AND have a
        # next appointment scheduled (they're actively being treated)
        if pid_int and pid_int in recent_patient_ids and next_appt:
            tx_removed += 1
        else:
            treatment_filtered.append(row)
    treatment = treatment_filtered
    if tx_removed:
        print(f"  ✓ Treatment: removed {tx_removed} recently-seen+scheduled patients "
              f"({treatment_before} → {len(treatment)})")

    # Count high-risk treatment patients
    high_risk = sum(
        1 for r in treatment
        if str(r.get("caries_risk", "")).lower() == "high"
    )

    # Build output JSON matching the dashboard's expected structure
    output = {
        "recall": recall,
        "overdue": overdue,
        "treatment": treatment,
        "contact_log": contact_log_clean,
        "bad_phones": bad_phones,
        "doctor_leaderboard": leaderboard,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "generated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "data_as_of": {
            "recall": str(freshness.get("recall_as_of", "")),
            "treatment": str(freshness.get("treatment_as_of", "")),
        },
        "stats": {
            "recall_households": len(recall),
            "overdue_households": len(overdue),
            "treatment_patients": len(treatment),
            "total_tx_value": sum(r.get("total_fee", 0) for r in treatment),
            "high_risk_patients": high_risk,
            "bad_phone_count": len(bad_phones),
            "contact_log_entries": len(contact_log_clean),
            "leaderboard_providers": len(leaderboard.get('providers', [])),
        },
        "filtering": {
            "recently_seen_patients": len(recent_patient_ids),
            "recently_seen_households": len(recent_household_rpids),
            "recall_removed": recall_removed,
            "overdue_removed": overdue_removed,
            "treatment_removed": tx_removed,
            "note": "Patients/households seen in last 45 days are excluded from call sheets",
        },
    }

    # Write call sheet output
    os.makedirs(os.path.dirname(OUTPUT_FILE) or "data", exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, default=json_serial)

    # Write production dashboard data (separate file for production.html)
    production_output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "by_date": daily_production.get('by_date', []),
        "by_office_daily": daily_production.get('by_office_daily', []),
        "by_provider_mtd": daily_production.get('by_provider_mtd', []),
        "monthly_summary": daily_production.get('monthly_summary', []),
        "by_zip": production_by_zip,
        "stats": {
            "daily_records": len(daily_production.get('by_date', [])),
            "office_daily_records": len(daily_production.get('by_office_daily', [])),
            "provider_count": len(daily_production.get('by_provider_mtd', [])),
        },
    }
    prod_file = os.path.join(os.path.dirname(OUTPUT_FILE) or "data", "production.json")
    with open(prod_file, "w") as f:
        json.dump(production_output, f, indent=2, default=json_serial)

    print(f"\n{'=' * 60}")
    print(f"✓ Wrote {OUTPUT_FILE}")
    print(f"  Recall:    {len(recall)} households (due next 30 days)")
    print(f"  Overdue:   {len(overdue)} households (past due)")
    print(f"  Treatment: {len(treatment)} patients (unscheduled)")
    print(f"  Tx Value:  ${output['stats']['total_tx_value']:,.2f}")
    if high_risk:
        print(f"  High Risk: {high_risk} patients")
    print(f"✓ Wrote {prod_file}")
    print(f"  Daily production: {len(daily_production.get('by_date', []))} days")
    print(f"  Office breakdown: {len(daily_production.get('by_office_daily', []))} records")
    print(f"\nDone: {datetime.utcnow().isoformat()}Z")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
