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

    # First try: query with NOT EXISTS against recent production (excludes
    # households where any family member was seen in the last 30 days)
    try:
        cursor.execute("""
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
                -- Exclude if ANY patient in this household had a visit
                -- in the last 30 days (they've already been seen)
                SELECT 1 FROM rpt.vw_income_allocation ia
                JOIN dbo.tbl_patient p ON ia.patient_id = p.PatID
                WHERE p.RPID = r.RPID
                  AND ia.service_date >= DATEADD(day, -30, GETDATE())
            )
            ORDER BY r.KidsDueCount DESC, r.SuggestedFamilyDate ASC
        """)
        rows = rows_to_dicts(cursor)
        print(f"  Recall (next 30 days): {len(rows)} households (filtered — excludes recently seen)")
        return rows
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

def query_doctor_leaderboard(conn):
    """Build doctor-level KPIs from treatment and production views."""
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
    try:
        cursor.execute("""
            SELECT
                provider_name               AS provider,
                SUM(net_production)          AS mtd_production,
                SUM(net_collections)         AS mtd_collections,
                COUNT(DISTINCT patient_id)   AS patients_seen
            FROM rpt.vw_income_allocation
            WHERE service_date >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
              AND provider_name IS NOT NULL
              AND provider_name <> ''
            GROUP BY provider_name
            ORDER BY SUM(net_production) DESC
        """)
        for row in rows_to_dicts(cursor):
            prov = str(row.get('provider', '')).strip()
            if not prov:
                continue
            if prov not in leaderboard:
                leaderboard[prov] = {'provider': prov}
            leaderboard[prov]['mtd_production'] = round(float(row.get('mtd_production', 0) or 0), 2)
            leaderboard[prov]['mtd_collections'] = round(float(row.get('mtd_collections', 0) or 0), 2)
            leaderboard[prov]['patients_seen_mtd'] = int(row.get('patients_seen', 0))
        print(f"  MTD production data loaded for {len(leaderboard)} providers")
    except Exception as e:
        print(f"  ⚠ MTD production query failed: {e}")

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


# ── Daily Production by Location ────────────────────────────────────────────
# Provides daily Gross/Net production by office for the production dashboard.
# Uses rpt.vw_income_allocation which is the same view PowerBI used.

def query_daily_production(conn):
    """Pull daily production by office for current month + prior month."""
    cursor = conn.cursor()
    result = {
        'by_date': [],
        'by_office_daily': [],
        'by_provider_mtd': [],
        'monthly_summary': [],
    }

    # Daily production by office — current month
    try:
        cursor.execute("""
            SELECT
                CAST(service_date AS DATE)   AS prod_date,
                office_name                  AS office,
                SUM(gross_production)        AS gross_production,
                SUM(net_production)          AS net_production,
                SUM(net_collections)         AS net_collections,
                SUM(adjustments)             AS adjustments,
                COUNT(DISTINCT patient_id)   AS patients_seen
            FROM rpt.vw_income_allocation
            WHERE service_date >= DATEADD(month, -1, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))
              AND office_name IS NOT NULL
              AND office_name <> ''
            GROUP BY CAST(service_date AS DATE), office_name
            ORDER BY CAST(service_date AS DATE), office_name
        """)
        for row in rows_to_dicts(cursor):
            result['by_office_daily'].append({
                'date': str(row.get('prod_date', '')),
                'office': normalize_office(str(row.get('office', ''))),
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
        cursor.execute("""
            SELECT
                CAST(service_date AS DATE)   AS prod_date,
                SUM(gross_production)        AS gross_production,
                SUM(net_production)          AS net_production,
                SUM(net_collections)         AS net_collections,
                SUM(adjustments)             AS adjustments,
                COUNT(DISTINCT patient_id)   AS patients_seen,
                COUNT(DISTINCT provider_name) AS providers_active
            FROM rpt.vw_income_allocation
            WHERE service_date >= DATEADD(month, -1, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))
              AND provider_name IS NOT NULL
              AND provider_name <> ''
            GROUP BY CAST(service_date AS DATE)
            ORDER BY CAST(service_date AS DATE)
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
    try:
        cursor.execute("""
            SELECT
                provider_name                AS provider,
                office_name                  AS office,
                SUM(gross_production)        AS gross_production,
                SUM(net_production)          AS net_production,
                SUM(net_collections)         AS net_collections,
                SUM(adjustments)             AS adjustments,
                COUNT(DISTINCT patient_id)   AS patients_seen,
                COUNT(DISTINCT CAST(service_date AS DATE)) AS days_worked
            FROM rpt.vw_income_allocation
            WHERE service_date >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
              AND provider_name IS NOT NULL
              AND provider_name <> ''
            GROUP BY provider_name, office_name
            ORDER BY SUM(net_production) DESC
        """)
        for row in rows_to_dicts(cursor):
            result['by_provider_mtd'].append({
                'provider': str(row.get('provider', '')).strip(),
                'office': normalize_office(str(row.get('office', ''))),
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
        cursor.execute("""
            SELECT
                YEAR(service_date)           AS yr,
                MONTH(service_date)          AS mo,
                SUM(gross_production)        AS gross_production,
                SUM(net_production)          AS net_production,
                SUM(net_collections)         AS net_collections,
                SUM(adjustments)             AS adjustments,
                COUNT(DISTINCT patient_id)   AS patients_seen
            FROM rpt.vw_income_allocation
            WHERE service_date >= DATEADD(month, -12, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))
              AND provider_name IS NOT NULL
            GROUP BY YEAR(service_date), MONTH(service_date)
            ORDER BY YEAR(service_date), MONTH(service_date)
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

def query_production_by_zip(conn):
    """Pull production aggregated by patient zip code for current month."""
    cursor = conn.cursor()
    result = []

    try:
        cursor.execute("""
            SELECT
                LEFT(LTRIM(RTRIM(p.Zip)), 5)   AS zip_code,
                p.City                           AS city,
                p.State                          AS state,
                ia.office_name                   AS office,
                SUM(ia.net_production)           AS net_production,
                SUM(ia.gross_production)         AS gross_production,
                COUNT(DISTINCT ia.patient_id)    AS patient_count
            FROM rpt.vw_income_allocation ia
            JOIN dbo.tbl_patient p ON ia.patient_id = p.PatID
            WHERE ia.service_date >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
              AND p.Zip IS NOT NULL
              AND LEN(LTRIM(RTRIM(p.Zip))) >= 5
              AND ia.provider_name IS NOT NULL
            GROUP BY LEFT(LTRIM(RTRIM(p.Zip)), 5), p.City, p.State, ia.office_name
            HAVING SUM(ia.net_production) > 0
            ORDER BY SUM(ia.net_production) DESC
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
        print(f"  ⚠ Production by zip failed (table join may not exist): {e}")

    return result


# ── Recently-Seen Patient Filtering ────────────────────────────────────────
# Patients who have had a hygiene/recall visit completed recently should
# NOT appear on the call sheet. The Denticon views may not auto-exclude
# them, so we build our own exclusion set.

def query_recently_seen_patients(conn):
    """Build exclusion sets of patients/households who have had a dental
    visit in the last 45 days.  Returns two things:
      - patient_ids: set of patient IDs seen recently
      - household_rpids: set of RPIDs where ANY family member was seen
    """
    cursor = conn.cursor()
    patient_ids = set()
    household_rpids = set()

    # 1) Patients with production in last 45 days (from income_allocation)
    try:
        cursor.execute("""
            SELECT DISTINCT patient_id
            FROM rpt.vw_income_allocation
            WHERE service_date >= DATEADD(day, -45, GETDATE())
              AND patient_id IS NOT NULL
        """)
        for row in cursor.fetchall():
            if row[0]:
                patient_ids.add(row[0])
        print(f"  Recently seen (production): {len(patient_ids)} patients in last 45 days")
    except Exception as e:
        print(f"  ⚠ Recent production query failed: {e}")

    # 2) Try to map patient_ids → RPIDs via tbl_patient (Denticon RP = Responsible Party)
    if patient_ids:
        try:
            # Denticon stores RPID on the patient record — the responsible party
            # who links family members into a household
            pid_list = ",".join(str(int(p)) for p in patient_ids if str(p).isdigit())
            if pid_list:
                cursor.execute(f"""
                    SELECT DISTINCT RPID
                    FROM dbo.tbl_patient
                    WHERE PatID IN ({pid_list})
                      AND RPID IS NOT NULL
                      AND RPID > 0
                """)
                for row in cursor.fetchall():
                    if row[0]:
                        household_rpids.add(int(row[0]))
                print(f"  Mapped to {len(household_rpids)} recently-seen households (RPIDs)")
        except Exception as e:
            print(f"  ⚠ RPID mapping failed: {e}")
            # Fallback: try using patient_id as RPID (sometimes they match)
            for pid in patient_ids:
                try:
                    household_rpids.add(int(pid))
                except (ValueError, TypeError):
                    pass

    # 3) Also try direct appointment table for completed hygiene visits
    try:
        cursor.execute("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME LIKE '%appt%' OR TABLE_NAME LIKE '%appointment%'
            ORDER BY TABLE_NAME
        """)
        appt_tables = [row[0] for row in cursor.fetchall()]
        if appt_tables:
            print(f"  Discovered appointment tables: {appt_tables}")
    except Exception:
        pass

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

    # Pull doctor leaderboard KPIs
    print("\nQuerying doctor leaderboard...")
    leaderboard = query_doctor_leaderboard(conn)

    # Pull daily production by location for production dashboard
    print("\nQuerying daily production by location...")
    daily_production = query_daily_production(conn)

    # Pull production by patient zip code for geographic heatmap
    print("\nQuerying production by zip code...")
    production_by_zip = query_production_by_zip(conn)

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
