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
  SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD
"""

import os
import sys
import json
import pymssql
from datetime import datetime, date

# ── Configuration ────────────────────────────────────────────────────────────
SQL_SERVER   = os.environ.get("SQL_SERVER", "")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "")
SQL_USER     = os.environ.get("SQL_USER", "")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")
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
    """JSON serializer for date/datetime objects."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not JSON serializable")


def connect():
    """Connect to Azure SQL Server."""
    if not all([SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD]):
        print("ERROR: Missing SQL credentials.")
        print("  Set SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD")
        sys.exit(1)

    print(f"Connecting to {SQL_SERVER}/{SQL_DATABASE}...")
    return pymssql.connect(
        server=SQL_SERVER,
        user=SQL_USER,
        password=SQL_PASSWORD,
        database=SQL_DATABASE,
        tds_version="7.3"
    )


# ── Recall (Due Next 30 Days) ──────────────────────────────────────────────
# Source: vw_RecallHouseholdDueNext30Days_PBI
# This is the same view used by the "Due Next 30 Days" page in
# PSPD_Recall_CallLists.pbix

def query_recall(conn):
    """Pull hygiene recall households due in the next 30 days."""
    cursor = conn.cursor(as_dict=True)
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
        rows = cursor.fetchall()
        print(f"  Recall (next 30 days): {len(rows)} households")
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
    cursor = conn.cursor(as_dict=True)
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
            ORDER BY OldestHygieneDate ASC, KidsDueCount DESC
        """)
        rows = cursor.fetchall()
        print(f"  Overdue: {len(rows)} households")
        return rows
    except Exception as e:
        print(f"  ✗ Overdue query failed: {e}")
        return []


# ── Unscheduled Treatment ──────────────────────────────────────────────────
# Source: vw_TxAction_Unscheduled_Current_Scheduler_v2
# This is the same view used by Treatment - Unscheduled.pbix

def query_treatment(conn):
    """Pull unscheduled treatment patients."""
    cursor = conn.cursor(as_dict=True)
    try:
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
                TxSummaryFormatted          AS tx_summary,
                TotalFee                    AS total_fee,
                BookingSlot                 AS booking_slot,
                DiagnosingProvider          AS diagnosing_provider,
                PrimaryCarrierName          AS insurance,
                RiskTag                     AS caries_risk,
                DaysSinceLastPlanActivity   AS days_since_plan,
                NextApptDate                AS next_appt_date,
                NextApptType                AS next_appt_type
            FROM vw_TxAction_Unscheduled_Current_Scheduler_v2
            ORDER BY TotalFee DESC
        """)
        rows = cursor.fetchall()
        print(f"  Treatment: {len(rows)} patients")
        return rows
    except Exception as e:
        print(f"  ✗ Treatment query failed: {e}")
        return []


# ── Data Freshness ──────────────────────────────────────────────────────────
# Check when the underlying data was last refreshed

def query_data_freshness(conn):
    """Get the last data refresh timestamp from the update stamp views."""
    cursor = conn.cursor(as_dict=True)
    timestamps = {}

    # Try vw_LastUpdateStamp (used by Recall model)
    try:
        cursor.execute("SELECT TOP 1 OperationalDate FROM vw_LastUpdateStamp")
        row = cursor.fetchone()
        if row:
            timestamps['recall_as_of'] = row['OperationalDate']
            print(f"  Recall data as of: {row['OperationalDate']}")
    except Exception:
        pass

    # Try vw_DataLastUpdate (used by Treatment model)
    try:
        cursor.execute("SELECT TOP 1 LastDataUTC FROM vw_DataLastUpdate")
        row = cursor.fetchone()
        if row:
            timestamps['treatment_as_of'] = row['LastDataUTC']
            print(f"  Treatment data as of: {row['LastDataUTC']}")
    except Exception:
        pass

    return timestamps


# ── Contact Log & Z-Codes ──────────────────────────────────────────────────
# These are custom tables maintained by the call sheet system itself,
# not from Denticon views. They may not exist on first run.

def query_contact_log(conn):
    """Pull SMS/contact log from the callsheet_log table (if it exists)."""
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute("""
            SELECT row_id, phone, name, type, status, date, message, error
            FROM rpt.callsheet_contact_log
            WHERE date >= DATEADD(day, -30, GETDATE())
            ORDER BY date DESC
        """)
        rows = cursor.fetchall()
        print(f"  Contact log: {len(rows)} entries (last 30 days)")
        return rows
    except Exception:
        print("  ⚠ No contact log table found (rpt.callsheet_contact_log)")
        print("    Contact history will be tracked in-browser until table is created.")
        return []


def query_bad_phones(conn):
    """Pull known bad phone numbers from the contact log."""
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute("""
            SELECT DISTINCT phone
            FROM rpt.callsheet_contact_log
            WHERE status IN ('failed', 'undelivered')
              AND (error LIKE '%211%' OR error LIKE '%614%' OR error LIKE '%608%')
        """)
        rows = cursor.fetchall()
        phones = [r['phone'] for r in rows if r['phone']]
        print(f"  Bad phones: {len(phones)} numbers flagged")
        return phones
    except Exception:
        print("  ⚠ Could not query bad phones (no log table)")
        return []


def query_zcode_status(conn):
    """Check for Z-code entries marking patients as 'contacted this cycle'.
    Z-codes are typically posted as procedures in the Denticon ledger."""
    cursor = conn.cursor(as_dict=True)
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
            rows = cursor.fetchall()
            if rows:
                print(f"  Z-codes: {len(rows)} patients contacted (pattern {i+1})")
                return {r['patient_id']: r.get('z_code_date', '') for r in rows}
        except Exception:
            continue

    print("  ⚠ No Z-code data found (table may not exist yet)")
    return {}


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

    conn.close()
    print("\n✓ Connection closed")

    # Enrich treatment rows with Z-code data
    enrich_treatment_with_zcode(treatment_rows, zcode_map)

    # Clean and serialize all rows
    recall = [clean_row(r) for r in recall_rows]
    overdue = [clean_row(r) for r in overdue_rows]
    treatment = [clean_row(r) for r in treatment_rows]
    contact_log_clean = [clean_row(r) for r in contact_log]

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
        },
    }

    # Write output
    os.makedirs(os.path.dirname(OUTPUT_FILE) or "data", exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, default=json_serial)

    print(f"\n{'=' * 60}")
    print(f"✓ Wrote {OUTPUT_FILE}")
    print(f"  Recall:    {len(recall)} households (due next 30 days)")
    print(f"  Overdue:   {len(overdue)} households (past due)")
    print(f"  Treatment: {len(treatment)} patients (unscheduled)")
    print(f"  Tx Value:  ${output['stats']['total_tx_value']:,.2f}")
    if high_risk:
        print(f"  High Risk: {high_risk} patients")
    print(f"\nDone: {datetime.utcnow().isoformat()}Z")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
