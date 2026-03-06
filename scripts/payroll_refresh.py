#!/usr/bin/env python3
"""
payroll_refresh.py — PSPD Doctor Payroll Data Pipeline
======================================================
Generates data/payroll.json consumed by payroll.html's loadLiveData() function.

TWO DATA PATHS (automatic failover):
  1. Azure SQL  — queries Denticon's income allocation tables directly
  2. Excel file — reads Tanya's "Income Allocation Report - Detail" export

Usage:
  # From Azure SQL (automated via GitHub Actions):
  python scripts/payroll_refresh.py

  # From Tanya's Excel export (manual fallback):
  python scripts/payroll_refresh.py --from-excel path/to/Denticon_NewMonthlyIncAllD.xlsx

  # Discover available Azure SQL views (run once to find correct tables):
  python scripts/payroll_refresh.py --discover

Environment Variables (for Azure SQL path):
  AZURE_SQL_CONN_STR — Full ODBC connection string for Denticon Azure SQL

Output:
  data/payroll.json — consumed by payroll.html loadLiveData()
"""

import json
import os
import sys
import argparse
import re
from datetime import datetime, date, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIGURATION — edit these when rates change or doctors join/leave
# ---------------------------------------------------------------------------

DOCTOR_CONFIG = {
    # Denticon provider name → display name, pay rate, owner flag
    'Slaven, Chad':       {'display': 'Dr. Slaven',  'pct': 0.36, 'owner': False},
    'Menon, Leena':       {'display': 'Dr. Menon',   'pct': 0.35, 'owner': False},
    'Choong, Carissa':    {'display': 'Dr. Choong',  'pct': 0.35, 'owner': False},
    'Benton, Patricia':   {'display': 'Dr. Benton',  'pct': 0.32, 'owner': False},
    'Welter, Erin':       {'display': 'Dr. Welter',  'pct': 0.31, 'owner': False},
    'Patel, Dusayant':    {'display': 'Dr. Patel',   'pct': 0.32, 'owner': False},
    'Bell, Kendra':       {'display': 'Dr. Bell',    'pct': 0.35, 'owner': False},
    'Schrack, Donald':    {'display': 'Dr. Schrack', 'pct': 0.45, 'owner': False},
    'Lugo, Christopher':  {'display': 'Dr. Lugo',    'pct': 0.36, 'owner': True},
}

# Terminated/inactive doctors — tracked but not in active payroll
TERMED_DOCTORS = {
    'Kirk, Kyle':  {'display': 'Dr. Kirk',    'note': 'Terminated'},
    'Ping, Sita':  {'display': 'Dr. Ping',    'note': 'Terminated'},
    'Laws':        {'display': 'Dr. Laws',    'note': 'Terminated'},
}

OFFICE_MAP = {
    # Sheet header text → (display name, abbreviation for per-doctor breakdown)
    'EVERETT':      ('Everett',      'EV'),
    'LAKE STEVENS': ('Lake Stevens', 'LS'),
    'MARYSVILLE':   ('Marysville',  'MV'),
    'MONROE':       ('Monroe',      'MO'),
    'STANWOOD':     ('Stanwood',    'SW'),
}

# X-ray procedure code prefixes — excluded from payNo calculation
XRAY_PREFIXES = ('D02', 'D03')

# Pay periods: (label, start_date, end_date, pay_date)
# Add new periods here as they come up
PAY_PERIODS = [
    ('1.16.26',  '2025-12-27', '2026-01-09', '2026-01-16'),
    ('1.30.26',  '2026-01-10', '2026-01-23', '2026-01-30'),
    ('2.13.26',  '2026-01-24', '2026-02-06', '2026-02-13'),
    ('2.27.26',  '2026-02-07', '2026-02-20', '2026-02-27'),
    ('3.13.26',  '2026-02-21', '2026-03-06', '2026-03-13'),
    ('3.27.26',  '2026-03-07', '2026-03-20', '2026-03-27'),
    ('4.10.26',  '2026-03-21', '2026-04-03', '2026-04-10'),
    ('4.24.26',  '2026-04-04', '2026-04-17', '2026-04-24'),
    ('5.8.26',   '2026-04-18', '2026-05-01', '2026-05-08'),
    ('5.22.26',  '2026-05-02', '2026-05-15', '2026-05-22'),
    ('6.5.26',   '2026-05-16', '2026-05-29', '2026-06-05'),
    ('6.19.26',  '2026-06-30', '2026-06-12', '2026-06-19'),
    ('7.2.26',   '2026-06-13', '2026-06-26', '2026-07-02'),
]


# ---------------------------------------------------------------------------
# EXCEL PARSING — reads Tanya's "Income Allocation Report - Detail"
# ---------------------------------------------------------------------------

def parse_excel(filepath):
    """
    Parse Denticon 'Income Allocation Report - Detail' Excel export.
    Returns list of transaction dicts with: office, provider, alloc_date,
    proc_code, income.
    """
    try:
        import openpyxl
    except ImportError:
        print("ERROR: openpyxl required. Install with: pip install openpyxl")
        sys.exit(1)

    wb = openpyxl.load_workbook(filepath, data_only=True)
    transactions = []

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        office = None
        current_provider = None

        for row in ws.iter_rows(min_row=1, values_only=False):
            # Cell values
            a_val = row[0].value if len(row) > 0 else None
            b_val = row[1].value if len(row) > 1 else None
            c_val = row[2].value if len(row) > 2 else None

            # Detect office from header: "Office: PSPD - EVERETT"
            if a_val and isinstance(a_val, str) and 'Office:' in a_val:
                match = re.search(r'PSPD\s*-\s*(.+)', a_val)
                if match:
                    raw_office = match.group(1).strip().upper()
                    office = raw_office
                continue

            # Detect provider header: "Provider :- Bell, Kendra  DDS  : BELL"
            if b_val and isinstance(b_val, str) and 'Provider :-' in b_val:
                parts = b_val.split(':-')[1].strip()
                # Extract "Last, First" before the double-space or DDS
                provider_match = re.match(r'([^,]+,\s*\S+)', parts)
                if provider_match:
                    current_provider = provider_match.group(1).strip()
                continue

            # Transaction rows have a datetime in column C (Alloc Date)
            if c_val and isinstance(c_val, datetime) and current_provider and office:
                income = row[14].value if len(row) > 14 else None  # Column O
                proc_code = row[12].value if len(row) > 12 else None  # Column M

                if income is not None:
                    transactions.append({
                        'office': office,
                        'provider': current_provider,
                        'alloc_date': c_val,
                        'proc_code': str(proc_code).strip() if proc_code else '',
                        'income': float(income),
                    })

    wb.close()
    return transactions


# ---------------------------------------------------------------------------
# AZURE SQL QUERYING — direct from Denticon database
# ---------------------------------------------------------------------------

def discover_azure_views(conn_str):
    """List all views/tables that might contain income allocation data."""
    import pyodbc
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    print("\n=== SEARCHING FOR INCOME/PAYMENT VIEWS ===")
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME LIKE '%Income%'
           OR TABLE_NAME LIKE '%Alloc%'
           OR TABLE_NAME LIKE '%Payment%'
           OR TABLE_NAME LIKE '%Pay%'
           OR TABLE_NAME LIKE '%Collection%'
           OR TABLE_NAME LIKE '%PAYMNT%'
           OR TABLE_NAME LIKE '%Payroll%'
        ORDER BY TABLE_TYPE, TABLE_NAME
    """)
    rows = cursor.fetchall()
    if rows:
        for r in rows:
            print(f"  [{r.TABLE_TYPE}] {r.TABLE_SCHEMA}.{r.TABLE_NAME}")
    else:
        print("  No matching views found.")

    print("\n=== ALL VIEWS (for reference) ===")
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'VIEW'
        ORDER BY TABLE_NAME
    """)
    for r in cursor.fetchall():
        print(f"  {r.TABLE_SCHEMA}.{r.TABLE_NAME}")

    print("\n=== TABLES WITH 'PGID4951' PREFIX ===")
    cursor.execute("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME LIKE 'PGID4951%'
        ORDER BY TABLE_NAME
    """)
    for r in cursor.fetchall():
        print(f"  {r.TABLE_NAME}")

    conn.close()


def query_azure_income_allocation(conn_str, start_date, end_date):
    """
    Query income allocation data from Azure SQL.

    NOTE: The exact view/table name needs to be discovered first.
    Run with --discover to find it. Once found, update the query below.

    Common Denticon table candidates:
      - PGID4951_INCOMALLOC
      - PGID4951_PAYMNT
      - vw_IncomeAllocation_PBI
      - vw_PaymentDetail
    """
    import pyodbc
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # -----------------------------------------------------------------------
    # IMPORTANT: Update this query once you discover the correct view/table
    # by running: python payroll_refresh.py --discover
    # -----------------------------------------------------------------------
    # Try multiple possible table/view names in order of likelihood
    CANDIDATE_QUERIES = [
        # Candidate 1: PBI view (most likely if Denticon provides one)
        """
        SELECT
            OfficeName,
            ProviderName,
            AllocDate,
            ProcCode,
            Income
        FROM vw_IncomeAllocation_PBI
        WHERE AllocDate >= ? AND AllocDate <= ?
        """,
        # Candidate 2: Direct Denticon payment table
        """
        SELECT
            loc.OfficeName,
            CONCAT(prov.LastName, ', ', prov.FirstName) AS ProviderName,
            pa.AllocDate,
            pa.ProcCode,
            pa.Income
        FROM PGID4951_PAYALLOC pa
        JOIN PGID4951_PROVIDER prov ON pa.ProviderID = prov.ProviderID
        JOIN PGID4951_LOCATION loc ON pa.LocationID = loc.LocationID
        WHERE pa.AllocDate >= ? AND pa.AllocDate <= ?
        """,
        # Candidate 3: Another common pattern
        """
        SELECT
            OfficeName,
            ProviderName,
            AllocationDate AS AllocDate,
            ProcedureCode AS ProcCode,
            Amount AS Income
        FROM vw_IncomeAllocationDetail
        WHERE AllocationDate >= ? AND AllocationDate <= ?
        """,
    ]

    transactions = []
    for i, query in enumerate(CANDIDATE_QUERIES):
        try:
            cursor.execute(query, (start_date, end_date))
            rows = cursor.fetchall()
            print(f"  Azure query candidate {i+1} succeeded: {len(rows)} rows")
            for r in rows:
                office_raw = str(r.OfficeName).upper()
                # Normalize office name
                for key in OFFICE_MAP:
                    if key in office_raw:
                        office_raw = key
                        break
                transactions.append({
                    'office': office_raw,
                    'provider': r.ProviderName,
                    'alloc_date': r.AllocDate,
                    'proc_code': str(r.ProcCode).strip() if r.ProcCode else '',
                    'income': float(r.Income) if r.Income else 0.0,
                })
            break  # Success — stop trying candidates
        except Exception as e:
            print(f"  Azure query candidate {i+1} failed: {e}")
            continue

    conn.close()
    return transactions


# ---------------------------------------------------------------------------
# PROCESSING — transform transactions into payroll.json format
# ---------------------------------------------------------------------------

def find_pay_period(target_date=None):
    """Find the current pay period based on date."""
    if target_date is None:
        target_date = date.today()
    elif isinstance(target_date, str):
        target_date = date.fromisoformat(target_date)

    for label, start, end, pay in PAY_PERIODS:
        s = date.fromisoformat(start)
        e = date.fromisoformat(end)
        if s <= target_date <= e:
            return label, s, e, date.fromisoformat(pay)

    # Default: return the most recent period
    label, start, end, pay = PAY_PERIODS[-1]
    return label, date.fromisoformat(start), date.fromisoformat(end), date.fromisoformat(pay)


def determine_period_for_transactions(transactions):
    """Determine which pay period the transactions belong to based on alloc dates."""
    if not transactions:
        return None

    # Get the date range from transactions
    dates = [t['alloc_date'] for t in transactions if isinstance(t['alloc_date'], datetime)]
    if not dates:
        return None

    min_date = min(dates).date()
    max_date = max(dates).date()
    mid_date = min_date + (max_date - min_date) / 2

    # Find best matching period
    for label, start, end, pay in PAY_PERIODS:
        s = date.fromisoformat(start)
        e = date.fromisoformat(end)
        # Check if the transaction dates overlap with this period
        if min_date <= e and max_date >= s:
            return label, s, e, date.fromisoformat(pay)

    return None


def process_transactions(transactions, period_start, period_end, label, pay_date):
    """
    Transform raw transactions into the payroll.json format that
    payroll.html's loadLiveData() expects.
    """
    today = date.today()
    days_total = (period_end - period_start).days + 1
    days_elapsed = max(1, min(days_total, (today - period_start).days + 1))
    is_live = today <= period_end + timedelta(days=7)  # "live" until 7 days after period ends
    is_closed = today > pay_date

    # Aggregate by provider and office
    provider_data = {}   # provider_name → {coll, xray, offices: {office → amount}}
    office_totals = {}   # office_name → total

    # IMPORTANT: Denticon Income values are SIGNED.
    # Payments received = negative, adjustments = positive.
    # We sum raw values (preserving sign) then take abs() of the NET total.
    # This matches Tanya's report which shows abs(sum), NOT sum(abs).

    # First pass: accumulate raw signed values
    raw_provider = {}    # provider → {total: float, xray: float, offices: {office: float}}
    raw_office = {}      # office → float

    for t in transactions:
        prov = t['provider']
        office = t['office']
        income = t['income']  # Keep sign! Negative = payment received
        proc_code = t['proc_code']
        is_xray = any(proc_code.startswith(p) for p in XRAY_PREFIXES)

        if prov not in raw_provider:
            raw_provider[prov] = {'total': 0.0, 'xray': 0.0, 'offices': {}}
        raw_provider[prov]['total'] += income
        if is_xray:
            raw_provider[prov]['xray'] += income
        if office not in raw_provider[prov]['offices']:
            raw_provider[prov]['offices'][office] = 0.0
        raw_provider[prov]['offices'][office] += income

        if office not in raw_office:
            raw_office[office] = 0.0
        raw_office[office] += income

    # Second pass: convert to absolute values (abs of net sum)
    for prov, data in raw_provider.items():
        provider_data[prov] = {
            'coll': abs(data['total']),
            'xray': abs(data['xray']),
            'offices': {k: abs(v) for k, v in data['offices'].items()},
        }

    for office, total in raw_office.items():
        office_totals[office] = abs(total)

    # Build doctor array (active doctors)
    doctors = []
    for denticon_name, config in DOCTOR_CONFIG.items():
        pdata = provider_data.get(denticon_name, {'coll': 0.0, 'xray': 0.0, 'offices': {}})
        coll = round(pdata['coll'], 2)
        xray = round(pdata['xray'], 2)
        rate = config['pct']

        pay_with = round(coll * rate, 2)
        pay_no = 0.0 if config['owner'] else round((coll - xray) * rate, 2)

        doc_entry = {
            'name': config['display'],
            'pct': rate,
            'coll': coll,
            'payNo': pay_no,
            'payWith': pay_with,
        }

        # Per-office breakdown
        off = {}
        for raw_office, amount in pdata['offices'].items():
            for office_key, (_, abbr) in OFFICE_MAP.items():
                if office_key in raw_office.upper():
                    off[abbr] = round(amount, 2)
                    break
        if off:
            doc_entry['off'] = off

        doctors.append(doc_entry)

    # Sort doctors by collections descending
    doctors.sort(key=lambda d: d['coll'], reverse=True)

    # Build office array
    offices = []
    for raw_office, total in sorted(office_totals.items(), key=lambda x: -x[1]):
        display_name = raw_office
        for office_key, (disp, _) in OFFICE_MAP.items():
            if office_key in raw_office.upper():
                display_name = disp
                break
        offices.append({'name': display_name, 'amt': round(total, 2)})

    # Build termed array
    termed = []
    for denticon_name, config in TERMED_DOCTORS.items():
        pdata = provider_data.get(denticon_name, {'coll': 0.0})
        termed.append({
            'name': config['display'],
            'coll': round(pdata['coll'], 2),
            'note': config['note'],
        })

    # Format dates for display
    def fmt_date(d):
        return d.strftime('%b %-d, %Y')

    def fmt_date_short(d):
        return d.strftime('%b %-d')

    period = {
        'label': label,
        'dates': f"{fmt_date_short(period_start)} – {fmt_date_short(period_end)}, {period_end.year}",
        'payDate': fmt_date(pay_date),
        'status': 'closed' if is_closed else 'live',
        'daysElapsed': days_elapsed,
        'daysTotal': days_total,
        'doctors': doctors,
        'offices': offices,
        'termed': termed,
    }

    return period


# ---------------------------------------------------------------------------
# OUTPUT — write data/payroll.json
# ---------------------------------------------------------------------------

def write_payroll_json(periods_dict, output_path):
    """Write the payroll.json file that payroll.html consumes."""
    payload = {
        'periods': periods_dict,
        'last_updated': datetime.utcnow().isoformat() + 'Z',
        'generated_by': 'payroll_refresh.py',
        'source': 'Denticon Income Allocation Report',
    }

    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(payload, f, indent=2)

    # Print summary
    for key, period in periods_dict.items():
        total_coll = sum(d['coll'] for d in period['doctors'])
        total_pay = sum(d['payNo'] for d in period['doctors'])
        total_saved = sum(d['payWith'] - d['payNo'] for d in period['doctors'])
        print(f"\n  Period {key} ({period['status']}):")
        print(f"    Collections: ${total_coll:,.2f}")
        print(f"    Pay (no xray): ${total_pay:,.2f}")
        print(f"    X-Ray Savings: ${total_saved:,.2f}")
        print(f"    Offices: {len(period['offices'])}")
        print(f"    Active Doctors: {len(period['doctors'])}")
        for d in period['doctors']:
            saved = d['payWith'] - d['payNo']
            print(f"      {d['name']:14s}  coll=${d['coll']:>10,.2f}  payNo=${d['payNo']:>9,.2f}  saved=${saved:>8,.2f}")

    print(f"\n  Written to: {output_path}")
    print(f"  Timestamp: {payload['last_updated']}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='PSPD Payroll Data Pipeline — generates data/payroll.json'
    )
    parser.add_argument(
        '--from-excel', '-e',
        help='Path to Denticon Income Allocation Report Excel file'
    )
    parser.add_argument(
        '--discover',
        action='store_true',
        help='Discover available Azure SQL views/tables for income data'
    )
    parser.add_argument(
        '--output', '-o',
        default='data/payroll.json',
        help='Output path for payroll.json (default: data/payroll.json)'
    )
    parser.add_argument(
        '--period',
        help='Specific pay period label to generate (e.g., "3.13.26"). Default: auto-detect.'
    )
    parser.add_argument(
        '--all-periods',
        action='store_true',
        help='Generate all available periods (Azure SQL only)'
    )

    args = parser.parse_args()

    # Resolve output path relative to repo root
    # When run from repo root: data/payroll.json
    # When run from scripts/: ../data/payroll.json
    output_path = args.output
    if not os.path.isabs(output_path):
        # Try to find repo root (look for payroll.html)
        for candidate in ['.', '..', '../..']:
            if os.path.exists(os.path.join(candidate, 'payroll.html')):
                output_path = os.path.join(candidate, output_path)
                break

    conn_str = os.environ.get('AZURE_SQL_CONN_STR', '')

    # --- Discovery mode ---
    if args.discover:
        if not conn_str:
            print("ERROR: Set AZURE_SQL_CONN_STR environment variable")
            sys.exit(1)
        discover_azure_views(conn_str)
        return

    # --- Excel mode ---
    if args.from_excel:
        filepath = args.from_excel
        if not os.path.exists(filepath):
            print(f"ERROR: File not found: {filepath}")
            sys.exit(1)

        print(f"Reading Excel: {filepath}")
        transactions = parse_excel(filepath)
        print(f"  Parsed {len(transactions)} transaction rows")

        if not transactions:
            print("ERROR: No transactions found in file")
            sys.exit(1)

        # Determine pay period
        period_info = determine_period_for_transactions(transactions)
        if period_info is None:
            print("WARNING: Could not auto-detect pay period from alloc dates")
            if args.period:
                for label, start, end, pay in PAY_PERIODS:
                    if label == args.period:
                        period_info = (label, date.fromisoformat(start),
                                       date.fromisoformat(end), date.fromisoformat(pay))
                        break
            if period_info is None:
                print("ERROR: Specify --period to set the pay period manually")
                sys.exit(1)

        label, start, end, pay_date = period_info
        print(f"  Pay period: {label} ({start} to {end})")

        period = process_transactions(transactions, start, end, label, pay_date)
        write_payroll_json({label: period}, output_path)
        return

    # --- Azure SQL mode ---
    if not conn_str:
        print("ERROR: No data source specified.")
        print("  Use --from-excel <file> for Excel import")
        print("  Or set AZURE_SQL_CONN_STR for Azure SQL")
        sys.exit(1)

    print("Connecting to Azure SQL...")

    if args.all_periods:
        periods_dict = {}
        for label, start, end, pay in PAY_PERIODS:
            s = date.fromisoformat(start)
            e = date.fromisoformat(end)
            # Only query periods that have started
            if s > date.today():
                continue
            print(f"\n  Querying period {label} ({start} to {end})...")
            transactions = query_azure_income_allocation(conn_str, start, end)
            if transactions:
                period = process_transactions(transactions, s, e, label, date.fromisoformat(pay))
                periods_dict[label] = period
            else:
                print(f"    No data for period {label}")

        if periods_dict:
            write_payroll_json(periods_dict, output_path)
        else:
            print("ERROR: No data retrieved for any period")
            sys.exit(1)
    else:
        # Single period (current or specified)
        if args.period:
            for label, start, end, pay in PAY_PERIODS:
                if label == args.period:
                    period_info = (label, date.fromisoformat(start),
                                   date.fromisoformat(end), date.fromisoformat(pay))
                    break
            else:
                print(f"ERROR: Unknown period '{args.period}'")
                sys.exit(1)
        else:
            label, start, end, pay_date = find_pay_period()
            period_info = (label, start, end, pay_date)

        label, start, end, pay_date = period_info
        print(f"  Querying period {label} ({start} to {end})...")
        transactions = query_azure_income_allocation(conn_str, str(start), str(end))

        if not transactions:
            print("ERROR: No transactions returned from Azure SQL")
            print("  Run with --discover to check available tables")
            sys.exit(1)

        period = process_transactions(transactions, start, end, label, pay_date)
        write_payroll_json({label: period}, output_path)


if __name__ == '__main__':
    main()
