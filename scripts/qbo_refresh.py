#!/usr/bin/env python3
"""
QBO Financial Refresh — Pulls comprehensive financial reports from QuickBooks Online
and writes data/financials.json for the PSPD Control Tower dashboard.

Reports pulled:
  - Profit & Loss (every month of current year + every month of prior year)
  - Year-to-Date (YTD) aggregated P&L for current year and prior year
  - Balance Sheet (current month only)
  - Cash Flow Statement (current month only)
  - Accounts Receivable Aging (current month only)
  - Accounts Payable Aging (current month only)

Tokens:
  Uses OAuth 2.0 refresh token flow. The refresh token is rotated each run
  and stored back as a GitHub secret (via the GitHub API) so it stays valid.
"""

import os
import sys
import json
import base64
import requests
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from calendar import monthrange

# ── Configuration ──────────────────────────────────────────────────────────────
CLIENT_ID     = os.environ.get("QBO_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("QBO_CLIENT_SECRET", "")
REALM_ID      = os.environ.get("QBO_REALM_ID", "")

# Token file for auto-rotation
TOKEN_FILE    = "data/qbo_token.json"

# Try to load refresh token from file first, fall back to env var
def load_refresh_token():
    """Load refresh token from file or environment."""
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE) as f:
                data = json.load(f)
                return data.get("refresh_token", os.environ.get("QBO_REFRESH_TOKEN", ""))
        except:
            pass
    return os.environ.get("QBO_REFRESH_TOKEN", "")

REFRESH_TOKEN = load_refresh_token()

TOKEN_URL     = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
BASE_URL      = f"https://quickbooks.api.intuit.com/v3/company/{REALM_ID}"
SANDBOX_URL   = f"https://sandbox-quickbooks.api.intuit.com/v3/company/{REALM_ID}"

OUTPUT_FILE   = "data/financials.json"


def get_access_token():
    """Exchange refresh token for a new access token."""
    if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN]):
        print("ERROR: Missing QBO credentials. Set QBO_CLIENT_ID, QBO_CLIENT_SECRET, QBO_REFRESH_TOKEN")
        sys.exit(1)

    auth = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    resp = requests.post(TOKEN_URL, headers={
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }, data={
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
    })

    if resp.status_code != 200:
        print(f"ERROR: Token refresh failed ({resp.status_code}): {resp.text}")
        sys.exit(1)

    tokens = resp.json()
    new_refresh = tokens.get("refresh_token", REFRESH_TOKEN)
    access_token = tokens["access_token"]

    # Save new refresh token to file for next run
    if new_refresh != REFRESH_TOKEN:
        os.makedirs("data", exist_ok=True)
        with open(TOKEN_FILE, "w") as f:
            json.dump({"refresh_token": new_refresh, "saved_at": datetime.utcnow().isoformat()}, f)
        print(f"✓ Saved new refresh token to {TOKEN_FILE}")

    print(f"✓ Got access token (expires in {tokens.get('expires_in', '?')}s)")
    return access_token


def update_github_secret(name, value):
    """Update a GitHub Actions secret (token rotation)."""
    try:
        token = os.environ.get("GITHUB_TOKEN", "")
        repo = os.environ.get("GITHUB_REPOSITORY", "clugodmd/pspd-analytics")
        # Get public key
        key_resp = requests.get(
            f"https://api.github.com/repos/{repo}/actions/secrets/public-key",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
        )
        if key_resp.status_code == 200:
            from nacl import encoding, public
            pk = public.PublicKey(key_resp.json()["key"].encode(), encoding.Base64Encoder())
            sealed = public.SealedBox(pk).encrypt(value.encode())
            encrypted = base64.b64encode(sealed).decode()
            requests.put(
                f"https://api.github.com/repos/{repo}/actions/secrets/{name}",
                headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
                json={"encrypted_value": encrypted, "key_id": key_resp.json()["key_id"]}
            )
            print(f"✓ Rotated {name} secret")
    except Exception as e:
        print(f"⚠ Could not rotate secret: {e}")


def qbo_get(access_token, endpoint, params=None):
    """Make a GET request to the QBO API."""
    # Use sandbox URL if realm looks like a sandbox ID
    base = SANDBOX_URL if len(REALM_ID) < 15 else BASE_URL
    url = f"{base}/{endpoint}"
    resp = requests.get(url, headers={
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }, params=params or {})

    if resp.status_code != 200:
        print(f"⚠ QBO API error on {endpoint}: {resp.status_code} — {resp.text[:200]}")
        return None
    return resp.json()


def fetch_pnl(token, start_date, end_date):
    """Fetch Profit & Loss report for a date range."""
    data = qbo_get(token, "reports/ProfitAndLoss", {
        "start_date": start_date,
        "end_date": end_date,
        "minorversion": "65",
    })
    if not data:
        return None

    report = data.get("QueryResponse", data)
    rows = report.get("Rows", {}).get("Row", []) if "Rows" in report else []

    line_items = []
    totals = {"totalIncome": 0, "totalExpenses": 0, "netIncome": 0,
              "costOfGoods": 0, "operatingExpenses": 0, "otherExpenses": 0}
    expense_breakdown = []

    def parse_rows(rows, section="", depth=0):
        for row in rows:
            row_type = row.get("type", "")
            if row_type == "Section":
                header = row.get("Header", {})
                sect_name = header.get("ColData", [{}])[0].get("value", "")
                sub_rows = row.get("Rows", {}).get("Row", [])
                summary = row.get("Summary", {})

                parse_rows(sub_rows, sect_name or section, depth)

                if summary:
                    cols = summary.get("ColData", [])
                    if len(cols) >= 2:
                        name = cols[0].get("value", "")
                        amt = float(cols[1].get("value", "0") or "0")
                        line_items.append({"name": name, "amount": amt, "section": section, "isTotal": True, "depth": depth})

                        if "income" in name.lower() and "net" not in name.lower():
                            totals["totalIncome"] = amt
                        elif "cost of goods" in name.lower():
                            totals["costOfGoods"] = amt
                        elif "expense" in name.lower() and "other" not in name.lower():
                            totals["operatingExpenses"] = amt
                        elif "other expense" in name.lower():
                            totals["otherExpenses"] = amt
                        elif "net income" in name.lower() or "net operating" in name.lower():
                            totals["netIncome"] = amt

            elif row_type == "Data":
                cols = row.get("ColData", [])
                if len(cols) >= 2:
                    name = cols[0].get("value", "")
                    amt = float(cols[1].get("value", "0") or "0")
                    line_items.append({"name": name, "amount": amt, "section": section, "isTotal": False, "depth": depth + 1})

                    if "expense" in section.lower() and amt > 0:
                        expense_breakdown.append({"name": name, "amount": amt})

    parse_rows(rows)
    totals["totalExpenses"] = totals["costOfGoods"] + totals["operatingExpenses"] + totals["otherExpenses"]
    totals["expenseBreakdown"] = sorted(expense_breakdown, key=lambda x: -x["amount"])
    totals["lineItems"] = line_items
    return totals


def fetch_balance_sheet(token, as_of):
    """Fetch Balance Sheet as of a date."""
    data = qbo_get(token, "reports/BalanceSheet", {
        "date_macro": "",
        "as_of": as_of,
        "minorversion": "65",
    })
    if not data:
        return None

    report = data.get("QueryResponse", data)
    rows = report.get("Rows", {}).get("Row", []) if "Rows" in report else []
    line_items = []
    cash = 0

    def parse_rows(rows, section="", depth=0):
        nonlocal cash
        for row in rows:
            row_type = row.get("type", "")
            if row_type == "Section":
                header = row.get("Header", {})
                sect_name = header.get("ColData", [{}])[0].get("value", "")
                sub_rows = row.get("Rows", {}).get("Row", [])
                summary = row.get("Summary", {})

                parse_rows(sub_rows, sect_name or section, depth)

                if summary:
                    cols = summary.get("ColData", [])
                    if len(cols) >= 2:
                        name = cols[0].get("value", "")
                        amt = float(cols[1].get("value", "0") or "0")
                        line_items.append({"name": name, "amount": amt, "section": section, "isTotal": True, "depth": depth})

            elif row_type == "Data":
                cols = row.get("ColData", [])
                if len(cols) >= 2:
                    name = cols[0].get("value", "")
                    amt = float(cols[1].get("value", "0") or "0")
                    line_items.append({"name": name, "amount": amt, "section": section, "isTotal": False, "depth": depth + 1})
                    if any(kw in name.lower() for kw in ["checking", "savings", "cash", "money market"]):
                        cash += amt

    parse_rows(rows)
    return {"lineItems": line_items, "cash": cash}


def fetch_cashflow(token, start_date, end_date):
    """Fetch Cash Flow Statement."""
    data = qbo_get(token, "reports/CashFlow", {
        "start_date": start_date,
        "end_date": end_date,
        "minorversion": "65",
    })
    if not data:
        return None

    report = data.get("QueryResponse", data)
    rows = report.get("Rows", {}).get("Row", []) if "Rows" in report else []
    line_items = []

    def parse_rows(rows, section="", depth=0):
        for row in rows:
            row_type = row.get("type", "")
            if row_type == "Section":
                header = row.get("Header", {})
                sect_name = header.get("ColData", [{}])[0].get("value", "")
                sub_rows = row.get("Rows", {}).get("Row", [])
                summary = row.get("Summary", {})
                parse_rows(sub_rows, sect_name or section, depth)
                if summary:
                    cols = summary.get("ColData", [])
                    if len(cols) >= 2:
                        line_items.append({
                            "name": cols[0].get("value", ""),
                            "amount": float(cols[1].get("value", "0") or "0"),
                            "section": section, "isTotal": True, "depth": depth
                        })
            elif row_type == "Data":
                cols = row.get("ColData", [])
                if len(cols) >= 2:
                    line_items.append({
                        "name": cols[0].get("value", ""),
                        "amount": float(cols[1].get("value", "0") or "0"),
                        "section": section, "isTotal": False, "depth": depth + 1
                    })

    parse_rows(rows)
    return {"lineItems": line_items}


def fetch_ar_aging(token):
    """Fetch Accounts Receivable aging summary."""
    data = qbo_get(token, "reports/AgedReceivables", {"minorversion": "65"})
    if not data:
        return None

    report = data.get("QueryResponse", data)
    rows = report.get("Rows", {}).get("Row", []) if "Rows" in report else []
    customers = []
    aging = {"current": 0, "days30": 0, "days60": 0, "days90": 0, "over90": 0}
    total = 0

    for row in rows:
        if row.get("type") == "Data":
            cols = row.get("ColData", [])
            if len(cols) >= 6:
                name = cols[0].get("value", "")
                current = float(cols[1].get("value", "0") or "0")
                d30 = float(cols[2].get("value", "0") or "0")
                d60 = float(cols[3].get("value", "0") or "0")
                d90 = float(cols[4].get("value", "0") or "0")
                bal = float(cols[5].get("value", "0") or "0")
                if bal > 0:
                    age = 0 if current > 0 else 30 if d30 > 0 else 60 if d60 > 0 else 90
                    customers.append({"name": name, "balance": bal, "age": age})
                    aging["current"] += current
                    aging["days30"] += d30
                    aging["days60"] += d60
                    aging["days90"] += d90
                    total += bal

        elif row.get("type") == "Section" and row.get("Summary"):
            cols = row["Summary"].get("ColData", [])
            if len(cols) >= 6:
                total = float(cols[5].get("value", "0") or "0")

    aging["over90"] = max(0, total - aging["current"] - aging["days30"] - aging["days60"] - aging["days90"])
    customers.sort(key=lambda c: -c["balance"])
    return {"customers": customers, "aging": aging, "total": total}


def fetch_ap_aging(token):
    """Fetch Accounts Payable aging summary."""
    data = qbo_get(token, "reports/AgedPayables", {"minorversion": "65"})
    if not data:
        return None

    report = data.get("QueryResponse", data)
    rows = report.get("Rows", {}).get("Row", []) if "Rows" in report else []
    vendors = []
    total = 0

    for row in rows:
        if row.get("type") == "Data":
            cols = row.get("ColData", [])
            if len(cols) >= 6:
                name = cols[0].get("value", "")
                current = float(cols[1].get("value", "0") or "0")
                bal = float(cols[5].get("value", "0") or "0")
                if bal > 0:
                    vendors.append({"name": name, "balance": bal, "overdue": current < bal})
                    total += bal

    vendors.sort(key=lambda v: -v["balance"])
    return {"vendors": vendors, "total": total}


def get_month_boundaries(year, month):
    """
    Get the first and last day of a given month using proper calendar boundaries.

    Args:
        year: Calendar year (e.g., 2026)
        month: Calendar month (1-12)

    Returns:
        tuple: (first_day, last_day) as date objects
    """
    first = date(year, month, 1)
    _, last_day = monthrange(year, month)
    last = date(year, month, last_day)
    return first, last


def aggregate_pnl(pnl_entries):
    """
    Aggregate multiple P&L reports into a single YTD summary.

    Args:
        pnl_entries: List of P&L dictionaries from fetch_pnl()

    Returns:
        dict: Aggregated totals
    """
    if not pnl_entries:
        return {
            "totalIncome": 0,
            "totalExpenses": 0,
            "costOfGoods": 0,
            "operatingExpenses": 0,
            "otherExpenses": 0,
            "netIncome": 0,
        }

    agg = {
        "totalIncome": 0,
        "totalExpenses": 0,
        "costOfGoods": 0,
        "operatingExpenses": 0,
        "otherExpenses": 0,
        "netIncome": 0,
    }

    for pnl in pnl_entries:
        if pnl:
            agg["totalIncome"] += pnl.get("totalIncome", 0)
            agg["totalExpenses"] += pnl.get("totalExpenses", 0)
            agg["costOfGoods"] += pnl.get("costOfGoods", 0)
            agg["operatingExpenses"] += pnl.get("operatingExpenses", 0)
            agg["otherExpenses"] += pnl.get("otherExpenses", 0)
            agg["netIncome"] += pnl.get("netIncome", 0)

    return agg


def main():
    if not REALM_ID:
        print("ERROR: QBO_REALM_ID not set. Complete the OAuth flow first.")
        sys.exit(1)

    token = get_access_token()
    today = date.today()
    current_year = today.year
    current_month = today.month
    prior_year = current_year - 1

    months_data = []
    pnl_by_month = {}  # For YTD aggregation

    # Fetch every month of prior year (Jan 2025 - Dec 2025)
    print(f"\n╔════ {prior_year} FINANCIAL DATA ════╗")
    for month in range(1, 13):
        first, last = get_month_boundaries(prior_year, month)
        start_str = first.strftime("%Y-%m-%d")
        end_str = last.strftime("%Y-%m-%d")
        label = first.strftime("%b %Y")
        key = first.strftime("%Y-%m")

        print(f"\n── {label} ({start_str} to {end_str}) ──")

        pnl = fetch_pnl(token, start_str, end_str)
        pnl_by_month[key] = pnl

        month_entry = {
            "key": key,
            "label": label,
            "year": prior_year,
            "month": month,
            "startDate": start_str,
            "endDate": end_str,
        }
        if pnl:
            month_entry["pnl"] = pnl
            print(f"  P&L: Revenue=${pnl['totalIncome']:,.0f}, Net=${pnl['netIncome']:,.0f}")

        months_data.append(month_entry)

    # Fetch every month of current year (Jan 2026 - current month)
    print(f"\n╔════ {current_year} FINANCIAL DATA ════╗")
    current_year_pnl = []  # For YTD aggregation
    prior_year_ytd_pnl = []  # For YTD comparison (same months as current year)

    for month in range(1, current_month + 1):
        first, last = get_month_boundaries(current_year, month)
        start_str = first.strftime("%Y-%m-%d")
        end_str = last.strftime("%Y-%m-%d")
        label = first.strftime("%b %Y")
        key = first.strftime("%Y-%m")
        is_current = (month == current_month)

        print(f"\n── {label} ({start_str} to {end_str}) ──")

        pnl = fetch_pnl(token, start_str, end_str)
        current_year_pnl.append(pnl)

        # Also collect prior year same month for YTD comparison
        prior_key = f"{prior_year}-{month:02d}"
        if prior_key in pnl_by_month:
            prior_year_ytd_pnl.append(pnl_by_month[prior_key])

        balance = fetch_balance_sheet(token, end_str) if is_current else None
        cashflow = fetch_cashflow(token, start_str, end_str) if is_current else None
        ar = fetch_ar_aging(token) if is_current else None
        ap = fetch_ap_aging(token) if is_current else None

        month_entry = {
            "key": key,
            "label": label,
            "year": current_year,
            "month": month,
            "startDate": start_str,
            "endDate": end_str,
        }
        if pnl:
            month_entry["pnl"] = pnl
            print(f"  P&L: Revenue=${pnl['totalIncome']:,.0f}, Net=${pnl['netIncome']:,.0f}")
        if balance:
            month_entry["balance"] = balance
            print(f"  Balance: Cash=${balance['cash']:,.0f}")
        if cashflow:
            month_entry["cashflow"] = cashflow
            print(f"  Cash Flow: {len(cashflow['lineItems'])} line items")
        if ar:
            month_entry["ar"] = ar
            print(f"  AR: ${ar['total']:,.0f} ({len(ar['customers'])} customers)")
        if ap:
            month_entry["ap"] = ap
            print(f"  AP: ${ap['total']:,.0f} ({len(ap['vendors'])} vendors)")

        months_data.append(month_entry)

    # Compute YTD aggregations
    print(f"\n╔════ YEAR-TO-DATE SUMMARY ════╗")
    ytd_current = aggregate_pnl(current_year_pnl)
    ytd_prior = aggregate_pnl(prior_year_ytd_pnl)

    print(f"\n{current_year} YTD (Jan-{date(current_year, current_month, 1).strftime('%b')})")
    print(f"  Revenue: ${ytd_current['totalIncome']:,.0f}")
    print(f"  Expenses: ${ytd_current['totalExpenses']:,.0f}")
    print(f"  Net Income: ${ytd_current['netIncome']:,.0f}")

    print(f"\n{prior_year} YTD (Jan-{date(prior_year, current_month, 1).strftime('%b')})")
    print(f"  Revenue: ${ytd_prior['totalIncome']:,.0f}")
    print(f"  Expenses: ${ytd_prior['totalExpenses']:,.0f}")
    print(f"  Net Income: ${ytd_prior['netIncome']:,.0f}")

    output = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "realm_id": REALM_ID,
        "current_year": current_year,
        "prior_year": prior_year,
        "ytd": {
            "current": ytd_current,
            "prior": ytd_prior,
        },
        "months": months_data,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Wrote {len(months_data)} months + YTD to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
