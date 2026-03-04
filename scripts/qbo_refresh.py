#!/usr/bin/env python3
"""
QBO Financial Refresh — Pulls financial reports from QuickBooks Online
and writes data/financials.json for the PSPD Control Tower dashboard.

Reports pulled:
  - Profit & Loss (current month + 2 prior months)
  - Balance Sheet (current)
  - Cash Flow Statement (current month)
  - Accounts Receivable Aging
  - Accounts Payable Aging

Tokens:
  Uses OAuth 2.0 refresh token flow. The refresh token is rotated each run
  and stored back as a GitHub secret (via the GitHub API) so it stays valid.
"""

import os
import sys
import json
import base64
import requests
from datetime import datetime, timedelta

# ── Configuration ──────────────────────────────────────────────────────────────
CLIENT_ID     = os.environ.get("QBO_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("QBO_CLIENT_SECRET", "")
REFRESH_TOKEN = os.environ.get("QBO_REFRESH_TOKEN", "")
REALM_ID      = os.environ.get("QBO_REALM_ID", "")

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

    # If running in GitHub Actions, update the refresh token secret
    if new_refresh != REFRESH_TOKEN and os.environ.get("GITHUB_TOKEN"):
        update_github_secret("QBO_REFRESH_TOKEN", new_refresh)

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


def main():
    if not REALM_ID:
        print("ERROR: QBO_REALM_ID not set. Complete the OAuth flow first.")
        sys.exit(1)

    token = get_access_token()
    today = datetime.utcnow()

    months_data = []
    for months_ago in range(2, -1, -1):
        dt = today.replace(day=1) - timedelta(days=months_ago * 30)
        first_day = dt.replace(day=1)
        if months_ago == 0:
            last_day = today
        else:
            next_month = first_day.replace(day=28) + timedelta(days=4)
            last_day = next_month - timedelta(days=next_month.day)

        start_str = first_day.strftime("%Y-%m-%d")
        end_str = last_day.strftime("%Y-%m-%d")
        label = first_day.strftime("%b %Y")
        key = first_day.strftime("%Y-%m")

        print(f"\n── {label} ({start_str} to {end_str}) ──")

        pnl = fetch_pnl(token, start_str, end_str)
        balance = fetch_balance_sheet(token, end_str)
        cashflow = fetch_cashflow(token, start_str, end_str)
        ar = fetch_ar_aging(token) if months_ago == 0 else None
        ap = fetch_ap_aging(token) if months_ago == 0 else None

        month_entry = {
            "key": key,
            "label": label,
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

    output = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "realm_id": REALM_ID,
        "months": months_data,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Wrote {len(months_data)} months to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
