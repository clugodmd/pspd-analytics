#!/usr/bin/env python3
"""
doctor_admin.py — PSPD Doctor Administration Script
====================================================
Manages doctor configuration (rates, terminations, reactivations)
by updating data/doctors.json. Called by GitHub Actions workflow dispatch.

Usage:
  # Terminate a doctor
  python scripts/doctor_admin.py --terminate --doctor "Last, First" --last-day 2026-03-15 --note "Resigned"

  # Change a doctor's rate
  python scripts/doctor_admin.py --change-rate --doctor "Last, First" --new-rate 0.35 --effective 2026-04-01

  # Reactivate a terminated doctor
  python scripts/doctor_admin.py --reactivate --doctor "Last, First" --rate 0.35

  # List all doctors
  python scripts/doctor_admin.py --list
"""

import json
import sys
import argparse
from datetime import datetime, date
from pathlib import Path


# ---------------------------------------------------------------------------
# Path to doctors.json (relative to repo root)
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
DOCTORS_JSON = REPO_ROOT / "data" / "doctors.json"


def load_doctors():
    """Load doctors.json. Exit with error if not found."""
    if not DOCTORS_JSON.exists():
        print(f"ERROR: {DOCTORS_JSON} not found", file=sys.stderr)
        sys.exit(1)
    with open(DOCTORS_JSON, 'r') as f:
        return json.load(f)


def save_doctors(data, updated_by="admin"):
    """Save doctors.json with updated timestamp."""
    data["last_updated"] = datetime.utcnow().isoformat() + "Z"
    data["updated_by"] = updated_by
    with open(DOCTORS_JSON, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Saved {DOCTORS_JSON}")


def terminate_doctor(doctor_name, last_day, note=""):
    """Move a doctor from active to terminated."""
    data = load_doctors()

    if doctor_name not in data["doctors"]:
        print(f"ERROR: '{doctor_name}' not found in active doctors", file=sys.stderr)
        print(f"Active doctors: {list(data['doctors'].keys())}", file=sys.stderr)
        sys.exit(1)

    doc = data["doctors"].pop(doctor_name)

    # Close the current rate history entry
    for entry in doc.get("rate_history", []):
        if entry.get("end") is None:
            entry["end"] = last_day

    data["terminated"][doctor_name] = {
        "display": doc["display"],
        "status": "terminated",
        "last_day": last_day,
        "termination_date": date.today().isoformat(),
        "note": note or "Terminated",
        "final_rate": doc["pct"],
        "owner": doc.get("owner", False),
        "pay_basis": doc.get("pay_basis", "collections"),
        "rate_history": doc.get("rate_history", [])
    }

    save_doctors(data, updated_by=f"admin:terminate:{doctor_name}")
    print(f"TERMINATED: {doctor_name} (last day: {last_day})")


def change_rate(doctor_name, new_rate, effective_date):
    """Change a doctor's pay rate with effective date tracking."""
    data = load_doctors()

    if doctor_name not in data["doctors"]:
        print(f"ERROR: '{doctor_name}' not found in active doctors", file=sys.stderr)
        print(f"Active doctors: {list(data['doctors'].keys())}", file=sys.stderr)
        sys.exit(1)

    doc = data["doctors"][doctor_name]
    old_rate = doc["pct"]

    if old_rate == new_rate:
        print(f"WARNING: Rate is already {new_rate} for {doctor_name}", file=sys.stderr)
        sys.exit(0)

    # Close the current rate history entry
    rate_history = doc.get("rate_history", [])
    for entry in rate_history:
        if entry.get("end") is None:
            # End the day before the new rate starts
            eff = date.fromisoformat(effective_date)
            from datetime import timedelta
            entry["end"] = (eff - timedelta(days=1)).isoformat()

    # Add new rate entry
    rate_history.append({
        "pct": new_rate,
        "effective": effective_date,
        "end": None
    })

    doc["pct"] = new_rate
    doc["rate_history"] = rate_history

    save_doctors(data, updated_by=f"admin:rate_change:{doctor_name}")
    print(f"RATE CHANGED: {doctor_name} from {old_rate} -> {new_rate} (effective {effective_date})")


def reactivate_doctor(doctor_name, rate):
    """Move a doctor from terminated back to active."""
    data = load_doctors()

    if doctor_name not in data["terminated"]:
        print(f"ERROR: '{doctor_name}' not found in terminated doctors", file=sys.stderr)
        print(f"Terminated doctors: {list(data['terminated'].keys())}", file=sys.stderr)
        sys.exit(1)

    term = data["terminated"].pop(doctor_name)

    data["doctors"][doctor_name] = {
        "display": term["display"],
        "pct": rate,
        "owner": term.get("owner", False),
        "pay_basis": term.get("pay_basis", "collections"),
        "status": "active",
        "rate_history": term.get("rate_history", []) + [
            {"pct": rate, "effective": date.today().isoformat(), "end": None}
        ]
    }

    save_doctors(data, updated_by=f"admin:reactivate:{doctor_name}")
    print(f"REACTIVATED: {doctor_name} at rate {rate}")


def list_doctors():
    """Print all doctors (active and terminated)."""
    data = load_doctors()

    print("\n=== ACTIVE DOCTORS ===")
    for name, doc in data["doctors"].items():
        status = "OWNER" if doc.get("owner") else "ASSOC"
        print(f"  {doc['display']:15s}  {name:25s}  Rate: {doc['pct']:.0%}  [{status}]")

    print(f"\n=== TERMINATED DOCTORS ===")
    for name, doc in data["terminated"].items():
        days = (date.today() - date.fromisoformat(doc["last_day"])).days
        print(f"  {doc['display']:15s}  {name:25s}  Last: {doc['last_day']}  ({days}d ago)  {doc.get('note','')}")

    print(f"\nLast updated: {data.get('last_updated', 'unknown')}")
    print(f"Updated by: {data.get('updated_by', 'unknown')}")


def main():
    parser = argparse.ArgumentParser(description="PSPD Doctor Administration")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--terminate", action="store_true", help="Terminate a doctor")
    group.add_argument("--change-rate", action="store_true", help="Change a doctor's rate")
    group.add_argument("--reactivate", action="store_true", help="Reactivate a terminated doctor")
    group.add_argument("--list", action="store_true", help="List all doctors")

    parser.add_argument("--doctor", type=str, help="Doctor name (Last, First)")
    parser.add_argument("--last-day", type=str, help="Last working day (YYYY-MM-DD)")
    parser.add_argument("--note", type=str, default="", help="Termination note")
    parser.add_argument("--new-rate", type=float, help="New pay rate (e.g. 0.35)")
    parser.add_argument("--rate", type=float, help="Rate for reactivation")
    parser.add_argument("--effective", type=str, help="Effective date (YYYY-MM-DD)")

    args = parser.parse_args()

    if args.list:
        list_doctors()
        return

    if not args.doctor:
        parser.error("--doctor is required for terminate, change-rate, and reactivate")

    if args.terminate:
        if not args.last_day:
            parser.error("--last-day is required for --terminate")
        # Validate date format
        try:
            date.fromisoformat(args.last_day)
        except ValueError:
            parser.error(f"Invalid date format: {args.last_day}. Use YYYY-MM-DD")
        terminate_doctor(args.doctor, args.last_day, args.note)

    elif args.change_rate:
        if not args.new_rate:
            parser.error("--new-rate is required for --change-rate")
        if not args.effective:
            parser.error("--effective is required for --change-rate")
        # Validate
        try:
            date.fromisoformat(args.effective)
        except ValueError:
            parser.error(f"Invalid date format: {args.effective}. Use YYYY-MM-DD")
        if not (0.0 < args.new_rate < 1.0):
            parser.error(f"Rate must be between 0 and 1, got {args.new_rate}")
        change_rate(args.doctor, args.new_rate, args.effective)

    elif args.reactivate:
        if not args.rate:
            parser.error("--rate is required for --reactivate")
        if not (0.0 < args.rate < 1.0):
            parser.error(f"Rate must be between 0 and 1, got {args.rate}")
        reactivate_doctor(args.doctor, args.rate)


if __name__ == "__main__":
    main()
