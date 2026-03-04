#!/usr/bin/env python3
"""
Fetch DAILY rep-level metrics from Close CRM.
Writes data.json for the GitHub Pages dashboard.

Daily metrics per rep (with targets):
  1. New Meetings Booked — target 4
  2. New Meetings Shown — target 3
  3. New Opps Qualified — target 2
  4. Closed Won Opps — target 1
  5. Revenue Booked — target $5k
  6. Close Rate — target 30%
  7. QA Score — target >7 (TBD)
  8. Avg Revenue per Deal — target $8k
  9. CRM Compliance — target 100%
 10. Task Adherence — target 100% (TBD)
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError
from base64 import b64encode
from calendar import monthrange

# ── Config ───────────────────────────────────────────────────────────────────

CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY", "")
BASE_URL = "https://api.close.com/api/v1"

PIPELINE_ID = "pipe_78hyBUVS7IKikGEmstObu1"
CLOSED_WON_STATUS_ID = "stat_WnFc0uhjcjV0cc3bVzdFVqDz7av6rbsOmOvHUsO6s03"

EXCLUDED_LEAD_STATUSES = {
    "stat_hWIGHjzyNpl4YjIFSFz3VK4fp2ny10SFJLKAihmo4KT",  # Canceled (by Lead)
    "stat_YV4ZngDB4IGjLjlOf0YTFEWuKZJ6fhNxVkzQkvKYfdB",  # Outside the US
}

# Custom field IDs and display names (lead object)
CF_FIRST_CALL_BOOKED_ID   = "cf_JsJZIVh7QDcFQBXr4cTRBxf1AkREpLdsKiZB4AEJ8Xh"
CF_FIRST_CALL_BOOKED_NAME = "First Call Booked Date"

CF_FIRST_CALL_SHOW_ID     = "cf_OPyvpU45RdvjLqfm8V1VWwNxrGKogEH2IBJmfCj0Uhq"
CF_FIRST_CALL_SHOW_NAME   = "First Call Show Up (Opp)"

CF_LEAD_OWNER_ID           = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"
CF_LEAD_OWNER_NAME         = "Lead Owner"

CF_QUALIFIED_ID            = "cf_ZDx7NBQaDzV1yYrFcBMzt6cIYj81dAcswpNN0CQzCPS"
CF_QUALIFIED_NAME          = "Qualified (Opp)"

CF_CALL_DISPOSITION_ID     = "cf_n2QvikNfeZ0uWObMsyCJmnXnrbWNLGlSvYiKJTwxTqU"
CF_CALL_DISPOSITION_NAME   = "Todays Call Disposition (Opp)"

# Daily targets (per rep)
DAILY_TARGETS = {
    "booked": 4,
    "shown": 3,
    "qualified": 2,
    "deals": 1,
    "revenue": 5000,
    "close_rate": 30,
    "qa_score": 7,
    "avg_rev_per_deal": 8000,
    "crm_compliance": 100,
    "task_adherence": 100,
}

REP_QUOTAS = {
    "Christian Hartwell": 100_000,
    "Lyle Hubbard": 100_000,
    "Ategeka Musinguzi": 100_000,
    "Scott Seymour": 100_000,
    "Eric Piccione": 100_000,
    "Jordan Humphrey": 75_000,
    "Jason Aaron": 75_000,
    "Robin Perkins": 75_000,
    "William Chase": 75_000,
    "Ryan Jones": 75_000,
    "John Kirk": 75_000,
    "Jake Skinner": 75_000,
    "Vince Bartolini": 50_000,
    "Julia Scaroni": 50_000,
    "Elvis Ellis": 50_000,
    "Chris Wanke": 50_000,
    "Andrea Shoop": 50_000,
    "Joe Dysert": 0,
}

EXCLUDE_USERS = {"Kristin Nelson", "Mallory Kent", "Unknown", "Ahmad Bukhari"}
MANAGER_USERS = {"Joe Dysert"}


# ── API helpers ──────────────────────────────────────────────────────────────

def api_get(endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    if params:
        url = f"{url}?{urlencode(params)}"

    auth = b64encode(f"{CLOSE_API_KEY}:".encode()).decode()
    req = Request(url, headers={
        "Authorization": f"Basic {auth}",
        "Accept": "application/json",
    })

    try:
        with urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        body = e.read().decode() if e.fp else ""
        print(f"API error {e.code} for {url}: {body}", file=sys.stderr)
        raise


def fetch_org_users():
    data = api_get("/user/")
    users = {}
    for u in data.get("data", []):
        first = u.get("first_name", "")
        last = u.get("last_name", "")
        full = f"{first} {last}".strip()
        users[u["id"]] = full
    return users


def get_custom_value(custom_dict, field_id, field_name):
    val = custom_dict.get(field_name)
    if val is not None:
        return val
    val = custom_dict.get(field_id)
    if val is not None:
        return val
    val = custom_dict.get(f"custom.{field_id}")
    if val is not None:
        return val
    return ""


def resolve_owner_to_name(owner_raw, user_map, name_to_id):
    if not owner_raw:
        return "Unknown"
    if isinstance(owner_raw, dict):
        uid = owner_raw.get("id", "")
        if uid in user_map:
            return user_map[uid]
        return owner_raw.get("name", "Unknown")
    owner_str = str(owner_raw).strip()
    if owner_str in user_map:
        return user_map[owner_str]
    if owner_str in name_to_id:
        return owner_str
    for rep_name in REP_QUOTAS:
        if owner_str == rep_name:
            return rep_name
    return owner_str if owner_str else "Unknown"


def safe_pct(num, den):
    if not den:
        return None
    return round(num / den * 100, 1)


def is_field_filled(value):
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    return True


# ── Data fetchers ────────────────────────────────────────────────────────────

def fetch_closed_won_today(today_str):
    all_opps = []
    skip = 0
    limit = 100
    while True:
        params = {
            "status_id": CLOSED_WON_STATUS_ID,
            "date_won__gte": today_str,
            "date_won__lte": today_str,
            "_skip": str(skip),
            "_limit": str(limit),
        }
        data = api_get("/opportunity/", params)
        opps = data.get("data", [])
        all_opps.extend(opps)
        if not data.get("has_more", False):
            break
        skip += limit
    return [o for o in all_opps if o.get("pipeline_id") == PIPELINE_ID]


def fetch_leads_booked_today(today_str, user_map, name_to_id):
    query_str = (
        f'"First Call Booked Date" >= "{today_str}" '
        f'"First Call Booked Date" <= "{today_str}"'
    )

    all_leads = []
    skip = 0
    limit = 200
    while True:
        params = {
            "query": query_str,
            "_skip": str(skip),
            "_limit": str(limit),
        }
        data = api_get("/lead/", params)
        leads = data.get("data", [])
        all_leads.extend(leads)
        if not data.get("has_more", False):
            break
        skip += limit

    print(f"  Raw leads returned for today: {len(all_leads)}")

    rep_booked = {}
    rep_shown = {}
    rep_qualified = {}
    rep_crm_filled = {}
    rep_crm_total = {}
    excluded_count = 0

    for lead in all_leads:
        lead_status_id = lead.get("status_id", "")
        if lead_status_id in EXCLUDED_LEAD_STATUSES:
            excluded_count += 1
            continue

        custom = lead.get("custom", {})
        merged = {}
        merged.update(custom)
        for k, v in lead.items():
            if k.startswith("custom."):
                merged[k] = v
                merged[k.replace("custom.", "")] = v

        owner_raw = get_custom_value(merged, CF_LEAD_OWNER_ID, CF_LEAD_OWNER_NAME)
        rep_name = resolve_owner_to_name(owner_raw, user_map, name_to_id)

        if rep_name in EXCLUDE_USERS:
            continue

        rep_booked[rep_name] = rep_booked.get(rep_name, 0) + 1

        show_up = get_custom_value(merged, CF_FIRST_CALL_SHOW_ID, CF_FIRST_CALL_SHOW_NAME)
        if str(show_up).strip().lower() == "yes":
            rep_shown[rep_name] = rep_shown.get(rep_name, 0) + 1

        qualified_val = get_custom_value(merged, CF_QUALIFIED_ID, CF_QUALIFIED_NAME)
        if str(qualified_val).strip().lower() == "yes":
            rep_qualified[rep_name] = rep_qualified.get(rep_name, 0) + 1

        # CRM Compliance (4 fields per lead)
        crm_checks = 0
        crm_filled = 0

        crm_checks += 1
        if is_field_filled(show_up):
            crm_filled += 1

        disposition = get_custom_value(merged, CF_CALL_DISPOSITION_ID, CF_CALL_DISPOSITION_NAME)
        crm_checks += 1
        if is_field_filled(disposition):
            crm_filled += 1

        crm_checks += 1
        if is_field_filled(qualified_val):
            crm_filled += 1

        crm_checks += 1
        opp_confidence_filled = False
        opportunities = lead.get("opportunities", [])
        for opp in opportunities:
            if opp.get("pipeline_id") == PIPELINE_ID:
                confidence = opp.get("confidence", 0) or 0
                if confidence > 0:
                    opp_confidence_filled = True
                    break
        if opp_confidence_filled:
            crm_filled += 1

        rep_crm_filled[rep_name] = rep_crm_filled.get(rep_name, 0) + crm_filled
        rep_crm_total[rep_name] = rep_crm_total.get(rep_name, 0) + crm_checks

    print(f"  Excluded {excluded_count} leads (Canceled/Outside US)")
    print(f"  Counting {len(all_leads) - excluded_count} leads after exclusions")

    return rep_booked, rep_shown, rep_qualified, rep_crm_filled, rep_crm_total


# ── Main ─────────────────────────────────────────────────────────────────────

def build_dashboard_data():
    if not CLOSE_API_KEY:
        print("ERROR: CLOSE_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    now_utc = datetime.now(timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        pst = ZoneInfo("America/Los_Angeles")
    except ImportError:
        pst = timezone(timedelta(hours=-8))
    now = now_utc.astimezone(pst)

    today_str = now.strftime("%Y-%m-%d")
    print(f"Fetching DAILY data for {today_str} (PST)...")

    # Step 1: User map
    print("  Fetching org users...")
    user_map = fetch_org_users()
    name_to_id = {v: k for k, v in user_map.items()}
    print(f"  Found {len(user_map)} users.")

    # Step 2: Closed/Won opportunities TODAY
    print("  Fetching Closed/Won opportunities for today...")
    opps = fetch_closed_won_today(today_str)
    print(f"  Found {len(opps)} Closed/Won opportunities today.")

    rep_revenue = {}
    rep_deals = {}
    seen_leads = set()

    for opp in opps:
        user_id = opp.get("user_id")
        rep_name = user_map.get(user_id, "Unknown")
        if rep_name in EXCLUDE_USERS:
            continue

        value_dollars = (opp.get("value", 0) or 0) / 100
        lead_id = opp.get("lead_id", "")

        rep_revenue[rep_name] = rep_revenue.get(rep_name, 0) + value_dollars

        lead_key = f"{rep_name}:{lead_id}"
        if lead_key not in seen_leads:
            rep_deals[rep_name] = rep_deals.get(rep_name, 0) + 1
            seen_leads.add(lead_key)

    # Step 3: Meeting funnel + CRM compliance for today
    print("  Fetching leads booked today...")
    rep_booked, rep_shown, rep_qualified, rep_crm_filled, rep_crm_total = \
        fetch_leads_booked_today(today_str, user_map, name_to_id)
    print(f"  Meetings booked today by {len(rep_booked)} reps.")

    # Step 4: Build per-rep data
    all_rep_names = set()
    all_rep_names.update(rep_revenue.keys())
    all_rep_names.update(rep_booked.keys())
    all_rep_names.update(REP_QUOTAS.keys())
    all_rep_names -= EXCLUDE_USERS

    reps = []
    for name in all_rep_names:
        revenue = rep_revenue.get(name, 0)
        deals = rep_deals.get(name, 0)
        booked = rep_booked.get(name, 0)
        shown = rep_shown.get(name, 0)
        qualified = rep_qualified.get(name, 0)
        crm_filled = rep_crm_filled.get(name, 0)
        crm_total = rep_crm_total.get(name, 0)

        avg_rev = round(revenue / deals, 2) if deals > 0 else None

        reps.append({
            "name": name,
            "booked": booked,
            "shown": shown,
            "qualified": qualified,
            "deals": deals,
            "revenue": round(revenue, 2),
            "close_rate": safe_pct(deals, booked),
            "qa_score": None,
            "avg_rev_per_deal": avg_rev,
            "crm_compliance": safe_pct(crm_filled, crm_total),
            "crm_filled": crm_filled,
            "crm_total": crm_total,
            "task_adherence": None,
            "is_manager": name in MANAGER_USERS,
        })

    reps.sort(key=lambda r: r["booked"], reverse=True)

    # Step 5: Team totals
    total_booked = sum(r["booked"] for r in reps)
    total_shown = sum(r["shown"] for r in reps)
    total_qualified = sum(r["qualified"] for r in reps)
    total_deals = sum(r["deals"] for r in reps)
    total_revenue = sum(r["revenue"] for r in reps)
    total_crm_filled = sum(r["crm_filled"] for r in reps)
    total_crm_total = sum(r["crm_total"] for r in reps)
    total_avg_rev = round(total_revenue / total_deals, 2) if total_deals > 0 else None

    return {
        "updated_at": now.strftime("%Y-%m-%d %I:%M %p PST"),
        "date_label": now.strftime("%A, %B %d, %Y"),
        "date_str": today_str,
        "targets": DAILY_TARGETS,
        "total_booked": total_booked,
        "total_shown": total_shown,
        "total_qualified": total_qualified,
        "total_deals": total_deals,
        "total_revenue": round(total_revenue, 2),
        "team_close_rate": safe_pct(total_deals, total_booked),
        "team_avg_rev_per_deal": total_avg_rev,
        "team_crm_compliance": safe_pct(total_crm_filled, total_crm_total),
        "reps": reps,
    }


if __name__ == "__main__":
    data = build_dashboard_data()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    output_path = os.path.join(repo_root, "data.json")

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"✅ Wrote {output_path}")
    print(f"   {len(data['reps'])} reps | {data['total_booked']} booked | "
          f"{data['total_deals']} deals | ${data['total_revenue']:,.2f} revenue")
