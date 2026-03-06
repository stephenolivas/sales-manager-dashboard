#!/usr/bin/env python3
"""
Fetch WEEK-TO-DATE rep-level metrics from Close CRM.
Writes data.json for the GitHub Pages dashboard.

Strategy:
  1. Paginate ALL meetings from /activity/meeting with _skip/_limit=100
  2. Filter to current week (Monday through today PST) in Python
  3. Classify titles in Python (include/exclude patterns)
  4. Fetch lead data only for meetings that survive filtering (with _fields)
  5. Separately fetch Closed/Won opps for the week

Weekly targets per rep:
  Meetings Booked: 20    Close Rate: 30%
  Meetings Shown: 15     QA Score: >7 (TBD)
  Opps Qualified: 10     Avg/Deal: $8k
  Opps Closed Won: 3     CRM Compliance: 100%
  Revenue Booked: $24k   Task Adherence: 100% (TBD)
"""

import json
import os
import sys
import time
import re
import requests
from datetime import datetime, timezone, timedelta
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

# Custom field IDs (lead object)
CF_FIRST_CALL_SHOW_ID     = "cf_OPyvpU45RdvjLqfm8V1VWwNxrGKogEH2IBJmfCj0Uhq"
CF_LEAD_OWNER_ID           = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"
CF_QUALIFIED_ID            = "cf_ZDx7NBQaDzV1yYrFcBMzt6cIYj81dAcswpNN0CQzCPS"
CF_CALL_DISPOSITION_ID     = "cf_n2QvikNfeZ0uWObMsyCJmnXnrbWNLGlSvYiKJTwxTqU"

# Fields to request when fetching individual leads
LEAD_FIELDS = ",".join([
    "id", "display_name", "status_id",
    f"custom.{CF_FIRST_CALL_SHOW_ID}",
    f"custom.{CF_LEAD_OWNER_ID}",
    f"custom.{CF_QUALIFIED_ID}",
    f"custom.{CF_CALL_DISPOSITION_ID}",
    "opportunities",
])

# Weekly targets (per rep)
WEEKLY_TARGETS = {
    "booked": 15,
    "shown": 11,
    "qualified": 8,
    "deals": 3,
    "revenue": 24000,
    "close_rate": 37.5,
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

EXCLUDE_USERS = {
    "Kristin Nelson", "Spencer Reynolds", "Stephen Olivas",
    "Ahmad Bukhari", "Mallory Kent", "Unknown",
}
MANAGER_USERS = {"Joe Dysert"}


# ── Meeting title classification ─────────────────────────────────────────────

INCLUDE_PATTERNS = [
    re.compile(r"vending\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"vendingpren[eu]+rs?\s+consultation", re.IGNORECASE),
    re.compile(r"vendingpren[eu]+rs?\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"new\s+vendingpren[eu]+r\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"vending\s+consult\b", re.IGNORECASE),
]

EXCLUDE_TITLE_CONTAINS = [
    "vending quick discovery",
    "follow-up", "follow up", "fallow up", "f/u",
    "next steps", "rescheduled", "reschedule",
    "enrollment", "silver start up", "bronze enrollment",
    "questions on enrollment",
]


def is_first_call_meeting(title):
    if not title:
        return False
    t = title.strip()
    tl = t.lower()
    if tl.startswith("canceled:"):
        return False
    for pattern in EXCLUDE_TITLE_CONTAINS:
        if pattern in tl:
            return False
    if "anthony" in tl and "q&a" in tl:
        return False
    for regex in INCLUDE_PATTERNS:
        if regex.search(t):
            return True
    return False


# ── API helpers ──────────────────────────────────────────────────────────────

session = None


def init_session():
    global session
    session = requests.Session()
    session.auth = (CLOSE_API_KEY, "")
    session.headers.update({"Content-Type": "application/json"})


def api_get(endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    for attempt in range(5):
        time.sleep(0.5)
        resp = session.get(url, params=params or {})
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 5))
            print(f"    Rate limited, waiting {retry_after}s...", flush=True)
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return resp.json()
    raise Exception(f"Failed after 5 retries: {url}")


def fetch_org_users():
    data = api_get("/user/")
    users = {}
    for u in data.get("data", []):
        first = u.get("first_name", "")
        last = u.get("last_name", "")
        full = f"{first} {last}".strip()
        users[u["id"]] = full
    return users


def resolve_owner(raw_owner, user_map, name_to_id):
    if not raw_owner:
        return "Unknown"
    if isinstance(raw_owner, dict):
        uid = raw_owner.get("id", "")
        if uid in user_map:
            return user_map[uid]
        return raw_owner.get("name", "Unknown")
    owner_str = str(raw_owner).strip()
    if owner_str in user_map:
        return user_map[owner_str]
    if owner_str in name_to_id:
        return owner_str
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


# ── Week helpers ─────────────────────────────────────────────────────────────

def get_week_range(now_pst):
    """Get Monday through today (PST) as date strings."""
    today = now_pst.date()
    # Monday = 0, so weekday() gives days since Monday
    monday = today - timedelta(days=today.weekday())
    dates = []
    d = monday
    while d <= today:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return monday.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"), dates


# ── Step 1: Fetch ALL meetings, filter to this week in Python ────────────────

def fetch_all_meetings_for_week(week_dates):
    """Paginate all meetings, filter to this week's dates (PST)."""
    try:
        from zoneinfo import ZoneInfo
        pst = ZoneInfo("America/Los_Angeles")
    except ImportError:
        pst = timezone(timedelta(hours=-8))

    week_set = set(week_dates)

    all_meetings = []
    skip = 0
    limit = 100

    while True:
        data = api_get("/activity/meeting/", {"_skip": skip, "_limit": limit})
        meetings = data.get("data", [])
        all_meetings.extend(meetings)

        if skip % 1000 == 0 and skip > 0:
            print(f"    Fetched {len(all_meetings)} meetings so far...", flush=True)

        if not data.get("has_more", False):
            break
        skip += limit

    print(f"  Total meetings in org: {len(all_meetings)}", flush=True)

    # Filter to this week — convert UTC timestamps to PST
    week_meetings = []
    for m in all_meetings:
        start = m.get("starts_at") or m.get("activity_at") or ""
        if not start:
            continue
        try:
            dt_str = start.replace("Z", "+00:00")
            dt = datetime.fromisoformat(dt_str)
            dt_pst = dt.astimezone(pst)
            if dt_pst.strftime("%Y-%m-%d") in week_set:
                week_meetings.append(m)
        except (ValueError, TypeError):
            if start[:10] in week_set:
                week_meetings.append(m)

    print(f"  Meetings this week (Mon-today): {len(week_meetings)}", flush=True)
    return week_meetings


# ── Step 2: Classify by user + title ─────────────────────────────────────────

def classify_meetings(meetings, user_map):
    excluded_user = 0
    excluded_title = 0
    qualifying = []

    for m in meetings:
        user_id = m.get("user_id", "")
        rep_name = user_map.get(user_id, "Unknown")
        if rep_name in EXCLUDE_USERS:
            excluded_user += 1
            continue

        title = m.get("title", "") or ""

        if not title.strip():
            qualifying.append(m)
        elif is_first_call_meeting(title):
            qualifying.append(m)
        else:
            excluded_title += 1

    print(f"  User excluded: {excluded_user}", flush=True)
    print(f"  Title excluded: {excluded_title}", flush=True)
    print(f"  Qualifying first-call meetings: {len(qualifying)}", flush=True)
    return qualifying


# ── Step 3: Fetch lead data for qualifying meetings ──────────────────────────

def fetch_leads_for_meetings(meetings, user_map, name_to_id):
    rep_booked = {}
    rep_shown = {}
    rep_qualified = {}
    rep_crm_filled = {}
    rep_crm_total = {}

    seen_leads = set()
    lead_cache = {}
    fetch_errors = 0

    for m in meetings:
        lead_id = m.get("lead_id", "")
        if not lead_id or lead_id in seen_leads:
            continue
        seen_leads.add(lead_id)

        if lead_id not in lead_cache:
            try:
                lead_data = api_get(f"/lead/{lead_id}/", {"_fields": LEAD_FIELDS})
                lead_cache[lead_id] = lead_data
            except Exception as e:
                print(f"    ⚠️ Failed to fetch lead {lead_id}: {e}", flush=True)
                fetch_errors += 1
                continue

        lead = lead_cache[lead_id]

        status_id = lead.get("status_id", "")
        if status_id in EXCLUDED_LEAD_STATUSES:
            continue

        # Custom fields
        show_up = lead.get(f"custom.{CF_FIRST_CALL_SHOW_ID}", "")
        owner_raw = lead.get(f"custom.{CF_LEAD_OWNER_ID}", "")
        qualified_val = lead.get(f"custom.{CF_QUALIFIED_ID}", "")
        disposition = lead.get(f"custom.{CF_CALL_DISPOSITION_ID}", "")

        custom = lead.get("custom", {})
        if not show_up:
            show_up = custom.get(CF_FIRST_CALL_SHOW_ID, "")
        if not owner_raw:
            owner_raw = custom.get(CF_LEAD_OWNER_ID, "")
        if not qualified_val:
            qualified_val = custom.get(CF_QUALIFIED_ID, "")
        if not disposition:
            disposition = custom.get(CF_CALL_DISPOSITION_ID, "")

        rep_name = resolve_owner(owner_raw, user_map, name_to_id)
        if rep_name in EXCLUDE_USERS:
            continue

        rep_booked[rep_name] = rep_booked.get(rep_name, 0) + 1

        if str(show_up).strip().lower() == "yes":
            rep_shown[rep_name] = rep_shown.get(rep_name, 0) + 1

        if str(qualified_val).strip().lower() == "yes":
            rep_qualified[rep_name] = rep_qualified.get(rep_name, 0) + 1

        # CRM Compliance: 4 fields per lead
        crm_checks = 4
        crm_filled = 0
        if is_field_filled(show_up):
            crm_filled += 1
        if is_field_filled(disposition):
            crm_filled += 1
        if is_field_filled(qualified_val):
            crm_filled += 1

        opp_confidence_filled = False
        for opp in lead.get("opportunities", []):
            if opp.get("pipeline_id") == PIPELINE_ID:
                confidence = opp.get("confidence", 0) or 0
                if confidence > 0:
                    opp_confidence_filled = True
                    break
        if opp_confidence_filled:
            crm_filled += 1

        rep_crm_filled[rep_name] = rep_crm_filled.get(rep_name, 0) + crm_filled
        rep_crm_total[rep_name] = rep_crm_total.get(rep_name, 0) + crm_checks

    if fetch_errors:
        print(f"  ⚠️ {fetch_errors} lead fetch errors", flush=True)

    return rep_booked, rep_shown, rep_qualified, rep_crm_filled, rep_crm_total


# ── Step 4: Fetch Closed/Won opps for the week ──────────────────────────────

def fetch_closed_won_week(monday_str, today_str):
    all_opps = []
    skip = 0
    while True:
        data = api_get("/opportunity/", {
            "status_id": CLOSED_WON_STATUS_ID,
            "date_won__gte": monday_str,
            "date_won__lte": today_str,
            "_skip": skip,
            "_limit": 100,
        })
        opps = data.get("data", [])
        all_opps.extend(opps)
        if not data.get("has_more", False):
            break
        skip += 100
    return [o for o in all_opps if o.get("pipeline_id") == PIPELINE_ID]


# ── Main ─────────────────────────────────────────────────────────────────────

def build_dashboard_data():
    if not CLOSE_API_KEY:
        print("ERROR: CLOSE_API_KEY not set.", file=sys.stderr, flush=True)
        sys.exit(1)

    init_session()

    now_utc = datetime.now(timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        pst = ZoneInfo("America/Los_Angeles")
    except ImportError:
        pst = timezone(timedelta(hours=-8))
    now = now_utc.astimezone(pst)
    today_str = now.strftime("%Y-%m-%d")

    monday_str, today_str, week_dates = get_week_range(now)
    day_of_week = now.date().weekday() + 1  # 1=Mon, 5=Fri

    print(f"Fetching WTD data: {monday_str} through {today_str} (day {day_of_week} of week)...", flush=True)

    # Users
    print("  Fetching org users...", flush=True)
    user_map = fetch_org_users()
    name_to_id = {v: k for k, v in user_map.items()}
    print(f"  Found {len(user_map)} users.", flush=True)

    # Closed/Won opps this week
    print("  Fetching Closed/Won opportunities for the week...", flush=True)
    opps = fetch_closed_won_week(monday_str, today_str)
    print(f"  Found {len(opps)} Closed/Won opportunities this week.", flush=True)

    rep_revenue = {}
    rep_deals = {}
    seen_opp_leads = set()

    for opp in opps:
        user_id = opp.get("user_id")
        rep_name = user_map.get(user_id, "Unknown")
        if rep_name in EXCLUDE_USERS:
            continue
        value_dollars = (opp.get("value", 0) or 0) / 100
        lead_id = opp.get("lead_id", "")
        rep_revenue[rep_name] = rep_revenue.get(rep_name, 0) + value_dollars
        lead_key = f"{rep_name}:{lead_id}"
        if lead_key not in seen_opp_leads:
            rep_deals[rep_name] = rep_deals.get(rep_name, 0) + 1
            seen_opp_leads.add(lead_key)

    # Meetings: fetch all → filter to this week → classify → fetch leads
    print("  Fetching all meetings (paginated)...", flush=True)
    week_meetings = fetch_all_meetings_for_week(week_dates)

    print("  Classifying by user + title...", flush=True)
    qualifying = classify_meetings(week_meetings, user_map)

    print(f"  Fetching lead data for {len(qualifying)} qualifying meetings...", flush=True)
    rep_booked, rep_shown, rep_qualified, rep_crm_filled, rep_crm_total = \
        fetch_leads_for_meetings(qualifying, user_map, name_to_id)
    print(f"  Final counts by {len(rep_booked)} reps.", flush=True)

    # Build per-rep data
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

    # Team totals — manager excluded from meeting/shown/qualified/CRM,
    # but included for revenue and deals
    non_mgr = [r for r in reps if not r["is_manager"]]
    num_reps = len(non_mgr)

    total_booked = sum(r["booked"] for r in non_mgr)
    total_shown = sum(r["shown"] for r in non_mgr)
    total_qualified = sum(r["qualified"] for r in non_mgr)
    total_crm_filled = sum(r["crm_filled"] for r in non_mgr)
    total_crm_total = sum(r["crm_total"] for r in non_mgr)

    # Revenue and deals include everyone (including manager)
    total_deals = sum(r["deals"] for r in reps)
    total_revenue = sum(r["revenue"] for r in reps)
    total_avg_rev = round(total_revenue / total_deals, 2) if total_deals > 0 else None

    # Team targets = individual target × number of non-manager reps
    team_targets = {
        "booked": WEEKLY_TARGETS["booked"] * num_reps,
        "shown": WEEKLY_TARGETS["shown"] * num_reps,
        "qualified": WEEKLY_TARGETS["qualified"] * num_reps,
        "deals": WEEKLY_TARGETS["deals"] * num_reps,
        "revenue": WEEKLY_TARGETS["revenue"] * num_reps,
        "close_rate": WEEKLY_TARGETS["close_rate"],  # same % target
        "avg_rev_per_deal": WEEKLY_TARGETS["avg_rev_per_deal"],  # same $ target
        "crm_compliance": WEEKLY_TARGETS["crm_compliance"],  # same % target
    }

    # Week label: "Mar 2 – Mar 7, 2026"
    mon_dt = datetime.strptime(monday_str, "%Y-%m-%d")
    fri_dt = mon_dt + timedelta(days=4)
    week_label = f"{mon_dt.strftime('%b %d')} – {fri_dt.strftime('%b %d, %Y')}"

    return {
        "updated_at": now.strftime("%Y-%m-%d %I:%M %p PST"),
        "week_label": week_label,
        "monday_str": monday_str,
        "today_str": today_str,
        "day_of_week": day_of_week,
        "num_reps": num_reps,
        "targets": WEEKLY_TARGETS,
        "team_targets": team_targets,
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

    # ── Weekly archive: save snapshot keyed by Monday's date ───────────
    # Overwrites throughout the week; last run Friday = final snapshot
    archive_dir = os.path.join(repo_root, "archives")
    os.makedirs(archive_dir, exist_ok=True)

    monday_str = data["monday_str"]  # e.g. "2026-03-03"
    archive_path = os.path.join(archive_dir, f"data_week_{monday_str}.json")
    with open(archive_path, "w") as f:
        json.dump(data, f, indent=2)

    index_path = os.path.join(archive_dir, "index.json")
    try:
        with open(index_path, "r") as f:
            index_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        index_data = {}

    # Migrate from old daily format if needed
    if "weeks" not in index_data:
        index_data = {"weeks": []}

    if monday_str not in index_data["weeks"]:
        index_data["weeks"].append(monday_str)
        index_data["weeks"].sort(reverse=True)

    with open(index_path, "w") as f:
        json.dump(index_data, f, indent=2)

    print(f"✅ Wrote {output_path}", flush=True)
    print(f"📁 Archived week of {monday_str} to {archive_path}", flush=True)
    print(f"   {len(data['reps'])} reps | {data['total_booked']} booked | "
          f"{data['total_deals']} deals | ${data['total_revenue']:,.2f} revenue", flush=True)
