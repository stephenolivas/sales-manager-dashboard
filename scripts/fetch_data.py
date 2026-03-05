#!/usr/bin/env python3
"""
Fetch DAILY rep-level metrics from Close CRM.
Writes data.json for the GitHub Pages dashboard.

Strategy (proven to work with Close API):
  1. Paginate ALL meetings from /activity/meeting with _skip/_limit=100
  2. Filter to today's date in Python (date filters are silently ignored by Close)
  3. Classify titles in Python (include/exclude patterns)
  4. Fetch lead data only for meetings that survive filtering (with _fields)
  5. Separately fetch Closed/Won opps for today
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

# Fields to request when fetching individual leads (keeps response small)
LEAD_FIELDS = ",".join([
    "id", "display_name", "status_id",
    f"custom.{CF_FIRST_CALL_SHOW_ID}",
    f"custom.{CF_LEAD_OWNER_ID}",
    f"custom.{CF_QUALIFIED_ID}",
    f"custom.{CF_CALL_DISPOSITION_ID}",
    "opportunities",
])

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
    """Return True if the meeting title qualifies as a first strategy call."""
    if not title:
        return False
    t = title.strip()
    tl = t.lower()

    # Exclude: starts with "Canceled:"
    if tl.startswith("canceled:"):
        return False

    # Exclude: contains any exclude pattern
    for pattern in EXCLUDE_TITLE_CONTAINS:
        if pattern in tl:
            return False

    # Exclude: Anthony Q&A
    if "anthony" in tl and "q&a" in tl:
        return False

    # Must match an include pattern
    for regex in INCLUDE_PATTERNS:
        if regex.search(t):
            return True

    return False


# ── API helpers (using requests.Session for connection reuse) ────────────────

session = None


def init_session():
    global session
    session = requests.Session()
    session.auth = (CLOSE_API_KEY, "")
    session.headers.update({"Content-Type": "application/json"})


def api_get(endpoint, params=None):
    """GET with rate limit handling and retry logic."""
    url = f"{BASE_URL}{endpoint}"
    for attempt in range(5):
        time.sleep(0.5)  # Global throttle: ~120 req/min
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
    """Resolve Lead Owner field (user_id, name, or dict) to rep name."""
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


# ── Step 1: Fetch ALL meetings, filter to today in Python ────────────────────

def fetch_all_meetings_for_today(today_str):
    """Paginate all meetings, filter to today's date in Python.

    Close API date filters on /activity/meeting are silently ignored,
    so we must fetch everything and filter locally.

    IMPORTANT: starts_at is in UTC. A 4pm PST meeting = midnight UTC next day.
    Must convert to PST before comparing dates.
    """
    try:
        from zoneinfo import ZoneInfo
        pst = ZoneInfo("America/Los_Angeles")
    except ImportError:
        pst = timezone(timedelta(hours=-8))

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

    # Filter to today — convert UTC timestamps to PST before comparing
    today_meetings = []
    for m in all_meetings:
        start = m.get("starts_at") or m.get("activity_at") or ""
        if not start:
            continue
        try:
            # Parse ISO datetime and convert to PST
            dt_str = start.replace("Z", "+00:00")
            dt = datetime.fromisoformat(dt_str)
            dt_pst = dt.astimezone(pst)
            if dt_pst.strftime("%Y-%m-%d") == today_str:
                today_meetings.append(m)
        except (ValueError, TypeError):
            # Fallback: raw string comparison
            if start[:10] == today_str:
                today_meetings.append(m)

    print(f"  Meetings scheduled for today (PST): {len(today_meetings)}", flush=True)
    return today_meetings


# ── Step 2: Classify by user + title ─────────────────────────────────────────

def classify_meetings(meetings, user_map):
    """Filter meetings by user exclusion and title patterns.

    Returns list of qualifying meetings.
    """
    excluded_user = 0
    excluded_title = 0
    qualifying = []

    for m in meetings:
        # User exclusion
        user_id = m.get("user_id", "")
        rep_name = user_map.get(user_id, "Unknown")
        if rep_name in EXCLUDE_USERS:
            excluded_user += 1
            continue

        # Title classification
        title = m.get("title", "") or ""

        if not title.strip():
            # Blank title — likely GCal sync issue. Count it (not a follow-up/canceled).
            qualifying.append(m)
            print(f"    Blank title included ({rep_name}, lead={m.get('lead_id', '?')})", flush=True)
        elif is_first_call_meeting(title):
            qualifying.append(m)
        else:
            excluded_title += 1
            print(f"    Excluded title ({rep_name}): {title}", flush=True)

    print(f"  User excluded: {excluded_user}", flush=True)
    print(f"  Title excluded: {excluded_title}", flush=True)
    print(f"  Qualifying first-call meetings: {len(qualifying)}", flush=True)
    return qualifying


# ── Step 3: Fetch lead data for qualifying meetings ──────────────────────────

def fetch_leads_for_meetings(meetings, user_map, name_to_id):
    """For each qualifying meeting, fetch its lead and tally metrics.

    Returns: rep_booked, rep_shown, rep_qualified, rep_crm_filled, rep_crm_total
    """
    rep_booked = {}
    rep_shown = {}
    rep_qualified = {}
    rep_crm_filled = {}
    rep_crm_total = {}

    # Deduplicate by lead_id (one count per lead)
    seen_leads = set()
    lead_cache = {}
    fetch_errors = 0

    for m in meetings:
        lead_id = m.get("lead_id", "")
        if not lead_id or lead_id in seen_leads:
            continue
        seen_leads.add(lead_id)

        # Fetch lead with _fields to keep response small
        if lead_id not in lead_cache:
            try:
                lead_data = api_get(f"/lead/{lead_id}/", {"_fields": LEAD_FIELDS})
                lead_cache[lead_id] = lead_data
            except Exception as e:
                print(f"    ⚠️ Failed to fetch lead {lead_id}: {e}", flush=True)
                fetch_errors += 1
                continue

        lead = lead_cache[lead_id]

        # Exclude by lead status
        status_id = lead.get("status_id", "")
        if status_id in EXCLUDED_LEAD_STATUSES:
            continue

        # Get custom fields (returned as custom.cf_XXX keys with _fields param)
        show_up = lead.get(f"custom.{CF_FIRST_CALL_SHOW_ID}", "")
        owner_raw = lead.get(f"custom.{CF_LEAD_OWNER_ID}", "")
        qualified_val = lead.get(f"custom.{CF_QUALIFIED_ID}", "")
        disposition = lead.get(f"custom.{CF_CALL_DISPOSITION_ID}", "")

        # Also check nested custom dict (Close returns both sometimes)
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

        # Tally booked
        rep_booked[rep_name] = rep_booked.get(rep_name, 0) + 1

        # Tally shown
        if str(show_up).strip().lower() == "yes":
            rep_shown[rep_name] = rep_shown.get(rep_name, 0) + 1

        # Tally qualified
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

        # Opp confidence > 0
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


# ── Step 4: Fetch Closed/Won opps for today ──────────────────────────────────

def fetch_closed_won_today(today_str):
    all_opps = []
    skip = 0
    while True:
        data = api_get("/opportunity/", {
            "status_id": CLOSED_WON_STATUS_ID,
            "date_won__gte": today_str,
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

    print(f"Fetching DAILY data for {today_str} (PST)...", flush=True)

    # Users
    print("  Fetching org users...", flush=True)
    user_map = fetch_org_users()
    name_to_id = {v: k for k, v in user_map.items()}
    print(f"  Found {len(user_map)} users.", flush=True)

    # Closed/Won opps today
    print("  Fetching Closed/Won opportunities for today...", flush=True)
    opps = fetch_closed_won_today(today_str)
    print(f"  Found {len(opps)} Closed/Won opportunities today.", flush=True)

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

    # Meetings: fetch all → filter to today → classify titles → fetch leads
    print("  Fetching all meetings (paginated)...", flush=True)
    today_meetings = fetch_all_meetings_for_today(today_str)

    print("  Classifying by user + title...", flush=True)
    qualifying = classify_meetings(today_meetings, user_map)

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

    # Team totals
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

    print(f"✅ Wrote {output_path}", flush=True)
    print(f"   {len(data['reps'])} reps | {data['total_booked']} booked | "
          f"{data['total_deals']} deals | ${data['total_revenue']:,.2f} revenue", flush=True)
