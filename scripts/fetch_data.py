#!/usr/bin/env python3
"""
Fetch rep-level meeting funnel + close rate metrics from Close CRM.
Writes data.json for the GitHub Pages dashboard.
"""

import json
import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from calendar import monthrange

# ── Config ───────────────────────────────────────────────────────────────────

API_KEY = os.environ["CLOSE_API_KEY"]
BASE_URL = "https://api.close.com/api/v1"
AUTH = (API_KEY, "")
TZ = ZoneInfo("America/Los_Angeles")

PIPELINE_ID = "pipe_78hyBUVS7IKikGEmstObu1"
CLOSED_WON_STATUS = "stat_WnFc0uhjcjV0cc3bVzdFVqDz7av6rbsOmOvHUsO6s03"

# Custom fields (Lead object)
CF_FIRST_CALL_BOOKED = "cf_JsJZIVh7QDcFQBXr4cTRBxf1AkREpLdsKiZB4AEJ8Xh"
CF_FIRST_CALL_SHOW = "cf_OPyvpU45RdvjLqfm8V1VWwNxrGKogEH2IBJmfCj0Uhq"
CF_LEAD_OWNER = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"
CF_QUALIFIED = "cf_ZDx7NBQaDzV1yYrFcBMzt6cIYj81dAcswpNN0CQzCPS"

# Lead statuses to EXCLUDE from meeting metrics
EXCLUDED_STATUSES = [
    "stat_hWIGHjzyNpl4YjIFSFz3VK4fp2ny10SFJLKAihmo4KT",  # Canceled (by Lead)
    "stat_YV4ZngDB4IGjLjlOf0YTFEWuKZJ6fhNxVkzQkvKYfdB",  # Outside the US
]

# Reps to exclude from dashboard
EXCLUDED_NAMES = {"Kristin Nelson", "Mallory Kent", "Unknown", "Ahmad Bukhari"}

# Rep quotas (monthly)
REP_QUOTAS = {
    "Christian Hartwell": 100000,
    "Lyle Hubbard": 100000,
    "Ategeka Musinguzi": 100000,
    "Scott Seymour": 100000,
    "Eric Piccione": 100000,
    "Jordan Humphrey": 75000,
    "Jason Aaron": 75000,
    "Robin Perkins": 75000,
    "William Chase": 75000,
    "Ryan Jones": 75000,
    "John Kirk": 75000,
    "Jake Skinner": 75000,
    "Vince Bartolini": 50000,
    "Julia Scaroni": 50000,
    "Elvis Ellis": 50000,
    "Chris Wanke": 50000,
    "Andrea Shoop": 50000,
    "Joe Dysert": 0,
}

TEAM_QUOTA = 906000

MANAGERS = {"Joe Dysert"}

# ── Helpers ──────────────────────────────────────────────────────────────────

def close_get(endpoint, params=None):
    """GET from Close API with pagination support."""
    url = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, auth=AUTH, params=params or {})
    resp.raise_for_status()
    return resp.json()


def close_get_all(endpoint, params=None):
    """Paginate through all results from a Close API list endpoint."""
    params = dict(params or {})
    params.setdefault("_limit", 200)
    params["_skip"] = 0
    results = []
    while True:
        data = close_get(endpoint, params)
        results.extend(data.get("data", []))
        if not data.get("has_more", False):
            break
        params["_skip"] += params["_limit"]
    return results


def build_user_map():
    """Map user IDs → display names."""
    users = close_get("user")
    user_map = {}
    for u in users.get("data", []):
        uid = u["id"]
        name = f"{u.get('first_name', '')} {u.get('last_name', '')}".strip()
        user_map[uid] = name
    return user_map


def resolve_owner(raw_owner, user_map):
    """Resolve Lead Owner field which may be user_id or display name."""
    if not raw_owner:
        return None
    if raw_owner.startswith("user_"):
        return user_map.get(raw_owner, raw_owner)
    return raw_owner


def working_days_in_month(year, month):
    """Count Mon-Fri days in a month."""
    _, num_days = monthrange(year, month)
    count = 0
    for d in range(1, num_days + 1):
        dt = datetime(year, month, d)
        if dt.weekday() < 5:
            count += 1
    return count


def working_days_elapsed(year, month, today_day):
    """Count Mon-Fri days elapsed so far (inclusive of today)."""
    count = 0
    for d in range(1, today_day + 1):
        dt = datetime(year, month, d)
        if dt.weekday() < 5:
            count += 1
    return count


def safe_pct(numerator, denominator):
    """Return percentage rounded to 1 decimal, or None if denominator is 0."""
    if not denominator:
        return None
    return round(numerator / denominator * 100, 1)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now(TZ)
    year, month, day = now.year, now.month, now.day
    _, days_in_month = monthrange(year, month)

    month_start = f"{year}-{month:02d}-01"
    month_end_date = datetime(year, month, days_in_month) + timedelta(days=1)
    month_end = f"{month_end_date.year}-{month_end_date.month:02d}-{month_end_date.day:02d}"
    today_str = f"{year}-{month:02d}-{day:02d}"

    wd_total = working_days_in_month(year, month)
    wd_elapsed = working_days_elapsed(year, month, day)
    pct_month = safe_pct(wd_elapsed, wd_total) or 0

    user_map = build_user_map()

    # ── 1. Fetch Closed/Won Opportunities ────────────────────────────────
    opps = close_get_all("opportunity", {
        "status_id": CLOSED_WON_STATUS,
        "pipeline_id": PIPELINE_ID,
        "date_won__gte": month_start,
        "date_won__lt": month_end,
    })

    # Revenue + deals per rep (one deal per unique lead per rep)
    rep_revenue = {}
    rep_deals = {}  # rep → set of lead_ids
    today_revenue = 0.0
    today_deals = 0

    for opp in opps:
        value = (opp.get("value") or 0) / 100.0  # cents → dollars
        assigned = opp.get("user_name") or user_map.get(opp.get("assigned_to", ""), "Unknown")
        lead_id = opp.get("lead_id", "")
        date_won = opp.get("date_won", "")

        if assigned in EXCLUDED_NAMES:
            continue

        rep_revenue[assigned] = rep_revenue.get(assigned, 0) + value
        if assigned not in rep_deals:
            rep_deals[assigned] = set()
        rep_deals[assigned].add(lead_id)

        if date_won == today_str:
            today_revenue += value
            today_deals += 1

    # ── 2. Fetch Meeting Metrics (leads with First Call Booked this month) ─
    # Use GET-based query with Close query language
    excluded_str = ",".join(f'"{s}"' for s in EXCLUDED_STATUSES)
    query_str = (
        f'custom.{CF_FIRST_CALL_BOOKED} >= "{month_start}" '
        f'custom.{CF_FIRST_CALL_BOOKED} < "{month_end}" '
        f'lead_status_id not in ({excluded_str})'
    )

    leads = close_get_all("lead", {
        "query": query_str,
        "_fields": ",".join([
            "id",
            "status_id",
            f"custom.{CF_FIRST_CALL_BOOKED}",
            f"custom.{CF_FIRST_CALL_SHOW}",
            f"custom.{CF_LEAD_OWNER}",
            f"custom.{CF_QUALIFIED}",
        ]),
    })

    # Tally meeting metrics per rep
    rep_booked = {}
    rep_shown = {}
    rep_no_show = {}
    rep_no_entry = {}
    rep_qualified = {}

    for lead in leads:
        custom = lead.get("custom", {})
        raw_owner = custom.get(CF_LEAD_OWNER)
        owner = resolve_owner(raw_owner, user_map)

        if not owner or owner in EXCLUDED_NAMES:
            continue

        show_up = custom.get(CF_FIRST_CALL_SHOW)
        qualified = custom.get(CF_QUALIFIED)

        rep_booked[owner] = rep_booked.get(owner, 0) + 1

        if show_up == "Yes":
            rep_shown[owner] = rep_shown.get(owner, 0) + 1
        elif show_up == "No":
            rep_no_show[owner] = rep_no_show.get(owner, 0) + 1
        else:
            rep_no_entry[owner] = rep_no_entry.get(owner, 0) + 1

        if qualified == "Yes":
            rep_qualified[owner] = rep_qualified.get(owner, 0) + 1

    # ── 3. Build per-rep data ────────────────────────────────────────────
    all_rep_names = set(REP_QUOTAS.keys()) | set(rep_booked.keys()) | set(rep_revenue.keys())
    all_rep_names -= EXCLUDED_NAMES

    reps = []
    for name in sorted(all_rep_names):
        booked = rep_booked.get(name, 0)
        shown = rep_shown.get(name, 0)
        no_show = rep_no_show.get(name, 0)
        no_entry = rep_no_entry.get(name, 0)
        qualified = rep_qualified.get(name, 0)
        revenue = rep_revenue.get(name, 0)
        deals = len(rep_deals.get(name, set()))
        quota = REP_QUOTAS.get(name, 0)

        reps.append({
            "name": name,
            "revenue": round(revenue, 2),
            "deals": deals,
            "quota": quota,
            "pct_to_quota": safe_pct(revenue, quota),
            "booked": booked,
            "shown": shown,
            "no_show": no_show,
            "no_entry": no_entry,
            "qualified": qualified,
            "show_rate": safe_pct(shown, booked),
            "close_rate": safe_pct(deals, booked),
            "shown_to_close_rate": safe_pct(deals, shown),
            "qualified_to_close_rate": safe_pct(deals, qualified),
            "is_manager": name in MANAGERS,
        })

    # Sort by booked descending (default)
    reps.sort(key=lambda r: r["booked"], reverse=True)

    # ── 4. Team totals ───────────────────────────────────────────────────
    total_booked = sum(r["booked"] for r in reps)
    total_shown = sum(r["shown"] for r in reps)
    total_no_show = sum(r["no_show"] for r in reps)
    total_no_entry = sum(r["no_entry"] for r in reps)
    total_qualified = sum(r["qualified"] for r in reps)
    total_revenue = sum(r["revenue"] for r in reps)
    total_deals = sum(r["deals"] for r in reps)

    output = {
        "updated_at": now.strftime("%Y-%m-%d %I:%M %p PST"),
        "month_label": now.strftime("%B %Y"),
        "day_of_month": day,
        "days_in_month": days_in_month,
        "working_days_total": wd_total,
        "working_days_elapsed": wd_elapsed,
        "pct_month_passed": pct_month,
        "team_quota": TEAM_QUOTA,
        "total_revenue": round(total_revenue, 2),
        "total_deals": total_deals,
        "total_booked": total_booked,
        "total_shown": total_shown,
        "total_no_show": total_no_show,
        "total_no_entry": total_no_entry,
        "total_qualified": total_qualified,
        "team_show_rate": safe_pct(total_shown, total_booked),
        "team_close_rate": safe_pct(total_deals, total_booked),
        "team_shown_to_close_rate": safe_pct(total_deals, total_shown),
        "team_qualified_to_close_rate": safe_pct(total_deals, total_qualified),
        "pct_team_quota": safe_pct(total_revenue, TEAM_QUOTA),
        "today_revenue": round(today_revenue, 2),
        "today_deals": today_deals,
        "reps": reps,
    }

    # ── 5. Write output ──────────────────────────────────────────────────
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    out_path = os.path.join(repo_root, "data.json")

    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"✅ Wrote {out_path}")
    print(f"   {len(reps)} reps | {total_booked} booked | {total_shown} shown | {total_deals} deals | ${total_revenue:,.2f} revenue")


if __name__ == "__main__":
    main()
