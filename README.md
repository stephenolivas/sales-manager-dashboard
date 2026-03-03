# Rep Metrics Dashboard

A GitHub Pages dashboard that tracks **meeting funnel metrics and close rates** per sales rep, pulling data from Close CRM via GitHub Actions.

## What it tracks

### Per Rep:
| Metric | Description |
|--------|-------------|
| **Meetings Booked** | Leads with "First Call Booked Date" in current month |
| **Shown (Yes)** | Booked leads where "First Call Show Up (Opp)" = "Yes" |
| **No-Show (No)** | Booked leads where "First Call Show Up (Opp)" = "No" |
| **No Entry (Blank)** | Booked leads where "First Call Show Up (Opp)" is blank |
| **Qualified (Yes)** | Booked leads where "Qualified (Opp)" = "Yes" |
| **Show Rate %** | Shown / Booked × 100 |
| **Booked → Close %** | Deals / Booked × 100 |
| **Shown → Close %** | Deals / Shown × 100 |
| **Qualified → Close %** | Deals / Qualified × 100 |

### Team Summary (KPI cards):
All of the above aggregated across the full team.

## Setup

### 1. Create a new GitHub repo

```bash
git init rep-metrics
cd rep-metrics
```

### 2. Copy all files into the repo

```
rep-metrics/
├── index.html
├── data.json
├── scripts/
│   └── fetch_data.py
└── .github/
    └── workflows/
        └── update-dashboard.yml
```

### 3. Add your Close API key as a GitHub Secret

Go to **Settings → Secrets → Actions** and add:
- `CLOSE_API_KEY` — your Close CRM API key

### 4. Enable GitHub Pages

Go to **Settings → Pages** and set:
- Source: **Deploy from a branch**
- Branch: **main** / root

### 5. Run the workflow

Go to **Actions → Update Dashboard Data → Run workflow** to populate `data.json` with live data.

## Automation

The GitHub Actions workflow runs every 15 minutes, Monday–Friday, 7 AM – 5 PM PST. It fetches fresh data from Close CRM and commits the updated `data.json`.

The dashboard auto-refreshes in the browser every 5 minutes.

## Customization

- **Rep quotas & exclusions** — Edit `REP_QUOTAS`, `EXCLUDED_NAMES`, and `MANAGERS` in `scripts/fetch_data.py`
- **Team quota** — Update `TEAM_QUOTA` in the script
- **Styling** — Modify CSS variables in `index.html` under `:root`
