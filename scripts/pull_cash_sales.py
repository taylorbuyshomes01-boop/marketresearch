#!/usr/bin/env python3
"""
Cash Sales Intelligence Dashboard
==================================
Pulls SFR cash sale data from ATTOM API for qualifying wholesale markets.
Smart 60-day refresh: only re-pulls ATTOM when a market is new or data is stale.
Markets that drop off the Redfin qualifying list are automatically pruned.

Files read (from docs/ in repo):
  docs/qualifying_markets.json  — saved by pull_and_report.py each week
  docs/cash/cash_data.json      — persistent state (last_pulled per zip)

Files written (committed by workflow's git push):
  docs/cash/cash_data.json      — updated persistent state
  docs/cash/index.html          — generated dashboard

Workflow step to add in weekly-market-research.yml (before the Commit step):
  - name: Generate cash sales dashboard
    run: python scripts/pull_cash_sales.py
    env:
      ATTOM_API_KEY: ${{ secrets.ATTOM_API_KEY }}
"""

import os
import json
import time
import sys
import requests
from datetime import datetime, timedelta
from pathlib import Path

# ─── Config ────────────────────────────────────────────────────────────────
ATTOM_KEY     = os.environ.get('ATTOM_API_KEY', '')
ATTOM_BASE    = 'https://api.gateway.attomdata.com'
REFRESH_DAYS  = 60     # Re-pull ATTOM data after this many days
LOOKBACK_DAYS = 180    # Pull last 6 months of cash sales from ATTOM
RATE_LIMIT_S  = 0.5    # Seconds between ATTOM API calls

DOCS_DIR = Path(__file__).parent.parent / 'docs'
CASH_DIR = DOCS_DIR / 'cash'
CASH_DATA_FILE = CASH_DIR / 'cash_data.json'
QUALIFYING_FILE = DOCS_DIR / 'qualifying_markets.json'
SEED_CSV = Path(__file__).parent.parent / 'scripts' / 'cash_sales_seed.csv'


# ─── File I/O ──────────────────────────────────────────────────────────────

def load_cash_data():
    """Load existing persistent cash data from disk."""
    if CASH_DATA_FILE.exists():
        try:
            with open(CASH_DATA_FILE, 'r') as f:
                data = json.load(f)
            print(f"  Loaded {len(data.get('zips', {}))} existing zip records")
            return data
        except Exception as e:
            print(f"  WARNING: Could not read cash_data.json: {e}")
    return {'zips': {}, 'metros': {}, 'last_updated': None, 'seed_loaded': False}


def save_cash_data(data):
    """Save cash data to disk (committed by workflow git push)."""
    CASH_DIR.mkdir(parents=True, exist_ok=True)
    with open(CASH_DATA_FILE, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  Saved cash_data.json ({len(data.get('zips', {}))} zips)")


def load_qualifying_markets():
    """Load qualifying zip markets saved by pull_and_report.py."""
    if not QUALIFYING_FILE.exists():
        print("  WARNING: docs/qualifying_markets.json not found.")
        print("  Run pull_and_report.py first, or this is the first run.")
        return None
    try:
        with open(QUALIFYING_FILE, 'r') as f:
            data = json.load(f)
        zips = data.get('zips', [])
        print(f"  Loaded {len(zips)} qualifying zips from Redfin data")
        return data
    except Exception as e:
        print(f"  ERROR: Could not read qualifying_markets.json: {e}")
        return None


# ─── Seed from CSV ─────────────────────────────────────────────────────────

def load_seed_csv(csv_path):
    """Load the initial ATTOM CSV export as seed data."""
    try:
        import csv
        from collections import defaultdict

        zip_counts = defaultdict(lambda: {'cash': 0, 'total': 0, 'metro': '', 'state': '',
                                           'city': '', 'yoy_pct': 0, 'dom': 0, 'ratio': 0,
                                           'med_price': 0, 'metro_rank': 999})
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                zc = str(row.get('zip', '')).strip().zfill(5)
                if not zc or len(zc) != 5:
                    continue
                zip_counts[zc]['cash'] += 1
                zip_counts[zc]['total'] += 1
                zip_counts[zc]['metro'] = row.get('metro', '')
                zip_counts[zc]['state'] = row.get('state', '')
                zip_counts[zc]['city'] = row.get('city', '').title()
                zip_counts[zc]['metro_rank'] = int(row.get('metro_rank', 999) or 999)
                try:
                    zip_counts[zc]['yoy_pct'] = float(row.get('metro_yoy_pct', 0) or 0)
                    zip_counts[zc]['dom'] = float(row.get('metro_dom', 0) or 0)
                    zip_counts[zc]['ratio'] = float(row.get('metro_ratio', 0) or 0)
                    zip_counts[zc]['med_price'] = float(row.get('metro_med_price', 0) or 0)
                except Exception:
                    pass

        result = {}
        seed_date = datetime.now().strftime('%Y-%m-%d')
        for zc, info in zip_counts.items():
            cash = info['cash']
            total = info['total']
            result[zc] = {
                'cash_count': cash,
                'total_count': total,
                'cash_pct': round(cash / total * 100, 1) if total > 0 else 0,
                'last_pulled': seed_date,
                'source': 'seed_csv',
                'metro': info['metro'],
                'state': info['state'],
                'city': info['city'],
                'metro_rank': info['metro_rank'],
                'yoy_pct': info['yoy_pct'],
                'dom': info['dom'],
                'ratio': info['ratio'],
                'med_price': info['med_price'],
                'lat': None,
                'lon': None,
            }
        print(f"  Seeded {len(result)} zips from CSV ({sum(z['cash_count'] for z in result.values())} cash sales)")
        return result
    except Exception as e:
        print(f"  WARNING: Could not load seed CSV: {e}")
        return {}


# ─── ATTOM API ─────────────────────────────────────────────────────────────

def pull_attom_zip(zip_code):
    """
    Pull last LOOKBACK_DAYS of SFR sales for a zip from ATTOM.
    Returns dict with cash_count, total_count, cash_pct, or None on failure.
    """
    if not ATTOM_KEY:
        return None

    start_dt = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    end_dt   = datetime.now().strftime('%Y-%m-%d')
    headers  = {'apikey': ATTOM_KEY, 'accept': 'application/json'}

    total_cash  = 0
    total_sales = 0
    page        = 1

    while True:
        params = {
            'postalcode':          zip_code,
            'startsalesearchdate': start_dt,
            'endsalesearchdate':   end_dt,
            'propertytype':        'SFR',
            'pagesize':            100,
            'page':                page,
        }
        try:
            r = requests.get(
                f'{ATTOM_BASE}/propertyapi/v1.0.0/sale/detail',
                headers=headers, params=params, timeout=30
            )
            time.sleep(RATE_LIMIT_S)
        except Exception as e:
            print(f" [timeout/error: {e}]", end='')
            return None

        if r.status_code == 404:
            break
        if r.status_code == 429:
            print(" [rate limited, sleeping 15s]", end='', flush=True)
            time.sleep(15)
            continue
        if r.status_code != 200:
            print(f" [ATTOM {r.status_code}]", end='')
            break

        data  = r.json()
        props = data.get('property', [])
        if not props:
            break

        for prop in props:
            total_sales += 1
            sale = prop.get('sale', {})
            # ATTOM cash purchase indicators
            cp = sale.get('cashPurchase')
            is_cash = (
                cp is True
                or str(cp).lower() in ('true', 'y', 'yes', '1')
                or (not sale.get('lenderName') and not sale.get('loanAmount') and cp is not False)
            )
            if is_cash:
                total_cash += 1

        # Pagination
        status     = data.get('status', {})
        page_total = int(status.get('total', 0) or 0)
        if page * 100 >= page_total or len(props) < 100:
            break
        page += 1

    cash_pct = round(total_cash / total_sales * 100, 1) if total_sales > 0 else 0
    return {
        'cash_count':  total_cash,
        'total_count': total_sales,
        'cash_pct':    cash_pct,
        'last_pulled': datetime.now().strftime('%Y-%m-%d'),
        'source':      'attom_api',
    }


# ─── Lat/Lon Lookup ────────────────────────────────────────────────────────

def enrich_with_coords(zip_records, existing_zips=None):
    """Add lat/lon centroids to zip records using pgeocode."""
    existing_zips = existing_zips or {}
    missing = [zc for zc, info in zip_records.items()
               if not info.get('lat') and not existing_zips.get(zc, {}).get('lat')]
    if not missing:
        return

    try:
        import pgeocode
        nomi = pgeocode.Nominatim('us')
        geo  = nomi.query_postal_code(missing)
        geo.index = missing

        import pandas as pd
        for zc in missing:
            if zc not in geo.index:
                continue
            g = geo.loc[zc]
            if pd.isna(g.get('latitude')):
                continue
            if zc in zip_records:
                zip_records[zc]['lat'] = round(float(g['latitude']), 4)
                zip_records[zc]['lon'] = round(float(g['longitude']), 4)
    except Exception as e:
        print(f"  WARNING: pgeocode lookup failed: {e}")


# ─── HTML Generator ────────────────────────────────────────────────────────

def generate_html(cash_data):
    """Generate the standalone Cash Sales Intelligence dashboard."""
    zips    = cash_data.get('zips', {})
    updated = cash_data.get('last_updated', '—')
    today   = datetime.now().strftime('%B %d, %Y')

    # ── Aggregate by metro ──
    from collections import defaultdict
    metro_agg = defaultdict(lambda: {
        'cash': 0, 'total': 0, 'rank': 999, 'yoy': 0, 'dom': 0, 'ratio': 0,
        'price': 0, 'zips': [], 'top_zip': '', 'top_zip_cash': 0
    })
    for zc, info in zips.items():
        metro = info.get('metro', 'Unknown')
        m = metro_agg[metro]
        m['cash']  += info.get('cash_count', 0)
        m['total'] += info.get('total_count', 0)
        m['rank']   = min(m['rank'],  info.get('metro_rank', 999))
        m['yoy']    = info.get('yoy_pct', m['yoy'])
        m['dom']    = info.get('dom', m['dom'])
        m['ratio']  = info.get('ratio', m['ratio'])
        m['price']  = info.get('med_price', m['price'])
        m['zips'].append(zc)
        if info.get('cash_count', 0) > m['top_zip_cash']:
            m['top_zip']      = zc
            m['top_zip_cash'] = info.get('cash_count', 0)

    metros_list = []
    for metro, m in metro_agg.items():
        pct = round(m['cash'] / m['total'] * 100, 1) if m['total'] > 0 else 0
        metros_list.append({
            'metro': metro, 'rank': m['rank'], 'cash': m['cash'],
            'total': m['total'], 'pct': pct, 'yoy': m['yoy'],
            'dom': m['dom'], 'ratio': m['ratio'], 'price': m['price'],
            'zips': len(m['zips']), 'top_zip': m['top_zip']
        })
    metros_list.sort(key=lambda x: x['cash'], reverse=True)

    # ── Build zip rows + map data ──
    zip_rows = []
    map_zips = {}
    for zc, info in sorted(zips.items(), key=lambda x: x[1].get('cash_count', 0), reverse=True):
        row = {
            'zip':    zc,
            'city':   info.get('city', '').title(),
            'state':  info.get('state', ''),
            'metro':  info.get('metro', ''),
            'cash':   info.get('cash_count', 0),
            'total':  info.get('total_count', 0),
            'pct':    info.get('cash_pct', 0),
            'yoy':    info.get('yoy_pct', 0),
            'dom':    info.get('dom', 0),
            'ratio':  info.get('ratio', 0),
            'price':  info.get('med_price', 0),
            'pulled': info.get('last_pulled', ''),
            'source': info.get('source', ''),
        }
        zip_rows.append(row)
        lat, lon = info.get('lat'), info.get('lon')
        if lat and lon:
            map_zips[zc] = {**row, 'lat': lat, 'lon': lon}

    # ── Summary stats ──
    total_cash  = sum(z.get('cash_count', 0) for z in zips.values())
    total_sales = sum(z.get('total_count', 0) for z in zips.values())
    overall_pct = round(total_cash / total_sales * 100, 1) if total_sales > 0 else 0
    top_metro   = metros_list[0]['metro'] if metros_list else '—'
    top_zip_row = zip_rows[0] if zip_rows else {}
    top_zip     = top_zip_row.get('zip', '—')
    top_zip_city = top_zip_row.get('city', '')

    metros_json   = json.dumps(metros_list, default=str)
    zip_rows_json = json.dumps(zip_rows, default=str)
    map_zip_json  = json.dumps(map_zips, default=str)

    # State options
    states = sorted(set(z.get('state', '') for z in zips.values() if z.get('state')))
    state_opts = ''.join(f'<option value="{s}">{s}</option>' for s in states)

    # Metro options
    metro_opts = ''.join(
        f'<option value="{m["metro"]}">{m["metro"]}</option>'
        for m in metros_list
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Cash Sales Intelligence — {today}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/topojson/3.0.2/topojson.min.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0a0a0f;color:#e2e8f0;font-family:'Inter',system-ui,sans-serif;min-height:100vh}}
.header{{padding:20px 28px 16px;border-bottom:1px solid #1e2535}}
.header h1{{font-size:1.4rem;font-weight:700;color:#f1f5f9}}
.header h1 span{{color:#22d3ee}}
.sub{{font-size:.78rem;color:#64748b;margin-top:4px}}
.nav-link{{float:right;font-size:.78rem;color:#64748b;text-decoration:none;padding:4px 10px;
  border:1px solid #1e2535;border-radius:4px;margin-top:2px}}
.nav-link:hover{{color:#e2e8f0;border-color:#334155}}
.stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;padding:20px 28px}}
.stat{{background:#0f1520;border:1px solid #1e2535;border-radius:8px;padding:16px}}
.stat-label{{font-size:.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.05em}}
.stat-val{{font-size:1.8rem;font-weight:700;margin-top:4px;color:#22d3ee}}
.stat-sub{{font-size:.72rem;color:#64748b;margin-top:2px}}
.tabs{{display:flex;gap:2px;padding:0 28px;border-bottom:1px solid #1e2535}}
.tab{{padding:10px 18px;font-size:.82rem;font-weight:500;color:#64748b;cursor:pointer;
  border-bottom:2px solid transparent;transition:all .2s}}
.tab.active{{color:#22d3ee;border-bottom-color:#22d3ee}}
.tab:hover:not(.active){{color:#94a3b8}}
.panel{{padding:20px 28px;display:none}}
.panel.active{{display:block}}

/* Map */
.map-controls{{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:14px}}
.map-controls label{{font-size:.75rem;color:#64748b}}
select{{background:#0f1520;border:1px solid #1e2535;color:#e2e8f0;
  padding:6px 10px;border-radius:5px;font-size:.78rem}}
select:focus{{outline:none;border-color:#22d3ee}}
#map-svg{{width:100%;background:#0d1117;border-radius:8px;border:1px solid #1e2535}}
.map-label{{font-size:.72rem;color:#94a3b8;margin-top:8px;text-align:center}}
.legend{{display:flex;gap:16px;align-items:center;flex-wrap:wrap;margin-top:10px}}
.legend-item{{display:flex;align-items:center;gap:5px;font-size:.7rem;color:#94a3b8}}
.legend-dot{{width:10px;height:10px;border-radius:50%}}

/* Metro cards */
.metro-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px}}
.metro-card{{background:#0f1520;border:1px solid #1e2535;border-radius:8px;padding:16px;
  transition:border-color .2s}}
.metro-card:hover{{border-color:#22d3ee33}}
.metro-card-rank{{font-size:.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.05em}}
.metro-card-name{{font-size:1rem;font-weight:600;color:#f1f5f9;margin:4px 0}}
.metro-card-cash{{font-size:1.6rem;font-weight:700;color:#22d3ee}}
.metro-card-pct{{font-size:.8rem;color:#94a3b8;margin-left:6px}}
.metro-card-meta{{display:flex;gap:12px;margin-top:8px;flex-wrap:wrap}}
.metro-badge{{background:#1e2535;color:#94a3b8;padding:2px 8px;border-radius:4px;font-size:.7rem}}
.metro-badge.hot{{background:rgba(34,211,238,.1);color:#22d3ee}}

/* Table */
.filters{{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:14px}}
.search-box{{flex:1;min-width:180px;background:#0f1520;border:1px solid #1e2535;
  color:#e2e8f0;padding:7px 12px;border-radius:5px;font-size:.8rem}}
.search-box:focus{{outline:none;border-color:#22d3ee}}
.results-count{{font-size:.75rem;color:#64748b;margin-bottom:8px}}
.tbl-wrap{{overflow-x:auto}}
table{{width:100%;border-collapse:collapse;font-size:.78rem}}
th{{background:#0f1520;color:#64748b;padding:8px 10px;text-align:left;
  font-weight:600;text-transform:uppercase;letter-spacing:.04em;font-size:.67rem;
  border-bottom:1px solid #1e2535;white-space:nowrap;cursor:pointer;user-select:none}}
th:hover{{color:#94a3b8}}
th.sorted{{color:#22d3ee}}
td{{padding:8px 10px;border-bottom:1px solid #0f1520;vertical-align:middle}}
tr:hover td{{background:#0f1520}}
.badge{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:.67rem;
  font-weight:600;text-transform:uppercase;letter-spacing:.04em}}
.cash-heavy{{background:rgba(34,211,238,.15);color:#22d3ee;border:1px solid rgba(34,211,238,.25)}}
.cash-mod{{background:rgba(74,222,128,.1);color:#4ade80;border:1px solid rgba(74,222,128,.2)}}
.cash-low{{background:rgba(250,204,21,.08);color:#facc15;border:1px solid rgba(250,204,21,.15)}}
.cash-min{{background:rgba(100,116,139,.1);color:#64748b;border:1px solid rgba(100,116,139,.2)}}
.dom-badge{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:.7rem;font-weight:600}}
.dom-fast{{background:rgba(74,222,128,.1);color:#4ade80}}
.dom-ok{{background:rgba(250,204,21,.08);color:#facc15}}
.dom-slow{{background:rgba(239,68,68,.08);color:#ef4444}}
.export-btn{{background:#0f1520;border:1px solid #22d3ee;color:#22d3ee;padding:6px 14px;
  border-radius:5px;font-size:.75rem;cursor:pointer;font-weight:600}}
.export-btn:hover{{background:#22d3ee;color:#0a0a0f}}

/* Tooltip */
.tooltip{{position:fixed;pointer-events:none;background:#1e2535;border:1px solid #334155;
  border-radius:6px;padding:10px 13px;font-size:.75rem;color:#e2e8f0;z-index:9999;
  opacity:0;transition:opacity .15s;max-width:220px}}
.tt-title{{font-weight:700;color:#22d3ee;margin-bottom:5px}}
.tt-row{{display:flex;justify-content:space-between;gap:12px;margin-top:2px}}
.tt-label{{color:#64748b}}
.tt-val{{font-weight:600;color:#f1f5f9}}
</style>
</head>
<body>

<div class="header">
  <a class="nav-link" href="../">← Market Research</a>
  <h1>💵 Cash Sales Intelligence <span>—</span> {today}</h1>
  <div class="sub">SFR cash transactions · ATTOM API · {LOOKBACK_DAYS}-day lookback · 60-day auto-refresh · Updated {updated}</div>
</div>

<div class="stats">
  <div class="stat">
    <div class="stat-label">Total Cash Sales</div>
    <div class="stat-val">{total_cash:,}</div>
    <div class="stat-sub">Across all tracked markets</div>
  </div>
  <div class="stat">
    <div class="stat-label">Avg Cash %</div>
    <div class="stat-val">{overall_pct}%</div>
    <div class="stat-sub">Of all SFR sales tracked</div>
  </div>
  <div class="stat">
    <div class="stat-label">Top Cash Metro</div>
    <div class="stat-val" style="font-size:1.1rem;margin-top:8px;color:#f1f5f9">{top_metro}</div>
    <div class="stat-sub">Most active cash buyers</div>
  </div>
  <div class="stat">
    <div class="stat-label">Top Cash Zip</div>
    <div class="stat-val" style="font-size:1.4rem;color:#a78bfa">{top_zip}</div>
    <div class="stat-sub">{top_zip_city}</div>
  </div>
</div>

<div class="tabs">
  <div class="tab active" data-tab="map">🗺 Map</div>
  <div class="tab" data-tab="metros">🏙 Metros</div>
  <div class="tab" data-tab="table">📋 Zip Table</div>
</div>

<!-- MAP TAB -->
<div class="panel active" id="panel-map">
  <div class="map-controls">
    <label>State:</label>
    <select id="mapState">
      <option value="">All States</option>
      {state_opts}
    </select>
    <label style="margin-left:8px">Metro:</label>
    <select id="mapMetro">
      <option value="">All Metros</option>
      {metro_opts}
    </select>
  </div>
  <svg id="map-svg" viewBox="0 0 960 600"></svg>
  <div class="map-label" id="map-label">Loading map…</div>
  <div class="legend">
    <div class="legend-item"><div class="legend-dot" style="background:#22d3ee"></div>≥40% Cash (Heavy)</div>
    <div class="legend-item"><div class="legend-dot" style="background:#4ade80"></div>20–40% Cash (Moderate)</div>
    <div class="legend-item"><div class="legend-dot" style="background:#facc15"></div>10–20% Cash (Low)</div>
    <div class="legend-item"><div class="legend-dot" style="background:#64748b"></div>&lt;10% Cash (Minimal)</div>
  </div>
</div>

<!-- METROS TAB -->
<div class="panel" id="panel-metros">
  <div class="metro-grid" id="metro-grid"></div>
</div>

<!-- TABLE TAB -->
<div class="panel" id="panel-table">
  <div class="filters">
    <select id="tblState">
      <option value="">All States</option>
      {state_opts}
    </select>
    <select id="tblMetro">
      <option value="">All Metros</option>
      {metro_opts}
    </select>
    <input class="search-box" id="tblSearch" placeholder="Search zip, city, metro…">
    <button class="export-btn" onclick="downloadCSV()">⬇ Export CSV</button>
  </div>
  <div class="results-count" id="tbl-count"></div>
  <div class="tbl-wrap">
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th onclick="sortBy('zip')">Zip ↕</th>
          <th onclick="sortBy('city')">City</th>
          <th onclick="sortBy('state')">State</th>
          <th onclick="sortBy('metro')">Metro</th>
          <th onclick="sortBy('cash')" class="sorted">Cash Sales ↓</th>
          <th onclick="sortBy('pct')">Cash %</th>
          <th onclick="sortBy('yoy')">YoY %</th>
          <th onclick="sortBy('dom')">DOM</th>
          <th onclick="sortBy('ratio')">Ratio</th>
          <th onclick="sortBy('price')">Med. Price</th>
          <th>Last Pulled</th>
        </tr>
      </thead>
      <tbody id="tbl-body"></tbody>
    </table>
  </div>
</div>

<div class="tooltip" id="tooltip"></div>

<script>
// ── Data ────────────────────────────────────────────────────────────────
const METROS   = {metros_json};
const ZIP_ROWS = {zip_rows_json};
const MAP_ZIPS = {map_zip_json};

// ── Tabs ────────────────────────────────────────────────────────────────
document.querySelectorAll('.tab').forEach(tab => {{
  tab.addEventListener('click', () => {{
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('panel-' + tab.dataset.tab).classList.add('active');
    if (tab.dataset.tab === 'map' && !mapLoaded) loadMap();
  }});
}});

// ── Helpers ─────────────────────────────────────────────────────────────
function cashClass(pct) {{
  if (pct >= 40) return 'cash-heavy';
  if (pct >= 20) return 'cash-mod';
  if (pct >= 10) return 'cash-low';
  return 'cash-min';
}}
function cashLabel(pct) {{
  if (pct >= 40) return 'Heavy';
  if (pct >= 20) return 'Mod';
  if (pct >= 10) return 'Low';
  return 'Min';
}}
function cashColor(pct) {{
  if (pct >= 40) return '#22d3ee';
  if (pct >= 20) return '#4ade80';
  if (pct >= 10) return '#facc15';
  return '#64748b';
}}
function domClass(d) {{
  if (d <= 20) return 'dom-fast';
  if (d <= 40) return 'dom-ok';
  return 'dom-slow';
}}
function fmt(n) {{ return n != null ? Number(n).toLocaleString() : '—'; }}
function fmtPct(n) {{ return n != null ? (+n).toFixed(1) + '%' : '—'; }}
function fmtPrice(n) {{ return n ? '$' + Math.round(n).toLocaleString() : '—'; }}

// ── Metro Cards ─────────────────────────────────────────────────────────
(function buildMetros() {{
  const grid = document.getElementById('metro-grid');
  METROS.forEach((m, i) => {{
    const pct = m.pct || 0;
    const card = document.createElement('div');
    card.className = 'metro-card';
    card.innerHTML = `
      <div class="metro-card-rank">Rank #${{i+1}} by Cash Sales</div>
      <div class="metro-card-name">${{m.metro}}</div>
      <div style="margin:6px 0">
        <span class="metro-card-cash">${{fmt(m.cash)}}</span>
        <span class="metro-card-pct">cash sales (${{pct.toFixed(1)}}% of ${{fmt(m.total)}} total)</span>
      </div>
      <div class="metro-card-meta">
        <span class="metro-badge ${{pct >= 30 ? 'hot' : ''}}">${{fmtPct(pct)}} Cash</span>
        <span class="metro-badge">YoY ${{m.yoy >= 0 ? '+' : ''}}${{(+m.yoy).toFixed(1)}}%</span>
        <span class="metro-badge">${{m.dom}}d DOM</span>
        <span class="metro-badge">${{m.zips}} zips</span>
        <span class="metro-badge">Top: ${{m.top_zip}}</span>
      </div>
    `;
    grid.appendChild(card);
  }});
}})();

// ── Table ────────────────────────────────────────────────────────────────
let sortCol = 'cash', sortAsc = false;
let filtered = [...ZIP_ROWS];

function getFiltered() {{
  const state = document.getElementById('tblState').value;
  const metro = document.getElementById('tblMetro').value;
  const q     = document.getElementById('tblSearch').value.toLowerCase();
  return ZIP_ROWS.filter(r =>
    (!state || r.state === state) &&
    (!metro || r.metro === metro) &&
    (!q || [r.zip, r.city, r.state, r.metro].join(' ').toLowerCase().includes(q))
  );
}}

function sortBy(col) {{
  if (sortCol === col) sortAsc = !sortAsc;
  else {{ sortCol = col; sortAsc = false; }}
  document.querySelectorAll('th').forEach(t => t.classList.remove('sorted'));
  event.target.classList.add('sorted');
  renderTable();
}}

function renderTable() {{
  filtered = getFiltered();
  filtered.sort((a, b) => {{
    const av = a[sortCol] ?? 0, bv = b[sortCol] ?? 0;
    return sortAsc ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
  }});
  document.getElementById('tbl-count').textContent =
    `Showing ${{filtered.length.toLocaleString()}} of ${{ZIP_ROWS.length.toLocaleString()}} zip codes`;
  const tbody = document.getElementById('tbl-body');
  tbody.innerHTML = filtered.slice(0, 500).map((r, i) => `
    <tr>
      <td style="color:#64748b;font-size:.7rem">${{i+1}}</td>
      <td style="font-family:monospace;font-size:.82rem;color:#a78bfa;font-weight:700">${{r.zip}}</td>
      <td>${{r.city || '—'}}</td>
      <td>${{r.state}}</td>
      <td style="color:#94a3b8;font-size:.75rem">${{r.metro}}</td>
      <td><strong style="color:#22d3ee">${{fmt(r.cash)}}</strong></td>
      <td><span class="badge ${{cashClass(r.pct)}}">${{cashLabel(r.pct)}} ${{fmtPct(r.pct)}}</span></td>
      <td style="color:${{r.yoy >= 0 ? '#4ade80' : '#ef4444'}};font-weight:600">
        ${{r.yoy >= 0 ? '+' : ''}}${{(+r.yoy).toFixed(1)}}%</td>
      <td><span class="dom-badge ${{domClass(r.dom)}}">${{r.dom}}d</span></td>
      <td style="color:${{r.ratio >= 0.5 ? '#4ade80' : '#ef4444'}}">${{(+r.ratio).toFixed(2)}}</td>
      <td style="color:#94a3b8">${{fmtPrice(r.price)}}</td>
      <td style="color:#64748b;font-size:.7rem">${{r.pulled || '—'}}</td>
    </tr>
  `).join('');
}}

['tblState','tblMetro','tblSearch'].forEach(id =>
  document.getElementById(id).addEventListener('input', renderTable));
renderTable();

// ── CSV Export ───────────────────────────────────────────────────────────
function downloadCSV() {{
  const headers = ['Zip','City','State','Metro','Cash Sales','Total Sales','Cash %','YoY %','DOM','Ratio','Med Price','Last Pulled'];
  const rows = filtered.map(r => [
    r.zip, r.city, r.state, r.metro, r.cash, r.total, r.pct, r.yoy, r.dom, r.ratio, r.price, r.pulled
  ]);
  const csv = [headers, ...rows].map(r => r.map(v => `"${{v ?? ''}}"`).join(',')).join('\\n');
  const a = document.createElement('a');
  a.href = 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv);
  a.download = 'cash_sales_' + new Date().toISOString().slice(0,10) + '.csv';
  a.click();
}}

// ── Map ──────────────────────────────────────────────────────────────────
let mapLoaded = false;

function loadMap() {{
  mapLoaded = true;
  const svg  = d3.select('#map-svg');
  const proj = d3.geoAlbersUsa().scale(1300).translate([487.5, 305]);
  const path = d3.geoPath().projection(proj);
  const tip  = document.getElementById('tooltip');

  d3.json('https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json').then(us => {{
    svg.append('g')
      .selectAll('path')
      .data(topojson.feature(us, us.objects.states).features)
      .join('path')
      .attr('d', path)
      .attr('fill', '#111827')
      .attr('stroke', '#1e2535')
      .attr('stroke-width', 0.5);

    drawDots(svg, proj, tip);
    document.getElementById('map-label').textContent =
      `${{Object.keys(MAP_ZIPS).length}} zip codes mapped`;
  }});
}}

function drawDots(svg, proj, tip) {{
  svg.selectAll('.zdot').remove();
  const stateF = document.getElementById('mapState').value;
  const metroF = document.getElementById('mapMetro').value;

  const dots = Object.entries(MAP_ZIPS).filter(([zc, z]) =>
    (!stateF || z.state === stateF) && (!metroF || z.metro === metroF)
  );

  const maxCash = d3.max(dots, ([,z]) => z.cash) || 1;
  const rScale  = d3.scaleSqrt().domain([0, maxCash]).range([3, 16]);

  dots.forEach(([zc, z]) => {{
    const pt = proj([z.lon, z.lat]);
    if (!pt) return;
    const col = cashColor(z.pct);
    const r   = rScale(z.cash);

    svg.append('circle')
      .attr('class', 'zdot')
      .attr('cx', pt[0]).attr('cy', pt[1]).attr('r', r)
      .attr('fill', col).attr('fill-opacity', 0.7)
      .attr('stroke', '#0a0a0f').attr('stroke-width', 0.5)
      .style('cursor', 'pointer')
      .on('mousemove', (evt) => {{
        tip.style.opacity = 1;
        tip.style.left = (evt.clientX + 12) + 'px';
        tip.style.top  = (evt.clientY - 10) + 'px';
        tip.innerHTML = `
          <div class="tt-title">${{zc}} — ${{z.city}}, ${{z.state}}</div>
          <div class="tt-row"><span class="tt-label">Metro</span><span class="tt-val" style="font-size:.7rem">${{z.metro}}</span></div>
          <div class="tt-row"><span class="tt-label">Cash Sales</span><span class="tt-val" style="color:#22d3ee">${{fmt(z.cash)}}</span></div>
          <div class="tt-row"><span class="tt-label">Cash %</span><span class="tt-val">${{fmtPct(z.pct)}}</span></div>
          <div class="tt-row"><span class="tt-label">YoY</span><span class="tt-val" style="color:${{z.yoy>=0?'#4ade80':'#ef4444'}}">${{z.yoy>=0?'+':''}}${{(+z.yoy).toFixed(1)}}%</span></div>
          <div class="tt-row"><span class="tt-label">DOM</span><span class="tt-val">${{z.dom}}d</span></div>
          <div class="tt-row"><span class="tt-label">Ratio</span><span class="tt-val">${{(+z.ratio).toFixed(2)}}</span></div>
        `;
      }})
      .on('mouseleave', () => {{ tip.style.opacity = 0; }});
  }});

  document.getElementById('map-label').textContent =
    `${{dots.length}} zips shown — circle size = cash sale volume`;
}}

// Trigger map on first tab show
document.querySelector('[data-tab="map"]').click();
setTimeout(() => loadMap(), 100);

// Map filters
['mapState','mapMetro'].forEach(id => {{
  document.getElementById(id).addEventListener('change', () => {{
    const svg  = d3.select('#map-svg');
    const proj = d3.geoAlbersUsa().scale(1300).translate([487.5, 305]);
    const tip  = document.getElementById('tooltip');
    drawDots(svg, proj, tip);
  }});
}});
</script>
</body>
</html>"""


# ─── Main ──────────────────────────────────────────────────────────────────

def main():
    print('=' * 60)
    print(f'Cash Sales Intelligence  |  {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 60)

    CASH_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Load persistent cash state
    cash_data = load_cash_data()
    zips      = cash_data.setdefault('zips', {})

    # 2. Seed from CSV if first run
    seed_csv_path = Path(__file__).parent.parent / 'cash_sales_seed.csv'
    if not cash_data.get('seed_loaded') and seed_csv_path.exists():
        print('\n  Seeding from CSV export...')
        seed_zips = load_seed_csv(seed_csv_path)
        for zc, info in seed_zips.items():
            if zc not in zips:
                zips[zc] = info
        cash_data['seed_loaded'] = True
        enrich_with_coords(zips)
        print(f'  Seed complete — {len(zips)} zips in database')

    # 3. Load qualifying markets from Redfin
    qualifying = load_qualifying_markets()
    if qualifying is None:
        # No Redfin data yet — just generate dashboard from existing data
        print('  Generating dashboard from existing data only...')
    else:
        qualifying_zips = {z['zip']: z for z in qualifying.get('zips', [])}

        # 4. Prune markets that no longer qualify
        to_remove = [zc for zc in list(zips.keys()) if zc not in qualifying_zips]
        if to_remove:
            print(f'\n  Pruning {len(to_remove)} markets that no longer qualify...')
            for zc in to_remove:
                metro = zips[zc].get('metro', '')
                del zips[zc]
                print(f'    Removed {zc} ({metro})')

        # 5. Determine what needs ATTOM refresh + register new qualifying zips
        # KEY DESIGN: We only call ATTOM for zips already in cash_data that are
        # 60+ days stale. Brand-new qualifying zips are added to the dashboard
        # with Redfin metadata only — no ATTOM call on first appearance.
        # This prevents burning quota on the full qualifying list every run.
        today    = datetime.now().date()
        to_pull  = []
        new_count = 0
        for zc, mkt in qualifying_zips.items():
            existing = zips.get(zc)
            if existing is None:
                # Brand-new qualifying zip — register it with Redfin metadata.
                # Set last_pulled = today so it won't be flagged stale until
                # REFRESH_DAYS from now, at which point ATTOM will be called.
                new_count += 1
                zips[zc] = {
                    'zip':         zc,
                    'cash_count':  0,
                    'total_count': 0,
                    'cash_pct':    0.0,
                    'last_pulled': today.strftime('%Y-%m-%d'),
                    'source':      'pending',
                    'metro':       mkt.get('metro', ''),
                    'state':       mkt.get('state', ''),
                    'city':        mkt.get('city', '').title(),
                    'metro_rank':  mkt.get('metro_rank', 999),
                    'yoy_pct':     mkt.get('yoy_pct', 0),
                    'dom':         mkt.get('dom', 0),
                    'ratio':       mkt.get('ratio', 0),
                    'med_price':   mkt.get('med_price', 0),
                    'lat':         mkt.get('lat'),
                    'lon':         mkt.get('lon'),
                    'lookback_days': LOOKBACK_DAYS,
                }
            else:
                try:
                    lp = existing.get('last_pulled')
                    if not lp:
                        # No date recorded — reset to today, skip ATTOM
                        zips[zc]['last_pulled'] = today.strftime('%Y-%m-%d')
                    else:
                        last = datetime.strptime(lp, '%Y-%m-%d').date()
                        age  = (today - last).days
                        if age >= REFRESH_DAYS:
                            to_pull.append((zc, mkt, f'stale ({age}d old)'))
                except Exception:
                    # Unparseable date — reset, skip ATTOM this cycle
                    zips[zc]['last_pulled'] = today.strftime('%Y-%m-%d')

        cached = len(qualifying_zips) - len(to_pull) - new_count
        print(f'\n  {new_count} new qualifying zips added to dashboard (no ATTOM pull yet)')
        print(f'  {len(to_pull)} existing zips need ATTOM refresh  |  {cached} using current data')

        # 6. Update metadata for qualifying zips from fresh Redfin data
        for zc, mkt in qualifying_zips.items():
            if zc in zips:
                zips[zc].update({
                    'metro':      mkt.get('metro', zips[zc].get('metro', '')),
                    'state':      mkt.get('state', zips[zc].get('state', '')),
                    'city':       mkt.get('city', zips[zc].get('city', '')).title(),
                    'metro_rank': mkt.get('metro_rank', zips[zc].get('metro_rank', 999)),
                    'yoy_pct':    mkt.get('yoy_pct', zips[zc].get('yoy_pct', 0)),
                    'dom':        mkt.get('dom', zips[zc].get('dom', 0)),
                    'ratio':      mkt.get('ratio', zips[zc].get('ratio', 0)),
                    'med_price':  mkt.get('med_price', zips[zc].get('med_price', 0)),
                })

        # 7. Pull ATTOM for new/stale zips
        if ATTOM_KEY and to_pull:
            print(f'\n  Calling ATTOM API for {len(to_pull)} zips...')
            for i, (zc, mkt, reason) in enumerate(to_pull):
                print(f'  [{i+1}/{len(to_pull)}] {zc} — {mkt.get("city","")}, {mkt.get("state","")} ({reason})', end='', flush=True)
                result = pull_attom_zip(zc)
                if result:
                    existing_lat = zips.get(zc, {}).get('lat')
                    existing_lon = zips.get(zc, {}).get('lon')
                    zips[zc] = {
                        **result,
                        'metro':      mkt.get('metro', ''),
                        'state':      mkt.get('state', ''),
                        'city':       mkt.get('city', '').title(),
                        'metro_rank': mkt.get('metro_rank', 999),
                        'yoy_pct':    mkt.get('yoy_pct', 0),
                        'dom':        mkt.get('dom', 0),
                        'ratio':      mkt.get('ratio', 0),
                        'med_price':  mkt.get('med_price', 0),
                        'lat':        mkt.get('lat') or existing_lat,
                        'lon':        mkt.get('lon') or existing_lon,
                    }
                    print(f' → {result["cash_count"]} cash / {result["total_count"]} total ({result["cash_pct"]}%)')
                else:
                    # Keep existing data if ATTOM call fails
                    if zc not in zips:
                        zips[zc] = {
                            'cash_count': 0, 'total_count': 0, 'cash_pct': 0,
                            'last_pulled': datetime.now().strftime('%Y-%m-%d'),
                            'source': 'attom_api_failed',
                            'metro': mkt.get('metro', ''), 'state': mkt.get('state', ''),
                            'city': mkt.get('city', '').title(),
                            'metro_rank': mkt.get('metro_rank', 999),
                            'yoy_pct': mkt.get('yoy_pct', 0), 'dom': mkt.get('dom', 0),
                            'ratio': mkt.get('ratio', 0), 'med_price': mkt.get('med_price', 0),
                            'lat': mkt.get('lat'), 'lon': mkt.get('lon'),
                        }
                    print(' → no data / failed')
        elif not ATTOM_KEY:
            print('  ATTOM_API_KEY not set — skipping live pulls, using existing data')

        # 8. Enrich new zips with lat/lon if missing
        needs_coords = [zc for zc, info in zips.items() if not info.get('lat')]
        if needs_coords:
            print(f'\n  Looking up coordinates for {len(needs_coords)} zips...')
            enrich_with_coords({zc: zips[zc] for zc in needs_coords}, zips)

    # 9. Save updated cash state
    cash_data['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    save_cash_data(cash_data)

    # 10. Generate and save HTML dashboard
    html = generate_html(cash_data)
    out_html = CASH_DIR / 'index.html'
    with open(out_html, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  Generated docs/cash/index.html')

    total_cash = sum(z.get('cash_count', 0) for z in zips.values())
    print(f'\n✅ Cash Sales Dashboard complete')
    print(f'   {len(zips)} zips tracked  |  {total_cash:,} cash sales indexed')
    print(f'   https://taylorbuyshomes01-boop.github.io/marketresearch/cash/')


if __name__ == '__main__':
    main()

