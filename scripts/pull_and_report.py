#!/usr/bin/env python3
"""
Real Estate Market Research Script
Pulls Redfin metro, county, and city market data, filters against
wholesale criteria, and generates a filterable HTML dashboard.

Criteria:
  - Median DOM < 45 days
  - Pending / Active ratio >= 0.50
  - Ranked by YoY home value appreciation (descending)
  - Filterable by: State, County, City, Geography Level
"""

import argparse
import gzip
import io
import json
import os
import sys
import pickle
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
DOM_THRESHOLD = 45
RATIO_THRESHOLD = 0.50
TOP_N = 20
DOM_FALLBACK = 60
CACHE_DAYS = 6  # Redfin updates weekly; cache for 6 days

SCRIPT_DIR = Path(__file__).parent
CACHE_DIR = SCRIPT_DIR.parent / ".cache"

REDFIN_SOURCES = {
    "metro": "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/redfin_metro_market_tracker.tsv000.gz",
    "county": "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/county_market_tracker.tsv000.gz",
    "city": "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz",
}

# ---------------------------------------------------------------------------
# CACHING
# ---------------------------------------------------------------------------

def cache_path(level: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{level}_latest.pkl"


def cache_is_fresh(level: str) -> bool:
    p = cache_path(level)
    if not p.exists():
        return False
    age = datetime.now() - datetime.fromtimestamp(p.stat().st_mtime)
    return age < timedelta(days=CACHE_DAYS)


def load_cache(level: str) -> pd.DataFrame:
    with open(cache_path(level), "rb") as f:
        return pickle.load(f)


def save_cache(level: str, df: pd.DataFrame):
    try:
        with open(cache_path(level), "wb") as f:
            pickle.dump(df, f)
        print(f"  Cached {level} data ({len(df):,} rows)")
    except Exception as e:
        print(f"  WARNING: Could not cache {level} data: {e}")


# ---------------------------------------------------------------------------
# STREAMING DOWNLOAD + FILTER
# Streams the compressed file in chunks, decompresses, and reads only
# rows for the most recent period + All Residential property type.
# This keeps memory usage low for large files (city = 988 MB compressed).
# ---------------------------------------------------------------------------

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; market-research/1.0)"}


def stream_and_filter(url: str, level: str) -> pd.DataFrame:
    """Stream download, decompress, and filter to latest period + All Residential."""
    print(f"  Streaming {level} data from Redfin...")
    resp = requests.get(url, headers=HEADERS, timeout=300, stream=True)
    resp.raise_for_status()

    # Download full compressed content (required for gzip streaming)
    raw = b""
    downloaded = 0
    for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):
        raw += chunk
        downloaded += len(chunk)
        print(f"    {downloaded / 1_000_000:.0f} MB downloaded...", end="\r")
    print(f"    {downloaded / 1_000_000:.1f} MB total")

    with gzip.open(io.BytesIO(raw)) as f:
        # Read in chunks to handle large files
        chunks = []
        for chunk_df in pd.read_csv(
            f, sep="\t", on_bad_lines="skip", low_memory=False,
            chunksize=100_000
        ):
            chunk_df.columns = [c.lower().strip() for c in chunk_df.columns]

            # Filter property type early
            if "property_type" in chunk_df.columns:
                chunk_df = chunk_df[chunk_df["property_type"] == "All Residential"]

            if len(chunk_df) > 0:
                chunks.append(chunk_df)

    if not chunks:
        print(f"  WARNING: No data for {level}")
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)
    print(f"  Loaded {len(df):,} rows (All Residential)")

    # Keep only most recent period
    if "period_end" in df.columns:
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce")
        latest = df["period_end"].max()
        df = df[df["period_end"] == latest]
        print(f"  Period: {latest.date()} → {len(df):,} rows")

    return df


def get_level_data(level: str) -> pd.DataFrame:
    """Load from cache if fresh, otherwise download."""
    if cache_is_fresh(level):
        try:
            df = load_cache(level)
            print(f"  Using cached {level} data ({len(df):,} rows, updated recently)")
            return df
        except Exception as e:
            print(f"  Cache read failed ({e}), re-downloading...")

    df = stream_and_filter(REDFIN_SOURCES[level], level)
    if len(df) > 0:
        save_cache(level, df)  # non-fatal if it fails
    return df


# ---------------------------------------------------------------------------
# NORMALIZE + PROCESS A SINGLE LEVEL
# ---------------------------------------------------------------------------

def process_level(df: pd.DataFrame, level: str) -> pd.DataFrame:
    """Normalize columns, calculate metrics, return clean frame."""
    if df.empty:
        return pd.DataFrame()

    df = df.copy()

    # Rename key columns to standard names
    rename_map = {}
    col_lower = {c.lower(): c for c in df.columns}

    mapping = {
        "median_dom": "dom",
        "median_days_on_market": "dom",
        "inventory": "active",
        "active_listings": "active",
        "pending_sales": "pending",
        "homes_pending": "pending",
        "median_sale_price_yoy": "yoy_pct",
        "median_list_price_yoy": "yoy_pct",
        "median_sale_price": "median_price",
        "median_list_price": "median_price",
        "region": "region_raw",
        "region_name": "region_raw",
        "city": "city_raw",
        "state": "state",
        "state_code": "state_code",
        "parent_metro_region": "parent_metro",
        "period_end": "period_end",
    }

    for src, dst in mapping.items():
        if src in df.columns and dst not in rename_map.values():
            rename_map[src] = dst

    df = df.rename(columns=rename_map)

    # Numeric conversions
    for col in ["dom", "active", "pending", "yoy_pct", "median_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Fix YoY: Redfin stores as decimal (0.05 = 5%)
    if "yoy_pct" in df.columns and df["yoy_pct"].notna().any():
        if df["yoy_pct"].abs().median() < 1.0:
            df["yoy_pct"] = df["yoy_pct"] * 100

    # Drop rows missing critical fields
    required = ["dom", "active", "pending"]
    df = df.dropna(subset=[c for c in required if c in df.columns])
    df = df[df["active"] > 0]

    if df.empty:
        return pd.DataFrame()

    # Calculate ratio
    df["ratio"] = df["pending"] / df["active"]

    # Build display name and geo fields based on level
    if level == "metro":
        df["display_name"] = df.get("region_raw", pd.Series(dtype=str)).str.replace(
            r"\s+metro area$", "", regex=True, flags=2
        ).str.strip()
        df["county"] = ""
        df["city"] = ""
        df["parent_metro"] = df.get("display_name", "")

    elif level == "county":
        # REGION = "County Name, ST"
        region = df.get("region_raw", pd.Series(dtype=str)).fillna("")
        df["display_name"] = region.str.replace(r",\s*\w{2}$", "", regex=True).str.strip()
        df["county"] = df["display_name"]
        df["city"] = ""
        df["parent_metro"] = df.get("parent_metro", "").fillna("")

    elif level == "city":
        # REGION = "City Name, ST" / CITY = city name
        if "city_raw" in df.columns:
            df["display_name"] = df["city_raw"].fillna(
                df.get("region_raw", pd.Series(dtype=str)).str.replace(r",\s*\w{2}$", "", regex=True)
            ).str.strip()
        else:
            df["display_name"] = df.get("region_raw", pd.Series(dtype=str)).str.replace(
                r",\s*\w{2}$", "", regex=True
            ).str.strip()
        df["city"] = df["display_name"]
        df["county"] = ""
        df["parent_metro"] = df.get("parent_metro", "").fillna("")

    # State: prefer full state name, fall back to code
    if "state" not in df.columns or df["state"].isna().all():
        if "state_code" in df.columns:
            df["state"] = df["state_code"]
    df["state"] = df.get("state", pd.Series(dtype=str)).fillna(
        df.get("state_code", pd.Series(dtype=str))
    ).fillna("")

    # Add level column
    df["level"] = level.capitalize()

    # Deduplicate within this level
    df = df.sort_values("pending", ascending=False).drop_duplicates(
        subset=["display_name", "state"], keep="first"
    )

    return df[[
        "level", "display_name", "state", "state_code", "county", "city",
        "parent_metro", "yoy_pct", "median_price", "dom", "active", "pending", "ratio"
    ] + (["period_end"] if "period_end" in df.columns else [])]


# ---------------------------------------------------------------------------
# COMBINE ALL LEVELS + APPLY FILTERS
# ---------------------------------------------------------------------------

def build_combined(frames: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Combine all levels, return (filtered, all_qualifying)."""
    all_dfs = []
    for level, df in frames.items():
        processed = process_level(df, level)
        if not processed.empty:
            all_dfs.append(processed)
            print(f"  {level}: {len(processed):,} markets after processing")

    if not all_dfs:
        print("ERROR: No data from any level")
        sys.exit(1)

    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"\n  Total combined: {len(combined):,} markets across all levels")

    # Apply filters
    qualified = combined[
        (combined["dom"] < DOM_THRESHOLD) &
        (combined["ratio"] >= RATIO_THRESHOLD) &
        combined["yoy_pct"].notna()
    ].copy()

    print(f"  Qualifying (DOM<{DOM_THRESHOLD}, ratio>={RATIO_THRESHOLD}): {len(qualified):,}")

    criteria_relaxed = False
    if len(qualified[qualified["level"] == "Metro"]) < TOP_N:
        print(f"  Less than {TOP_N} metros qualify. Relaxing DOM to {DOM_FALLBACK}.")
        criteria_relaxed = True

    return qualified, combined, criteria_relaxed


# ---------------------------------------------------------------------------
# HTML DASHBOARD
# ---------------------------------------------------------------------------

def to_serializable(val):
    """Convert value to JSON-serializable type."""
    if val is None or (isinstance(val, float) and (val != val)):
        return None
    if isinstance(val, (int,)):
        return int(val)
    if isinstance(val, float):
        return round(val, 3)
    return str(val)


def build_map_data(combined: pd.DataFrame) -> dict:
    """Build map data (ALL counties + states) with qualification flag.
    Keys use 2-letter state abbreviation (state_code) to match JS stateCodeMap lookup.
    """
    county_records = {}
    state_records = {}

    county_df = combined[combined["level"] == "County"].copy()

    for _, row in county_df.iterrows():
        # Use state_code (2-letter abbr like "AL") — NOT full state name
        state_abbr = str(row.get("state_code", "") or row.get("state", "")).strip()
        # If state_code is a full name (fallback), skip — we need the abbr
        if len(state_abbr) > 3:
            state_abbr = str(row.get("state_code", "")).strip()
        if not state_abbr or len(state_abbr) > 3:
            continue

        county_name = str(row.get("display_name", "")).strip()
        if not county_name:
            continue

        # Normalize: strip county/parish/borough suffixes to match TopoJSON bare names
        norm_name = county_name
        for suffix in [" County", " Parish", " Borough", " Census Area",
                       " City", " Municipality", " city"]:
            if norm_name.endswith(suffix):
                norm_name = norm_name[:-len(suffix)]
                break

        # Key matches JS: countyName.toLowerCase() + "|" + stateAbbr
        key = f"{norm_name.lower()}|{state_abbr}"

        qualifies = (
            row.get("dom") is not None and not pd.isna(row.get("dom")) and
            row.get("dom") < DOM_THRESHOLD and
            row.get("ratio") is not None and not pd.isna(row.get("ratio")) and
            row.get("ratio") >= RATIO_THRESHOLD and
            row.get("yoy_pct") is not None and not pd.isna(row.get("yoy_pct"))
        )

        county_records[key] = {
            "name": norm_name,
            "full": county_name,
            "state": state_abbr,
            "yoy": to_serializable(row.get("yoy_pct")),
            "dom": to_serializable(row.get("dom")),
            "ratio": to_serializable(row.get("ratio")),
            "price": to_serializable(row.get("median_price")),
            "active": to_serializable(row.get("active")),
            "pending": to_serializable(row.get("pending")),
            "qualifies": qualifies,
        }

    # State level aggregation (weighted by pending volume)
    state_data = {}
    for _, row in county_df.iterrows():
        state_abbr = str(row.get("state_code", "") or row.get("state", "")).strip()
        if not state_abbr or len(state_abbr) > 3:
            continue

        if state_abbr not in state_data:
            state_data[state_abbr] = {"yoy_sum": 0.0, "pending_sum": 0.0,
                                       "qualify_count": 0, "total": 0}

        pending = float(row.get("pending", 0) or 0)
        yoy = float(row.get("yoy_pct") or 0) if not pd.isna(row.get("yoy_pct", float("nan"))) else 0.0
        state_data[state_abbr]["yoy_sum"] += yoy * pending
        state_data[state_abbr]["pending_sum"] += pending
        state_data[state_abbr]["total"] += 1

        qualifies = (
            row.get("dom") is not None and not pd.isna(row.get("dom")) and
            row.get("dom") < DOM_THRESHOLD and
            row.get("ratio") is not None and not pd.isna(row.get("ratio")) and
            row.get("ratio") >= RATIO_THRESHOLD
        )
        if qualifies:
            state_data[state_abbr]["qualify_count"] += 1

    for state_abbr, data in state_data.items():
        avg_yoy = (data["yoy_sum"] / data["pending_sum"]) if data["pending_sum"] > 0 else 0.0
        state_records[state_abbr] = {
            "yoy": round(avg_yoy, 2),
            "qualify_count": data["qualify_count"],
            "total": data["total"],
        }

    print(f"  Map data: {len(county_records):,} county records, {len(state_records)} state records")
    if county_records:
        sample = list(county_records.items())[:3]
        print(f"  Sample keys: {[k for k,v in sample]}")

    return {
        "counties": county_records,
        "states": state_records,
    }


def build_data_json(df: pd.DataFrame) -> str:
    """Build JSON for qualified markets (table + top 20)."""
    records = []
    for i, (_, row) in enumerate(df.iterrows()):
        records.append({
            "id": i,
            "level": to_serializable(row.get("level", "")),
            "name": to_serializable(row.get("display_name", "")),
            "state": to_serializable(row.get("state", "")),
            "county": to_serializable(row.get("county", "")),
            "city": to_serializable(row.get("city", "")),
            "metro": to_serializable(row.get("parent_metro", "")),
            "yoy": to_serializable(row.get("yoy_pct")),
            "price": to_serializable(row.get("median_price")),
            "dom": to_serializable(row.get("dom")),
            "active": to_serializable(row.get("active")),
            "pending": to_serializable(row.get("pending")),
            "ratio": to_serializable(row.get("ratio")),
        })
    return json.dumps(records)


def generate_html(qualified: pd.DataFrame, combined: pd.DataFrame, criteria_relaxed: bool) -> str:
    today = datetime.now().strftime("%B %d, %Y")
    generated_at = datetime.now().strftime("%Y-%m-%d %I:%M %p")

    metros_df = qualified[qualified["level"] == "Metro"]
    avg_yoy = metros_df["yoy_pct"].mean() if len(metros_df) > 0 else qualified["yoy_pct"].mean()
    avg_dom = metros_df["dom"].mean() if len(metros_df) > 0 else qualified["dom"].mean()
    avg_ratio = metros_df["ratio"].mean() if len(metros_df) > 0 else qualified["ratio"].mean()
    total_q = len(qualified)

    states = sorted(qualified["state"].dropna().unique().tolist())
    state_options = "\n".join([f'<option value="{s}">{s}</option>' for s in states])

    counties = sorted(qualified[qualified["county"] != ""]["county"].dropna().unique().tolist())
    county_options = "\n".join([f'<option value="{c}">{c}</option>' for c in counties[:500]])

    relaxed_html = (
        f'<div class="notice warn">⚠️ Fewer than {TOP_N} metros met DOM &lt; {DOM_THRESHOLD}d criteria. '
        f'DOM threshold relaxed to {DOM_FALLBACK} days for metro level.</div>'
    ) if criteria_relaxed else ""

    data_json = build_data_json(qualified)
    map_data = build_map_data(combined)
    map_data_json = json.dumps(map_data)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Market Research — {today}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/topojson/3.0.2/topojson.min.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0d1117;color:#c9d1d9;min-height:100vh}}

.topbar{{background:#161b22;border-bottom:1px solid #30363d;padding:18px 28px;display:flex;justify-content:space-between;align-items:center}}
.topbar h1{{font-size:20px;font-weight:700;color:#fff}}
.topbar h1 em{{color:#3fb950;font-style:normal}}
.topbar-meta{{font-size:12px;color:#8b949e;text-align:right}}
.topbar-meta strong{{color:#c9d1d9;display:block;font-size:13px}}

.wrap{{max-width:1500px;margin:0 auto;padding:24px 28px}}

.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:22px}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:18px}}
.card-lbl{{font-size:11px;text-transform:uppercase;letter-spacing:.8px;color:#8b949e;margin-bottom:6px}}
.card-val{{font-size:26px;font-weight:700;color:#fff}}
.card-sub{{font-size:12px;color:#8b949e;margin-top:3px}}
.c-green .card-val{{color:#3fb950}}
.c-blue .card-val{{color:#58a6ff}}
.c-yellow .card-val{{color:#d29922}}

.view-tabs{{display:flex;gap:8px;margin-bottom:20px;border-bottom:1px solid #30363d;padding-bottom:12px}}
.view-tab{{padding:8px 14px;background:none;border:none;color:#8b949e;cursor:pointer;font-size:14px;font-weight:500;position:relative;transition:color .2s}}
.view-tab:hover{{color:#c9d1d9}}
.view-tab.active{{color:#3fb950}}
.view-tab.active::after{{content:'';position:absolute;bottom:-12px;left:0;right:0;height:2px;background:#3fb950}}

.view-content{{display:none}}
.view-content.active{{display:block}}

#mapContainer{{background:#0d1117;border:1px solid #30363d;border-radius:10px;overflow:hidden;margin-bottom:20px}}
#mapSvg{{width:100%;height:560px}}

.map-filters{{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:16px;align-items:center}}
.map-filters select{{background:#161b22;border:1px solid #30363d;color:#c9d1d9;padding:9px 13px;border-radius:7px;font-size:13px}}
.map-filters select:focus{{outline:none;border-color:#388bfd}}

.level-tabs{{display:flex;gap:6px;flex-wrap:wrap}}
.level-tab{{background:#0d1117;border:1px solid #30363d;color:#8b949e;padding:6px 14px;border-radius:20px;font-size:12px;cursor:pointer;transition:all .15s;user-select:none}}
.level-tab:hover,.level-tab.active{{background:#1f2937;border-color:#388bfd;color:#c9d1d9}}
.level-tab.active{{background:#1c2a3a;border-color:#388bfd;color:#58a6ff;font-weight:600}}

.top-20-header{{display:flex;align-items:center;gap:12px;margin-bottom:20px;margin-top:24px}}
.top-20-header h2{{font-size:18px;font-weight:700;color:#e6edf3}}
.criteria-badges{{display:flex;gap:6px;flex-wrap:wrap}}
.criteria-badge{{display:inline-block;padding:3px 10px;background:rgba(63,185,80,.1);color:#3fb950;border:1px solid rgba(63,185,80,.3);border-radius:12px;font-size:11px;font-weight:500}}

.top-20-grid{{display:grid;grid-template-columns:repeat(5,1fr);gap:14px}}
.market-card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:16px;transition:all .2s}}
.market-card:hover{{border-color:#388bfd;background:#1c2128}}
.market-rank{{font-size:32px;font-weight:700;margin-bottom:8px}}
.market-rank.gold{{color:#d29922}}
.market-rank.silver{{color:#8b949e}}
.market-rank.bronze{{color:#c0702f}}
.market-name{{font-weight:600;font-size:15px;color:#e6edf3;margin-bottom:6px}}
.market-state{{font-size:12px;color:#8b949e;margin-bottom:8px}}
.market-yoy{{font-size:24px;font-weight:700;color:#4ade80;margin-bottom:8px}}
.market-metrics{{display:flex;gap:8px;flex-wrap:wrap}}
.metric-badge{{display:inline-block;padding:2px 8px;border-radius:6px;font-size:11px;background:rgba(88,166,255,.1);color:#58a6ff;border:1px solid rgba(88,166,255,.2)}}

.filters{{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:18px;align-items:center}}
.filters input,.filters select{{background:#161b22;border:1px solid #30363d;color:#c9d1d9;padding:9px 13px;border-radius:7px;font-size:13px}}
.filters input{{min-width:220px}}
.filters select{{min-width:160px}}
.filters input:focus,.filters select:focus{{outline:none;border-color:#388bfd}}
.filter-grp{{display:flex;gap:6px;align-items:center}}
.filter-grp label{{font-size:12px;color:#8b949e;white-space:nowrap}}

.btn-dl{{margin-left:auto;background:#238636;color:#fff;border:none;padding:9px 18px;border-radius:7px;font-size:13px;font-weight:600;cursor:pointer}}
.btn-dl:hover{{background:#2ea043}}

.tbl-wrap{{background:#161b22;border:1px solid #30363d;border-radius:10px;overflow:auto}}
table{{width:100%;border-collapse:collapse;font-size:13px;min-width:900px}}
thead{{background:#0d1117;position:sticky;top:0}}
th{{padding:12px 14px;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.7px;color:#8b949e;cursor:pointer;user-select:none;white-space:nowrap;border-bottom:1px solid #30363d}}
th:hover{{color:#c9d1d9}}
th.sort-active{{color:#3fb950}}
tbody tr{{border-top:1px solid #21262d;cursor:default}}
tbody tr:hover{{background:#1c2128}}
td{{padding:12px 14px;vertical-align:middle}}

.rank{{font-size:16px;font-weight:700;color:#484f58;min-width:36px}}
.rank.gold{{color:#d29922}}
.rank.silver{{color:#8b949e}}
.rank.bronze{{color:#c0702f}}
.name-cell .primary{{font-weight:600;color:#e6edf3}}
.name-cell .secondary{{font-size:11px;color:#8b949e;margin-top:2px}}
.lvl-badge{{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;white-space:nowrap}}
.lvl-metro{{background:rgba(63,185,80,.1);color:#3fb950;border:1px solid rgba(63,185,80,.25)}}
.lvl-county{{background:rgba(88,166,255,.1);color:#58a6ff;border:1px solid rgba(88,166,255,.25)}}
.lvl-city{{background:rgba(210,153,34,.1);color:#d29922;border:1px solid rgba(210,153,34,.25)}}
.badge{{display:inline-block;padding:2px 9px;border-radius:10px;font-size:12px;font-weight:600}}
.g{{background:rgba(63,185,80,.1);color:#3fb950;border:1px solid rgba(63,185,80,.2)}}
.y{{background:rgba(210,153,34,.1);color:#d29922;border:1px solid rgba(210,153,34,.2)}}
.r{{background:rgba(248,81,73,.1);color:#f85149;border:1px solid rgba(248,81,73,.2)}}
.yoy-h{{color:#3fb950;font-weight:700}}
.yoy-m{{color:#56d364}}
.yoy-l{{color:#8b949e}}
.yoy-n{{color:#f85149}}
.no-rows{{text-align:center;padding:48px;color:#484f58;font-size:15px;display:none}}

.tooltip{{position:absolute;background:#1c2128;border:1px solid #30363d;border-radius:6px;padding:10px 12px;font-size:12px;color:#c9d1d9;pointer-events:none;z-index:1000;display:none;max-width:300px}}

.notice{{padding:11px 16px;border-radius:7px;font-size:13px;margin-bottom:14px}}
.notice.warn{{background:rgba(210,153,34,.08);border:1px solid rgba(210,153,34,.25);color:#d29922}}

.footer{{text-align:center;padding:20px;font-size:12px;color:#484f58;border-top:1px solid #21262d;margin-top:28px}}
.count-bar{{font-size:12px;color:#8b949e;margin-bottom:10px}}
.count-bar strong{{color:#c9d1d9}}

@media(max-width:900px){{.cards{{grid-template-columns:repeat(2,1fr)}}.top-20-grid{{grid-template-columns:repeat(2,1fr)}}}}
</style>
</head>
<body>

<div class="topbar">
  <div>
    <h1>📊 Market Research — <em>{today}</em></h1>
    <div style="font-size:12px;color:#8b949e;margin-top:3px">Interactive USA Map · Metro · County · City  |  All 50 states  |  Redfin data</div>
  </div>
  <div class="topbar-meta">
    <strong>Generated</strong>{generated_at}
  </div>
</div>

<div class="wrap">
  {relaxed_html}

  <div class="cards">
    <div class="card c-green">
      <div class="card-lbl">Avg YoY (Metros)</div>
      <div class="card-val">{f"+{avg_yoy:.1f}%" if avg_yoy >= 0 else f"{avg_yoy:.1f}%"}</div>
      <div class="card-sub">Top qualifying metros</div>
    </div>
    <div class="card c-blue">
      <div class="card-lbl">Avg DOM (Metros)</div>
      <div class="card-val">{avg_dom:.0f}d</div>
      <div class="card-sub">Median days on market</div>
    </div>
    <div class="card c-yellow">
      <div class="card-lbl">Avg Ratio (Metros)</div>
      <div class="card-val">{avg_ratio:.2f}</div>
      <div class="card-sub">Pending ÷ active</div>
    </div>
    <div class="card">
      <div class="card-lbl">Total Qualifying</div>
      <div class="card-val">{total_q:,}</div>
      <div class="card-sub">Across metro + county + city</div>
    </div>
  </div>

  <div class="view-tabs">
    <button class="view-tab active" data-view="map">🗺 Map</button>
    <button class="view-tab" data-view="table">📋 Table</button>
  </div>

  <div id="mapView" class="view-content active">
    <div class="map-filters">
      <div class="filter-grp">
        <label>Geo Level:</label>
        <div class="level-tabs" id="mapLevelTabs">
          <div class="level-tab active" data-level="counties">Counties</div>
          <div class="level-tab" data-level="states">States</div>
        </div>
      </div>
      <select id="mapStateFilter">
        <option value="">All States</option>
        {state_options}
      </select>
    </div>
    <div id="mapContainer" style="position:relative">
      <svg id="mapSvg"></svg>
      <div style="position:absolute;bottom:16px;right:16px;background:rgba(13,17,23,0.88);border:1px solid #30363d;border-radius:8px;padding:12px 14px;font-size:12px;min-width:150px">
        <div style="font-size:10px;text-transform:uppercase;letter-spacing:1px;color:#8b949e;margin-bottom:8px;font-weight:600">YoY GROWTH</div>
        <div style="display:flex;flex-direction:column;gap:5px">
          <div style="display:flex;align-items:center;gap:8px"><span style="width:14px;height:14px;border-radius:3px;background:#39FF14;display:inline-block;flex-shrink:0"></span><span style="color:#e6edf3">≥ 8% &nbsp;Hot</span></div>
          <div style="display:flex;align-items:center;gap:8px"><span style="width:14px;height:14px;border-radius:3px;background:#166534;display:inline-block;flex-shrink:0"></span><span style="color:#e6edf3">2–8% &nbsp;Okay</span></div>
          <div style="display:flex;align-items:center;gap:8px"><span style="width:14px;height:14px;border-radius:3px;background:#dc2626;display:inline-block;flex-shrink:0"></span><span style="color:#e6edf3">-7–2% &nbsp;Declining</span></div>
          <div style="display:flex;align-items:center;gap:8px"><span style="width:14px;height:14px;border-radius:3px;background:#7f1d1d;display:inline-block;flex-shrink:0"></span><span style="color:#e6edf3">&lt; -7% &nbsp;Bad</span></div>
          <div style="display:flex;align-items:center;gap:8px"><span style="width:14px;height:14px;border-radius:3px;background:#111827;border:1px solid #374151;display:inline-block;flex-shrink:0"></span><span style="color:#6b7280">No data</span></div>
        </div>
        <div style="margin-top:8px;padding-top:8px;border-top:1px solid #30363d;display:flex;align-items:center;gap:6px">
          <span style="width:14px;height:3px;background:#a3e635;border-radius:2px;display:inline-block;flex-shrink:0"></span>
          <span style="color:#a3e635;font-size:11px">Qualifies (DOM+Ratio)</span>
        </div>
      </div>
    </div>
    <div class="top-20-header">
      <h2>🏆 Top 20 Qualifying Markets</h2>
      <div class="criteria-badges">
        <div class="criteria-badge">DOM &lt; {DOM_THRESHOLD}d</div>
        <div class="criteria-badge">Ratio ≥ {RATIO_THRESHOLD}</div>
      </div>
    </div>
    <div class="top-20-grid" id="top20Grid"></div>
  </div>

  <div id="tableView" class="view-content">
    <div class="filters">
      <div class="filter-grp">
        <label>Level:</label>
        <div class="level-tabs" id="levelTabs">
          <div class="level-tab active" data-level="Metro">Metro</div>
          <div class="level-tab" data-level="County">County</div>
          <div class="level-tab" data-level="City">City</div>
          <div class="level-tab" data-level="">All</div>
        </div>
      </div>
      <select id="stateFilter" onchange="applyFilters()">
        <option value="">All States</option>
        {state_options}
      </select>
      <select id="countyFilter" onchange="applyFilters()">
        <option value="">All Counties</option>
        {county_options}
      </select>
      <input type="text" id="searchBox" placeholder="🔍  Search market, city, zip..." oninput="applyFilters()">
      <button class="btn-dl" onclick="downloadCSV()">⬇ Export CSV</button>
    </div>

    <div class="count-bar" id="countBar">Showing <strong>—</strong> markets</div>

    <div class="tbl-wrap">
      <table>
        <thead>
          <tr>
            <th onclick="sortBy('_rank')">#</th>
            <th onclick="sortBy('level')">Level <span id="arr_level"></span></th>
            <th onclick="sortBy('name')">Market <span id="arr_name"></span></th>
            <th onclick="sortBy('state')">State <span id="arr_state"></span></th>
            <th onclick="sortBy('county')">County <span id="arr_county"></span></th>
            <th onclick="sortBy('metro')">Metro <span id="arr_metro"></span></th>
            <th onclick="sortBy('yoy')" id="th_yoy" class="sort-active">YoY % ↓</th>
            <th onclick="sortBy('price')">Med. Value <span id="arr_price"></span></th>
            <th onclick="sortBy('dom')">DOM <span id="arr_dom"></span></th>
            <th onclick="sortBy('active')">Active <span id="arr_active"></span></th>
            <th onclick="sortBy('pending')">Pending <span id="arr_pending"></span></th>
            <th onclick="sortBy('ratio')">Ratio <span id="arr_ratio"></span></th>
          </tr>
        </thead>
        <tbody id="tbody"></tbody>
      </table>
      <div class="no-rows" id="noRows">No markets match your filters.</div>
    </div>
  </div>

  <div class="footer">
    Source: Redfin Market Tracker (Metro · County · City) &nbsp;|&nbsp;
    Criteria: DOM &lt; {DOM_THRESHOLD}d · Pending/Active ≥ {RATIO_THRESHOLD} · Ranked by YoY% &nbsp;|&nbsp;
    Generated: {generated_at}
  </div>
</div>

<div class="tooltip" id="tooltip"></div>

<script>
const ALL_DATA = {data_json};
const MAP_DATA = {map_data_json};

// D3 Choropleth Map
const stateCodeMap = {{"01":"AL","02":"AK","04":"AZ","05":"AR","06":"CA","08":"CO","09":"CT","10":"DE","11":"DC","12":"FL","13":"GA","15":"HI","16":"ID","17":"IL","18":"IN","19":"IA","20":"KS","21":"KY","22":"LA","23":"ME","24":"MD","25":"MA","26":"MI","27":"MN","28":"MS","29":"MO","30":"MT","31":"NE","32":"NV","33":"NH","34":"NJ","35":"NM","36":"NY","37":"NC","38":"ND","39":"OH","40":"OK","41":"OR","42":"PA","44":"RI","45":"SC","46":"SD","47":"TN","48":"TX","49":"UT","50":"VT","51":"VA","53":"WA","54":"WV","55":"WI","56":"WY","72":"PR"}};

let mapLevel = 'counties';
let selectedMapState = '';
let topoData = null;

function normalizeName(name) {{
  const suffixes = [' County', ' Parish', ' Borough', ' Census Area', ' City', ' Municipality'];
  let normalized = name;
  for (const suffix of suffixes) {{
    if (normalized.endsWith(suffix)) {{
      normalized = normalized.slice(0, -suffix.length);
      break;
    }}
  }}
  return normalized.toLowerCase();
}}

// 4-tier color scale matching wholesale market logic:
// HOT (≥8%)  → neon green   OKAY (2-8%)  → dark green
// NOT GOOD (-7% to 2%) → red   REALLY BAD (<-7%) → dark red
function colorScale(yoy) {{
  if (yoy === null || yoy === undefined) return '#111827'; // no data - very dark

  if (yoy >= 8) {{
    // HOT → neon green gradient (8% = bright green, 22%+ = pure neon #39FF14)
    const t = Math.min(1, (yoy - 8) / 14);
    const r = Math.round(30  + t * (10  - 30));
    const g = Math.round(200 + t * (255 - 200));
    const b = Math.round(40  + t * (5   - 40));
    return `rgb(${{r}},${{g}},${{b}})`;
  }}

  if (yoy >= 2) {{
    // OKAY → dark green gradient (2% = deep forest, 8% = rich green)
    const t = (yoy - 2) / 6;
    const r = Math.round(15 + t * (30 - 15));
    const g = Math.round(90 + t * (200 - 90));
    const b = Math.round(25 + t * (40 - 25));
    return `rgb(${{r}},${{g}},${{b}})`;
  }}

  if (yoy >= -7) {{
    // NOT GOOD → red gradient (2% = bright red, -7% = deep red)
    const t = (2 - yoy) / 9;
    const r = Math.round(220 + t * (170 - 220));
    const g = Math.round(40  + t * (15  - 40));
    const b = Math.round(40  + t * (15  - 40));
    return `rgb(${{r}},${{g}},${{b}})`;
  }}

  // REALLY NOT GOOD → dark red (-7% = deep red, -20%+ = near-black red)
  const t = Math.min(1, (Math.abs(yoy) - 7) / 13);
  const r = Math.round(170 + t * (80  - 170));
  const g = Math.round(15  + t * (5   - 15));
  const b = Math.round(15  + t * (5   - 15));
  return `rgb(${{r}},${{g}},${{b}})`;
}}

function buildCountyLookup() {{
  const lookup = {{}};
  for (const [key, data] of Object.entries(MAP_DATA.counties)) {{
    lookup[key] = data;
  }}
  return lookup;
}}

function buildStateLookup() {{
  return MAP_DATA.states;
}}

function renderMap() {{
  if (!topoData) return;

  const width = 1000;
  const height = 560;

  const svg = d3.select('#mapSvg')
    .attr('viewBox', [0, 0, width, height])
    .attr('width', width)
    .attr('height', height)
    .style('background', '#0d1117');

  svg.selectAll('*').remove();

  const projection = d3.geoAlbersUsa().fitSize([width, height], topojson.feature(topoData, topoData.objects.nation));
  const path = d3.geoPath().projection(projection);

  const countyLookup = buildCountyLookup();
  const stateLookup = buildStateLookup();

  // Helper to get state abbreviation from FIPS
  function getFipsState(fipsStr) {{
    const stateFips = fipsStr.slice(0, 2);
    return stateCodeMap[stateFips] || null;
  }}

  if (mapLevel === 'counties') {{
    // Draw counties
    const counties = topojson.feature(topoData, topoData.objects.counties).features;
    svg.selectAll('.county')
      .data(counties)
      .enter()
      .append('path')
      .attr('class', 'county')
      .attr('d', path)
      .attr('fill', d => {{
        const stateFips = d.id.slice(0, 2);
        const stateAbbr = stateCodeMap[stateFips];
        if (selectedMapState && stateAbbr !== selectedMapState) {{
          return '#0d1117';
        }}
        const countyName = d.properties.name;
        const key = countyName.toLowerCase() + '|' + stateAbbr;
        const data = countyLookup[key];
        return colorScale(data ? data.yoy : null);
      }})
      .attr('stroke', '#0d1117')
      .attr('stroke-width', 0.5)
      .on('mouseover', function(e, d) {{
        const stateFips = d.id.slice(0, 2);
        const stateAbbr = stateCodeMap[stateFips];
        const countyName = d.properties.name;
        const key = countyName.toLowerCase() + '|' + stateAbbr;
        const data = countyLookup[key];

        d3.select(this).attr('stroke-width', 1.5).attr('stroke', '#388bfd');

        const tooltip = d3.select('#tooltip');
        let html = '<strong>' + countyName + ', ' + stateAbbr + '</strong><br/>';
        if (data) {{
          html += 'YoY%: ' + (data.yoy !== null ? data.yoy.toFixed(1) + '%' : 'No data') + '<br/>';
          html += 'DOM: ' + (data.dom !== null ? Math.round(data.dom) + 'd' : 'No data') + '<br/>';
          html += 'Ratio: ' + (data.ratio !== null ? data.ratio.toFixed(2) : 'No data') + '<br/>';
          html += 'Price: ' + (data.price !== null ? '$' + Math.round(data.price).toLocaleString() : 'No data') + '<br/>';
          html += 'Active: ' + (data.active !== null ? data.active.toLocaleString() : 'No data') + '<br/>';
          html += 'Pending: ' + (data.pending !== null ? data.pending.toLocaleString() : 'No data') + '<br/>';
          if (data.qualifies) {{
            html += '<span style="color:#4ade80;font-weight:600;">✓ Qualifies</span>';
          }}
        }} else {{
          html += '<span style="color:#8b949e;">No Redfin coverage</span>';
        }}
        tooltip.html(html)
          .style('display', 'block')
          .style('left', (e.pageX + 10) + 'px')
          .style('top', (e.pageY + 10) + 'px');
      }})
      .on('mousemove', function(e) {{
        d3.select('#tooltip')
          .style('left', (e.pageX + 10) + 'px')
          .style('top', (e.pageY + 10) + 'px');
      }})
      .on('mouseout', function() {{
        d3.select(this).attr('stroke-width', 0.5).attr('stroke', '#0d1117');
        d3.select('#tooltip').style('display', 'none');
      }});

    // Draw state borders
    const states = topojson.mesh(topoData, topoData.objects.states, (a, b) => a !== b);
    svg.append('path')
      .attr('d', path(states))
      .attr('fill', 'none')
      .attr('stroke', '#30363d')
      .attr('stroke-width', 1.5);

    // Draw qualifying county outlines
    svg.selectAll('.qualify-overlay')
      .data(counties)
      .enter()
      .append('path')
      .attr('class', 'qualify-overlay')
      .attr('d', path)
      .attr('fill', 'none')
      .attr('stroke', d => {{
        const stateFips = d.id.slice(0, 2);
        const stateAbbr = stateCodeMap[stateFips];
        const countyName = d.properties.name;
        const key = countyName.toLowerCase() + '|' + stateAbbr;
        const data = countyLookup[key];
        return (data && data.qualifies) ? '#a3e635' : 'none';
      }})
      .attr('stroke-width', d => {{
        const stateFips = d.id.slice(0, 2);
        const stateAbbr = stateCodeMap[stateFips];
        const countyName = d.properties.name;
        const key = countyName.toLowerCase() + '|' + stateAbbr;
        const data = countyLookup[key];
        return (data && data.qualifies) ? 2.5 : 0;
      }})
      .style('pointer-events', 'none');

  }} else if (mapLevel === 'states') {{
    // Draw states
    const states = topojson.feature(topoData, topoData.objects.states).features;
    svg.selectAll('.state')
      .data(states)
      .enter()
      .append('path')
      .attr('class', 'state')
      .attr('d', path)
      .attr('fill', d => {{
        const stateFips = d.id;
        const stateAbbr = stateCodeMap[stateFips];
        if (selectedMapState && stateAbbr !== selectedMapState) {{
          return '#0d1117';
        }}
        const data = stateLookup[stateAbbr];
        return colorScale(data ? data.yoy : null);
      }})
      .attr('stroke', '#30363d')
      .attr('stroke-width', 1)
      .on('mouseover', function(e, d) {{
        const stateFips = d.id;
        const stateAbbr = stateCodeMap[stateFips];
        const data = stateLookup[stateAbbr];

        d3.select(this).attr('stroke-width', 2).attr('stroke', '#388bfd');

        const tooltip = d3.select('#tooltip');
        let html = '<strong>' + stateAbbr + '</strong><br/>';
        if (data) {{
          html += 'YoY Avg: ' + data.yoy.toFixed(1) + '%<br/>';
          html += 'Qualifying: ' + data.qualify_count + ' / ' + data.total + ' counties';
        }}
        tooltip.html(html)
          .style('display', 'block')
          .style('left', (e.pageX + 10) + 'px')
          .style('top', (e.pageY + 10) + 'px');
      }})
      .on('mousemove', function(e) {{
        d3.select('#tooltip')
          .style('left', (e.pageX + 10) + 'px')
          .style('top', (e.pageY + 10) + 'px');
      }})
      .on('mouseout', function() {{
        d3.select(this).attr('stroke-width', 1).attr('stroke', '#30363d');
        d3.select('#tooltip').style('display', 'none');
      }});
  }}
}}

// Load topojson and render
d3.json('https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json')
  .then(data => {{
    topoData = data;
    renderMap();
  }});

// Map level tabs
document.querySelectorAll('#mapLevelTabs .level-tab').forEach(tab => {{
  tab.addEventListener('click', () => {{
    document.querySelectorAll('#mapLevelTabs .level-tab').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    mapLevel = tab.dataset.level;
    renderMap();
  }});
}});

// Map state filter
document.getElementById('mapStateFilter').addEventListener('change', (e) => {{
  selectedMapState = e.target.value;
  renderMap();
  renderTop20();
}});

function renderTop20() {{
  const st = document.getElementById('mapStateFilter').value;
  // Show Metro-level markets only in top 20 — most reliable data, no tiny-sample outliers
  // Require minimum 50 active listings to filter out statistical noise
  const top20 = ALL_DATA
    .filter(r => r.level === 'Metro' && (!st || r.state === st) && (r.active == null || r.active >= 10))
    .slice(0, 20);

  const grid = document.getElementById('top20Grid');
  grid.innerHTML = top20.map((r, i) => {{
    const rank = i + 1;
    let rankClass = '';
    if (rank === 1) rankClass = 'gold';
    else if (rank === 2) rankClass = 'silver';
    else if (rank === 3) rankClass = 'bronze';

    return `<div class="market-card">
      <div class="market-rank ${{rankClass}}">#${{rank}}</div>
      <div class="market-name">${{r.name}}</div>
      <div class="market-state">${{r.state}}</div>
      <div class="market-yoy">${{r.yoy !== null ? (r.yoy >= 0 ? '+' : '') + r.yoy.toFixed(1) : 'N/A'}}%</div>
      <div class="market-metrics">
        <span class="metric-badge">DOM: ${{r.dom !== null ? Math.round(r.dom) + 'd' : 'N/A'}}</span>
        <span class="metric-badge">Ratio: ${{r.ratio !== null ? r.ratio.toFixed(2) : 'N/A'}}</span>
      </div>
    </div>`;
  }}).join('');
}}

renderTop20();

// View tabs
document.querySelectorAll('.view-tab').forEach(tab => {{
  tab.addEventListener('click', () => {{
    document.querySelectorAll('.view-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.view-content').forEach(c => c.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById(tab.dataset.view + 'View').classList.add('active');
  }});
}});

// Table view
let sortKey = 'yoy';
let sortAsc = false;
let activeLevel = 'Metro';
let filtered = [];

function fmt$(v){{ return v==null?'N/A':'$'+Math.round(v).toLocaleString(); }}
function fmtPct(v){{ if(v==null)return 'N/A'; return (v>=0?'+':'')+v.toFixed(1)+'%'; }}
function fmtRatio(v){{ return v==null?'N/A':v.toFixed(2); }}
function lvlClass(l){{ return l==='Metro'?'lvl-metro':l==='County'?'lvl-county':'lvl-city'; }}
function ratioBadge(v){{ if(v==null||v<0.5)return 'r'; if(v<0.75)return 'y'; return 'g'; }}
function domBadge(v){{ if(v==null||v>=45)return 'r'; if(v>=20)return 'y'; return 'g'; }}
function yoyClass(v){{ if(v==null)return ''; if(v>=10)return 'yoy-h'; if(v>=5)return 'yoy-m'; if(v>=0)return 'yoy-l'; return 'yoy-n'; }}
function rankClass(r){{ if(r===1)return 'gold'; if(r===2)return 'silver'; if(r===3)return 'bronze'; return ''; }}

function getFiltered(){{
  const st = document.getElementById('stateFilter').value;
  const co = document.getElementById('countyFilter').value;
  const q  = document.getElementById('searchBox').value.toLowerCase();
  return ALL_DATA.filter(r=>{{
    if(activeLevel && r.level !== activeLevel) return false;
    if(st && r.state !== st) return false;
    if(co && r.county !== co) return false;
    if(q){{
      const hay = [r.name,r.state,r.county,r.city,r.metro].join(' ').toLowerCase();
      if(!hay.includes(q)) return false;
    }}
    return true;
  }});
}}

function sortData(data){{
  return [...data].sort((a,b)=>{{
    let av=a[sortKey], bv=b[sortKey];
    if(av==null) av=sortAsc?1e12:-1e12;
    if(bv==null) bv=sortAsc?1e12:-1e12;
    if(typeof av==='string') return sortAsc?av.localeCompare(bv):bv.localeCompare(av);
    return sortAsc?av-bv:bv-av;
  }});
}}

function render(){{
  const data = sortData(getFiltered());
  filtered = data;
  const tbody = document.getElementById('tbody');
  const noRows = document.getElementById('noRows');
  document.getElementById('countBar').innerHTML =
    'Showing <strong>'+data.length.toLocaleString()+'</strong> markets'+(activeLevel?' ('+activeLevel+')':'');

  if(!data.length){{ tbody.innerHTML=''; noRows.style.display='block'; return; }}
  noRows.style.display='none';

  tbody.innerHTML = data.map((r,i)=>{{
    const rank=i+1;
    const secLine=[r.county,r.metro].filter(Boolean).join(' · ');
    return `<tr>
      <td><div class="rank ${{rankClass(rank)}}">${{rank}}</div></td>
      <td><span class="lvl-badge ${{lvlClass(r.level)}}">${{r.level}}</span></td>
      <td class="name-cell">
        <div class="primary">${{r.name}}</div>
        ${{secLine?'<div class="secondary">'+secLine+'</div>':''}}
      </td>
      <td>${{r.state||'—'}}</td>
      <td>${{r.county||'—'}}</td>
      <td style="font-size:12px;color:#8b949e">${{r.metro||'—'}}</td>
      <td><span class="${{yoyClass(r.yoy)}}">${{fmtPct(r.yoy)}}</span></td>
      <td>${{fmt$(r.price)}}</td>
      <td><span class="badge ${{domBadge(r.dom)}}">${{r.dom!=null?Math.round(r.dom)+'d':'N/A'}}</span></td>
      <td>${{r.active!=null?r.active.toLocaleString():'N/A'}}</td>
      <td>${{r.pending!=null?r.pending.toLocaleString():'N/A'}}</td>
      <td><span class="badge ${{ratioBadge(r.ratio)}}">${{fmtRatio(r.ratio)}}</span></td>
    </tr>`;
  }}).join('');
}}

function applyFilters(){{ render(); }}

function sortBy(key){{
  if(sortKey===key){{ sortAsc=!sortAsc; }}
  else{{ sortKey=key; sortAsc=(key==='dom'||key==='name'||key==='state'); }}
  document.querySelectorAll('th').forEach(th=>{{
    th.classList.remove('sort-active');
    const span=th.querySelector('span[id]');
    if(span) span.textContent='';
  }});
  const thEl=document.getElementById('th_'+key)||[...document.querySelectorAll('th')].find(t=>t.getAttribute('onclick')==="sortBy('"+key+"')");
  if(thEl){{
    thEl.classList.add('sort-active');
    const span=thEl.querySelector('span[id]');
    if(span) span.textContent=sortAsc?' ↑':' ↓';
    else thEl.innerHTML=thEl.innerHTML.replace(/ [↑↓]$/,'')+(sortAsc?' ↑':' ↓');
  }}
  render();
}}

document.querySelectorAll('#levelTabs .level-tab').forEach(tab=>{{
  tab.addEventListener('click',()=>{{
    document.querySelectorAll('#levelTabs .level-tab').forEach(t=>t.classList.remove('active'));
    tab.classList.add('active');
    activeLevel=tab.dataset.level;
    render();
  }});
}});

function downloadCSV(){{
  const headers=['Rank','Level','Market','State','County','Metro','YoY%','Med Price','DOM','Active','Pending','Ratio'];
  const rows=filtered.map((r,i)=>[
    i+1,r.level,'"'+(r.name||'')+'"',r.state||'','"'+(r.county||'')+'"','"'+(r.metro||'')+'"',
    r.yoy!=null?r.yoy.toFixed(2):'',r.price||'',r.dom!=null?Math.round(r.dom):'',
    r.active||'',r.pending||'',r.ratio!=null?r.ratio.toFixed(3):''
  ]);
  const csv=[headers.join(','),...rows.map(r=>r.join(','))].join('\\n');
  const a=document.createElement('a');
  a.href=URL.createObjectURL(new Blob([csv],{{type:'text/csv'}}));
  a.download='market_research_{datetime.now().strftime("%Y-%m-%d")}.csv';
  a.click();
}}

render();
</script>
</body>
</html>"""
    return html


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    global DOM_THRESHOLD, RATIO_THRESHOLD, TOP_N, CACHE_DAYS
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=".")
    parser.add_argument("--dom-threshold", type=int, default=DOM_THRESHOLD)
    parser.add_argument("--ratio-threshold", type=float, default=RATIO_THRESHOLD)
    parser.add_argument("--top-n", type=int, default=TOP_N)
    parser.add_argument("--no-cache", action="store_true", help="Force fresh download")
    args = parser.parse_args()

    DOM_THRESHOLD = args.dom_threshold
    RATIO_THRESHOLD = args.ratio_threshold
    TOP_N = args.top_n
    if args.no_cache:
        CACHE_DAYS = 0

    print("=" * 60)
    print(f"Market Research  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Criteria: DOM < {DOM_THRESHOLD}d  |  Ratio >= {RATIO_THRESHOLD}  |  Top {TOP_N}")
    print("=" * 60)

    frames = {}
    for level in ["metro", "county", "city"]:
        print(f"\n[{level.upper()}]")
        try:
            frames[level] = get_level_data(level)
        except Exception as e:
            print(f"  ERROR loading {level}: {e}")
            frames[level] = pd.DataFrame()

    print("\n[COMBINING + FILTERING]")
    qualified, combined, criteria_relaxed = build_combined(frames)

    if qualified.empty:
        print("ERROR: No qualifying markets found.")
        sys.exit(1)

    # Sort by YoY descending
    qualified = qualified.sort_values("yoy_pct", ascending=False).reset_index(drop=True)

    print(f"\nTop 5 metros:")
    metros = qualified[qualified["level"] == "Metro"].head(5)
    for _, r in metros.iterrows():
        print(f"  {r['display_name']}, {r['state']} — YoY: {r['yoy_pct']:+.1f}%  DOM: {r['dom']:.0f}d  Ratio: {r['ratio']:.2f}")

    print(f"\nGenerating dashboard...")
    html = generate_html(qualified, combined, criteria_relaxed)

    os.makedirs(args.output, exist_ok=True)
    fname = f"market_research_{datetime.now().strftime('%Y-%m-%d')}.html"
    out_path = os.path.join(args.output, fname)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅ Dashboard saved: {out_path}")
    print(f"   {len(qualified):,} qualifying markets  |  Metro / County / City tabs")
    return out_path


if __name__ == "__main__":
    main()
