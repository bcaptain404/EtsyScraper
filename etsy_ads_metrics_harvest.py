#!/usr/bin/env python3
"""
Etsy Ads harvester with policy-based per‑date reduction.

Problem this solves: when you capture a long date range, Etsy often returns BOTH a
per‑day series AND an aggregate "range total" payload. If you naïvely sum every
row you see for the same calendar date, you can shove the whole range onto one day.

Fix here: aggregate multiple values for the SAME date with a policy (default 'sum',
or 'min-nonzero' which picks the smallest non‑zero, useful to suppress range totals).
Also: tz offset for UTC→local, cents/micros scaling, robust key remaps, optional
skipping of rows that look like range totals.

Usage examples:
  python harvest_all_fields.py --in ./out --csv ./out/etsy_ads_daily_allfields.csv \
    --derived --tz-offset-hours -4 --aggregate-policy min-nonzero

  # If you want to keep potential range totals, drop the heuristic skip:
  python harvest_all_fields.py --in ./out --csv ./out/etsy_ads_daily_allfields.csv \
    --aggregate-policy min-nonzero --include-range-totals
"""

import argparse, csv, json, re
from pathlib import Path
from collections import defaultdict, Counter
from statistics import median
from datetime import datetime, timedelta, timezone

# ---- global tz offset (hours), set from CLI ----
TZ_OFFSET = 0

# Canonical remaps
REMAP = {
    # traffic
    "impressions": "views",
    "ad_impressions": "views",
    "attributed_impressions": "views",
    "impressioncount": "views",
    "impression_count": "views",

    # clicks
    "clicks": "clicks",
    "click_throughs": "clicks",
    "attributed_clicks": "clicks",
    "charged_clicks": "clicks",
    "clickcount": "clicks",
    "click_count": "clicks",

    # orders
    "orders": "orders",
    "purchases": "orders",
    "purchasecount": "orders",
    "ordercount": "orders",
    "conversions": "orders",
    "attributed_orders": "orders",

    # revenue
    "revenue": "revenue",
    "sales": "revenue",
    "attributed_sales": "revenue",
    "sales_cents": "revenue",
    "revenue_cents": "revenue",

    # spend/cost
    "spend": "spend",
    "spend_cents": "spend",
    "cost_cents": "spend",
    "spend_micros": "spend",
    "cost_micros": "spend",
    "spenttotal": "spend",
    "spendtotal": "spend",
    "spend_total": "spend",
    "costtotal": "spend",
}

# Keys that imply values are in cents (divide by 100) even without *_cents suffix
CENTS_LIKE = {"spenttotal", "spendtotal", "spend_total", "costtotal"}

DATE_KEYS = {"date","day","timestamp"}
SKIP_KEYS = {"id","listing_id","campaign_id","ad_group_id","shop_id","country","currency","__typename"}

# Heuristics that suggest a row is a RANGE TOTAL payload (not daily):
RANGE_HINT_KEYS = {
    "startdate", "enddate", "start_date", "end_date", "from", "to",
    "date_range", "range", "period", "total", "totals", "sum"
}


def looks_like_range_total(d: dict) -> bool:
    l = {k.lower() for k in d.keys()}
    if not (l & RANGE_HINT_KEYS):
        return False
    # If it also contains obvious per-day container keys, don't call it a range total
    if any(k in l for k in ("days", "daily", "by_day", "series", "data", "datapoints")):
        return False
    return True


def norm_date(v):
    """Normalize various date/timestamp forms to YYYY-MM-DD, applying TZ_OFFSET hours."""
    if v is None:
        return None
    if isinstance(v,(int,float)):
        ts=int(v)
        ts = ts//1000 if ts>1_000_000_000_000 else ts
        try:
            dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
            dt_local = dt_utc + timedelta(hours=TZ_OFFSET)
            return dt_local.date().isoformat()
        except Exception:
            return None
    if isinstance(v,str):
        s=v.strip()
        # Try datetime strings (apply offset when time present)
        for fmt in ("%Y-%m-%dT%H:%M:%S%z","%Y-%m-%dT%H:%M:%S.%fZ","%Y-%m-%dT%H:%M:%SZ","%Y-%m-%d %H:%M:%S"):
            try:
                dtp = datetime.strptime(s,fmt)
                dtp = dtp + timedelta(hours=TZ_OFFSET)
                return dtp.date().isoformat()
            except Exception:
                pass
        # Pure dates (no offset)
        for fmt in ("%Y-%m-%d","%m/%d/%Y"):
            try:
                return datetime.strptime(s,fmt).date().isoformat()
            except Exception:
                pass
        m=re.search(r"(20\d{2}-\d{2}-\d{2})", s)
        if m:
            return m.group(1)
    return None


def coerce_number(v):
    if v is None: return None
    if isinstance(v,(int,float)): return float(v)
    if isinstance(v,str):
        s=v.strip().replace(",","")
        try: return float(s)
        except: return None
    return None


def iter_dicts(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from iter_dicts(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from iter_dicts(it)


def reduce_vals(arr, policy: str) -> float:
    arr = [float(v) for v in arr if v is not None]
    if not arr:
        return 0.0
    if policy == "sum":
        return sum(arr)
    if policy == "min-nonzero":
        pos = [v for v in arr if v > 0]
        return min(pos) if pos else 0.0
    if policy == "min":
        return min(arr)
    if policy == "max":
        return max(arr)
    if policy == "median":
        return median(arr)
    return sum(arr)


def main():
    ap=argparse.ArgumentParser(description="Union numeric fields into a daily CSV (policy reducer, tz-aware, cents/micros, range-total filtering).")
    ap.add_argument("--in", dest="indir", default="./out", help="Directory with captured files")
    ap.add_argument("--csv", dest="csv", default="./out/etsy_ads_daily_allfields.csv", help="Output CSV path")
    ap.add_argument("--glob", default="**/*", help="Glob inside --in (default: **/*)")
    ap.add_argument("--derived", action="store_true", help="Add CTR, CPC, CPM, order_rate, ROAS if possible")
    ap.add_argument("--keep-raw", action="store_true", help="Also include raw source columns alongside remapped ones")
    ap.add_argument("--tz-offset-hours", type=int, default=0, help="Shift dates by this many hours (e.g., -4 for EDT)")
    ap.add_argument("--aggregate-policy", choices=["sum","min-nonzero","min","max","median"], default="sum",
                    help="How to combine multiple values for the same date & metric")
    ap.add_argument("--include-range-totals", action="store_true", help="Keep rows that look like range totals (default: skip)")
    ap.add_argument("--verbose", action="store_true", help="Print a brief report of detected keys")
    args=ap.parse_args()

    global TZ_OFFSET
    TZ_OFFSET = args.tz_offset_hours
    skip_range_totals = not args.include_range_totals

    indir=Path(args.indir).expanduser().resolve()
    files=[p for p in indir.glob(args.glob) if p.is_file()]
    if not files:
        print(f"[!] No files under {indir}")
        return 2

    # Collect ALL values per date & metric; reduce at the end per policy
    values_by_date = defaultdict(lambda: defaultdict(list))  # date -> metric -> [vals]
    raw_cols = set()
    key_freq = Counter()
    parsed=0

    for fp in files:
        try:
            txt = fp.read_text(encoding="utf-8", errors="replace").strip()
        except Exception:
            continue
        if not txt or (not txt.startswith("{") and not txt.startswith("[")):
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue
        parsed += 1

        for d in iter_dicts(data):
            if skip_range_totals and looks_like_range_total(d):
                continue

            # date
            date_val = None
            for dk in d.keys():
                if dk.lower() in DATE_KEYS:
                    date_val = d[dk]; break
            date_iso = norm_date(date_val)
            if not date_iso:
                continue

            # numeric metrics
            keyset = {k.lower() for k in d.keys()}
            for k,v in d.items():
                kl = k.lower()
                if kl in DATE_KEYS or kl in SKIP_KEYS:
                    continue
                num = coerce_number(v)
                if num is None:
                    continue
                key_freq[kl]+=1
                raw_cols.add(kl)

                # precedence: if *_cents/_micros exist for spend/revenue, ignore bare
                if kl in ("spend","revenue") and (
                    any(s in keyset for s in (f"{kl}_cents", f"{kl}_micros")) or
                    (kl == "spend" and any(s in keyset for s in CENTS_LIKE))
                ):
                    continue

                # scaling + base name
                if kl.endswith("_cents"):
                    num = num/100.0; base = kl[:-6]
                elif kl.endswith("_micros"):
                    num = num/1_000_000.0; base = kl[:-7]
                else:
                    base = kl
                    if base in CENTS_LIKE:
                        num = num/100.0

                std = REMAP.get(base, base)
                values_by_date[date_iso][std].append(num)
                if args.keep_raw and base != std:
                    values_by_date[date_iso][base].append(num)

    if not values_by_date:
        print(f"[!] Parsed {parsed} JSONs but found no dated numeric rows. Toggle date range and recapture.")
        return 3

    # Decide final columns: standards first, then rest alpha
    standard_order = ["views","clicks","spend","orders","revenue"]
    all_cols = set()
    for _, metrics in values_by_date.items():
        all_cols.update(metrics.keys())
    for s in standard_order: all_cols.add(s)
    other_cols = sorted(c for c in all_cols if c not in standard_order)
    header = ["date"] + standard_order + other_cols

    # include derived metric columns if requested
    if args.derived:
        header += ["ctr","cpc","cpm","order_rate","roas"]

    # Write CSV with policy-based reduction
    out = Path(args.csv).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("w", newline="", encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=header); w.writeheader()
        for d in sorted(values_by_date.keys()):
            metric_map = values_by_date[d]
            row = {"date": d}
            # canonical first
            for k in standard_order:
                row[k] = round(reduce_vals(metric_map.get(k, []), args.aggregate_policy), 6)
            # then all others
            for k in other_cols:
                row[k] = round(reduce_vals(metric_map.get(k, []), args.aggregate_policy), 6)

            if args.derived:
                v=row.get("views",0.0); c=row.get("clicks",0.0); s=row.get("spend",0.0); o=row.get("orders",0.0); r=row.get("revenue",0.0)
                row["ctr"]        = round((c/v) if v else 0.0, 6)
                row["cpc"]        = round((s/c) if c else 0.0, 6)
                row["cpm"]        = round((s/(v/1000.0)) if v else 0.0, 6)
                row["order_rate"] = round((o/c) if c else 0.0, 6)
                row["roas"]       = round((r/s) if s else 0.0, 6)

            w.writerow({k: row.get(k, "") for k in header})

    print(f"[✓] Wrote {out} with policy={args.aggregate_policy}. Parsed JSONs: {parsed}")
    if args.verbose:
        common = ', '.join([k for k,_ in Counter({k:0 for k in all_cols}).most_common(15)])
        print(f"[i] Columns included: {common}")

if __name__ == "__main__":
    raise SystemExit(main())

