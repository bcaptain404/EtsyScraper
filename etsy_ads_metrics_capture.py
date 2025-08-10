#!/usr/bin/env python3
"""
etsy_ads_scraper.py

Launches a persistent Chrome session using your existing Google Chrome profile (e.g. "Profile 2"),
navigates to Etsy's Ads dashboard, and captures the JSON responses the page requests. Matching
responses are saved to disk, and a best-effort CSV is produced from daily metrics where possible.

Usage examples:
  python3 etsy_ads_scraper.py --profile-dir "$HOME/.config/google-chrome/Profile 2" --out-dir ./out --headful
  python3 etsy_ads_scraper.py --profile-dir "$HOME/.config/chromium/Profile 2" --out-dir ./out --headful

Notes:
- This script does NOT bypass authentication. It reuses your logged-in Chrome profile so Etsy treats
  it as you. If you're not logged in under that profile, log in first and then rerun.
- It listens for XHR/Fetch responses and tries to parse JSON. It saves all JSON bodies and attempts
  to detect daily metrics payloads (impressions/views, clicks, spend, orders, revenue, date).
- Etsy can and does change private endpoints. This script is endpoint-agnostic and uses content-based
  heuristics to select relevant responses.

Dependencies:
  pip install playwright
  python -m playwright install chrome
  # If you don't have Google Chrome, you can use Chromium; adjust --profile-dir and --browser-channel.

"""

import argparse
import asyncio
import csv
import datetime as dt
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import async_playwright, Page, BrowserContext, Response

# Heuristic keys that suggest an ads metrics payload
METRIC_HINT_KEYS = {
    "views", "impressions", "clicks", "ctr", "spend", "orders", "conversions",
    "revenue", "sales", "date", "day", "timestamp", "attributed_orders"
}

# Common field name mappings for normalization
FIELD_ALIASES = {
    "views": ["views", "impressions"],
    "clicks": ["clicks"],
    "spend": ["spend", "cost"],
    "orders": ["orders", "conversions", "attributed_orders"],
    "revenue": ["revenue", "sales", "attributed_sales"],
    "date": ["date", "day", "timestamp"]
}

DEFAULT_OUT = "./out"
DEFAULT_URL = "https://www.etsy.com/your/shops/me/advertising"  # Etsy may redirect; that's fine.


def now_stamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def looks_like_metrics_payload(obj: Any) -> bool:
    """Return True if the JSON looks like it contains ads metrics data."""
    try:
        if isinstance(obj, dict):
            keys = set(k.lower() for k in obj.keys())
            if keys & METRIC_HINT_KEYS:
                return True
            # Sometimes the interesting part is nested under e.g. { data: [...] }
            for v in obj.values():
                if looks_like_metrics_payload(v):
                    return True
        elif isinstance(obj, list):
            if not obj:
                return False
            # If it's a list of dicts with metric-like keys or a date field, count that.
            sample = obj[0]
            if isinstance(sample, dict):
                keys = set(k.lower() for k in sample.keys())
                if keys & METRIC_HINT_KEYS:
                    return True
            # Or list of primitives (probably not metrics)
        return False
    except Exception:
        return False


def extract_daily_rows(obj: Any) -> List[Dict[str, Any]]:
    """
    Traverse the JSON object and collect rows that look like daily metrics.
    Returns a list of normalized rows: {date, views, clicks, spend, orders, revenue}
    Missing fields are left out.
    """
    rows: List[Dict[str, Any]] = []

    def norm_date(v: Any) -> Optional[str]:
        # Accept ISO date, or epoch seconds/ms, or YYYY-MM-DD strings.
        if v is None:
            return None
        if isinstance(v, (int, float)):
            # guess epoch seconds vs ms
            ts = int(v)
            if ts > 1_000_000_000_000:  # ms
                ts = ts // 1000
            try:
                return dt.datetime.utcfromtimestamp(ts).date().isoformat()
            except Exception:
                return None
        if isinstance(v, str):
            s = v.strip()
            # Try to parse common formats
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                try:
                    return dt.datetime.strptime(s, fmt).date().isoformat()
                except Exception:
                    pass
            # Fallback: YYYY-MM-DD-ish via regex
            m = re.search(r"(20\d{2}-\d{2}-\d{2})", s)
            if m:
                return m.group(1)
        return None

    def alias_lookup(d: Dict[str, Any], target: str) -> Optional[Any]:
        for name in FIELD_ALIASES[target]:
            if name in d:
                return d[name]
            lname = name.lower()
            for k in d.keys():
                if k.lower() == lname:
                    return d[k]
        return None

    def visit(node: Any):
        if isinstance(node, list):
            for item in node:
                visit(item)
        elif isinstance(node, dict):
            # Candidate row
            keys = {k.lower() for k in node.keys()}
            if keys & METRIC_HINT_KEYS:
                date_value = alias_lookup(node, "date")
                date_iso = norm_date(date_value)
                if date_iso:
                    rec: Dict[str, Any] = {"date": date_iso}
                    for field in ("views", "clicks", "spend", "orders", "revenue"):
                        val = alias_lookup(node, field)
                        if val is not None:
                            rec[field] = val
                    if len(rec) > 1:  # has at least date + one metric
                        rows.append(rec)
            # Continue descending for nested structures
            for v in node.values():
                visit(v)
        else:
            return

    visit(obj)

    # Deduplicate by (date, all metric values)
    uniq = []
    seen = set()
    for r in rows:
        key = tuple(sorted(r.items()))
        if key not in seen:
            seen.add(key)
            uniq.append(r)
    return uniq


def write_json(out_dir: Path, base: str, data: Any) -> Path:
    p = out_dir / f"{base}_{now_stamp()}.json"
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return p


def write_csv(out_dir: Path, base: str, rows: List[Dict[str, Any]]) -> Optional[Path]:
    if not rows:
        return None
    # Determine header union
    header = ["date", "views", "clicks", "spend", "orders", "revenue"]
    p = out_dir / f"{base}_{now_stamp()}.csv"
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in sorted(rows, key=lambda x: x.get("date", "")):
            w.writerow({k: r.get(k, "") for k in header})
    return p


async def run(args: argparse.Namespace) -> int:
    out_dir = Path(args.out_dir)
    ensure_dir(out_dir)

    browser_channel = args.browser_channel
    executable_path = args.executable

    # Use persistent context to reuse cookies from your profile (no login script needed)
    user_data_dir = Path(args.profile_dir).expanduser()
    if not user_data_dir.exists():
        print(f"[!] Profile root not found: {user_data_dir}")
        return 2

    if args.chrome_profile_name:
        prefs = user_data_dir / args.chrome_profile_name / "Preferences"
        if not prefs.exists():
            print(f"[!] Could not find profile '{args.chrome_profile_name}' under {user_data_dir}")
            print("    Try 'Default', 'Profile 1', 'Profile 2', ... or see chrome://version → Profile Path")
            return 2

    print(f"[*] Launching Chrome with profile: {user_data_dir}")

    async with async_playwright() as pw:
        browser_type = pw.chromium
        context: BrowserContext = await browser_type.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=not args.headful,
            channel=browser_channel if executable_path is None else None,
            executable_path=executable_path,
            ignore_default_args=["--enable-automation"],
            args=[
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                f"--profile-directory={args.chrome_profile_name or 'Profile 2'}",
                #*( [f"--profile-directory={args.chrome_profile_name}"] if args.chrome_profile_name else [] ),
                #"--profile-directory=Profile 2",
            ],
        )
        # Hide navigator.webdriver
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        try:
            # 1) Simple handler: PARSE + SAVE ONLY. No new pages, no re-registering.
            captured_files: List[Path] = []
            aggregated_rows: List[Dict[str, Any]] = []

            async def on_response(res: Response):
                try:
                    req = res.request
                    if req.resource_type not in ("xhr", "fetch"):
                        return
                    url = res.url
                    if args.save_all:
                        print(f"[net] {req.resource_type.upper():6} {res.status} {url}")

                    # loose URL filter + parse JSON-ish
                    if not re.search(r"/api/|/v\\d/|advert|ads|promoted|campaign|metrics", url, re.I):
                        if not args.save_all:
                            return

                    ctype = res.headers.get("content-type", "").lower()
                    data = None
                    if "json" in ctype:
                        data = await res.json()
                    elif args.save_all:
                        try:
                            txt = await res.text()
                            data = json.loads(txt)
                        except Exception:
                            data = None
                    if data is None:
                        return

                    base = re.sub(r"[^A-Za-z0-9]+", "_", url.split("?")[0])[-80:]
                    jp = write_json(out_dir, base, data)
                    captured_files.append(jp)
                    print(f"[+] JSON saved: {jp.name}")

                    if looks_like_metrics_payload(data):
                        rows = extract_daily_rows(data)
                        if rows:
                            aggregated_rows.extend(rows)
                            print(f"[+] Extracted {len(rows)} row(s) from {jp.name}")
                except Exception as e:
                    print(f"[warn] on_response error: {e}")

            # 2) One-time wiring & logging hooks
            context.set_default_timeout(30000)
            def _on_page(p):
                print(f"[page] opened: {p.url}")
                p.on("framenavigated", lambda fr: print(f"[nav] {fr.url}"))
                p.on("requestfailed",  lambda r:  print(f"[reqfail] {r.url} :: {r.failure.error_text if r.failure else 'unknown'}"))
                p.on("console",        lambda m:  print(f"[console:{m.type}] {m.text}"))
            context.on("page", _on_page)

            context.on("response", on_response)  # <-- REGISTER ONCE

            # 3) Open one page and navigate
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
            page: Page = await context.new_page()
            print(f"[*] goto → {args.url}")
            await page.goto(args.url, wait_until="domcontentloaded")

            if page.url in ("about:blank", ""):
                print("[warn] at about:blank → retry goto")
                await page.reload()
                await page.wait_for_timeout(400)
                await page.goto(args.url, wait_until="domcontentloaded")

            if args.autorun:
                try:
                    await page.wait_for_timeout(600)
                    await page.reload()
                    await page.wait_for_load_state("domcontentloaded")
                    import re as _re
                    btn = page.get_by_role("button", name=_re.compile(r"(Last|This|Custom|Date|Day|Week|Month|Year)", _re.I))
                    if await btn.count() > 0:
                        print("[i] autorun: clicking a date-range control")
                        await btn.nth(0).click()
                        await page.wait_for_timeout(800)
                    else:
                        await page.evaluate("""
                            document.dispatchEvent(new Event('visibilitychange'));
                            window.dispatchEvent(new Event('focus'));
                            window.dispatchEvent(new Event('resize'));
                        """)
                except Exception as e:
                    print(f"[warn] autorun nudge failed: {e}")

            # 4) Wait/loop
            if args.keep_open or args.capture_ms <= 0:
                print("[*] live capture: Ctrl+C to stop.")
                try:
                    while True:
                        await asyncio.sleep(3600)
                except KeyboardInterrupt:
                    print("\\n[!] stopping by user")
            else:
                print(f"[*] capturing for {args.capture_ms} ms…")
                await page.wait_for_timeout(args.capture_ms)

            # 5) Optional one-shot CSV
            csv_path = write_csv(out_dir, "etsy_ads_daily", aggregated_rows)
            if csv_path:
                print(f"[✓] Aggregated CSV written: {csv_path}")
            else:
                print("[!] No daily metrics detected yet. Toggle date range and rerun.")

        finally:
            if not args.keep_open:
                await context.close()


    return 0


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Capture Etsy Ads dashboard JSON and export daily CSV where possible.")
    p.add_argument("--chrome-profile-name", default=None, help="Chrome profile directory name (e.g. 'Default', 'Profile 2')")
    p.add_argument("--profile-dir", required=True, help="Path to your Chrome/Chromium user profile dir (e.g. ~/.config/google-chrome/Profile 2)")
    p.add_argument("--out-dir", default=DEFAULT_OUT, help="Where to write JSON/CSV outputs")
    p.add_argument("--url", default=DEFAULT_URL, help="Ads dashboard URL (defaults should redirect correctly)")
    p.add_argument("--headful", action="store_true", help="Run with a visible browser window")
    p.add_argument("--autorun", action="store_true", help="Attempt minimal automatic interaction to trigger requests")
    p.add_argument("--keep-open", action="store_true", help="Keep Chrome open and keep listening indefinitely (Ctrl+C to stop)")
    p.add_argument("--capture-ms", type=int, default=15000, help="How long to listen for network responses (ms)")
    p.add_argument("--save-all", action="store_true", help="Save all JSON XHR/Fetch, even if URL doesn't look ads-related")
    p.add_argument("--browser-channel", default="chrome", help="Playwright browser channel (chrome, chromium)")
    p.add_argument("--executable", default=None, help="Override browser executable path (e.g. /usr/bin/google-chrome-stable)")
    return p.parse_args(argv)


if __name__ == "__main__":
    try:
        ns = parse_args(sys.argv[1:])
        exit(asyncio.run(run(ns)))
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user")
        sys.exit(130)
