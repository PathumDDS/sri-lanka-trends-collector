# script/fetch_weekly_keyword.py
# Fetch weekly Google Trends for ONE keyword using rolling windows (3y window, 1y step).
# Robust per-window retries, jitter, and median overlap scaling (OECD-style).
#
# Usage (locally or in GitHub Actions):
#   python script/fetch_weekly_keyword.py "keyword phrase"
#
# Output directories (created automatically):
#   data/raw_windows/      - per-window CSVs (debug)
#   data/raw_weekly/       - final stitched weekly CSV for the keyword
#   data/preview/          - PNG preview plot (optional)
#
# NOTE: Non-destructive. Does NOT change keyword status files.

import os
import sys
import glob
import time
import math
import random
from datetime import datetime, date
from statistics import median
import numpy as np
import pandas as pd
from pytrends.request import TrendReq

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_WINDOWS_DIR = os.path.join(ROOT, "data", "raw_windows")
RAW_WEEKLY_DIR = os.path.join(ROOT, "data", "raw_weekly")
PREVIEW_DIR = os.path.join(ROOT, "data", "preview")
LOG_FILE = os.path.join(ROOT, "logs", "weekly_runs.log")

for d in (RAW_WINDOWS_DIR, RAW_WEEKLY_DIR, PREVIEW_DIR, os.path.join(ROOT, "logs")):
    os.makedirs(d, exist_ok=True)

# Params (OECD-style)
WINDOW_MONTHS = 36   # 3 years
STEP_MONTHS = 12     # 1 year step => overlap ~24 months
MIN_OVERLAP_WEEKS = 20

# Fetching / retry
MAX_RETRIES = 5
INITIAL_BACKOFF = 30   # seconds
JITTER_MAX = 10        # seconds extra jitter

# Pytrends config
GEO = "LK"
TZ = 330  # Sri Lanka +5:30

def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"{ts} - {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def add_months(d, months):
    # add months to a date
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    day = min(d.day, [31,
                      29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28,
                      31,30,31,30,31,31,30,31,30,31][month-1])
    return date(year, month, day)

def generate_windows(start_date, end_date, window_months=WINDOW_MONTHS, step_months=STEP_MONTHS):
    windows = []
    s = start_date
    while True:
        e = add_months(s, window_months)
        if e > end_date:
            e = end_date
        windows.append((s, e))
        if e >= end_date:
            break
        s = add_months(s, step_months)
    return windows

def timeframe_str(s, e):
    # pytrends timeframe string "YYYY-MM-DD YYYY-MM-DD"
    return f"{s.strftime('%Y-%m-%d')} {e.strftime('%Y-%m-%d')}"

def safe_name(kw):
    return kw.replace(" ", "_").replace("/", "_")

def fetch_window(pytrends, kw, timeframe):
    # tries to fetch the window; implements retries + exponential backoff + jitter
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            pytrends.build_payload([kw], cat=0, timeframe=timeframe, geo=GEO, gprop="")
            df = pytrends.interest_over_time()
            if df is None or df.empty:
                raise ValueError("Empty result")
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])
            # rename column
            df = df.rename(columns={kw: safe_name(kw)})
            return df
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            backoff = INITIAL_BACKOFF * (2 ** (attempt - 1))
            jitter = random.uniform(0, JITTER_MAX)
            wait = backoff + jitter
            log(f"Window fetch failed (attempt {attempt}) for '{kw}' timeframe {timeframe}: {e}. Backing off {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError("Unreachable")

def stitch_windows(windows_dfs):
    # windows_dfs = list of (start_date, end_date, df)
    # each df indexed by date (weekly). We stitch using median ratio in overlaps.
    if not windows_dfs:
        return None

    # Start with first window as base
    base_start, base_end, base_df = windows_dfs[0]
    result = base_df.copy()
    result.index = pd.to_datetime(result.index)
    # iterate next windows
    for (s, e, df) in windows_dfs[1:]:
        df.index = pd.to_datetime(df.index)
        # find overlap period dates
        overlap_index = result.index.intersection(df.index)
        if len(overlap_index) < MIN_OVERLAP_WEEKS:
            log(f"WARNING: insufficient overlap ({len(overlap_index)} weeks) between {s} and previous. Using available overlap.")
        # compute per-week ratios where df value > 0
        ratios = []
        for idx in overlap_index:
            a = result.loc[idx].iloc[0] if idx in result.index else np.nan
            b = df.loc[idx].iloc[0] if idx in df.index else np.nan
            # avoid division by zero or nan
            if pd.isna(a) or pd.isna(b) or b == 0:
                continue
            ratios.append(a / b)
        if not ratios:
            # can't scale -- skip scaling (append raw) but warn
            log(f"WARNING: no valid ratios found in overlap; appending raw window without scaling for window {s} to {e}")
            scaled_df = df.copy()
        else:
            k = float(median(ratios))
            log(f"Scaling factor for window {s} → {e} is median={k:.6f} (based on {len(ratios)} overlap weeks)")
            scaled_df = df * k
        # append only non-overlapping periods from scaled_df
        to_append = scaled_df.loc[~scaled_df.index.isin(result.index)]
        if not to_append.empty:
            result = pd.concat([result, to_append])
        # if overlap had data but result's values differ slightly, we keep base values for overlap
    result = result[~result.index.duplicated(keep='first')]
    result.sort_index(inplace=True)
    return result

def save_df_csv(df, path):
    df.to_csv(path)
    log(f"Saved CSV: {path}")

def make_preview_plot(original_windows, scaled_concat, out_png):
    # original_windows: list of (s,e,df) for plotting raw windows
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        plt.figure(figsize=(10, 5))
        # plot raw windows (thin lines)
        for (s, e, df) in original_windows:
            df_plot = df.copy()
            df_plot.index = pd.to_datetime(df_plot.index)
            plt.plot(df_plot.index, df_plot.iloc[:,0], alpha=0.25)
        # plot final stitched (thicker)
        plt.plot(pd.to_datetime(scaled_concat.index), scaled_concat.iloc[:,0], linewidth=1.2)
        plt.title("Weekly series: raw windows (thin) and stitched (thick)")
        plt.xlabel("Date")
        plt.ylabel("Scaled interest (relative)")
        plt.tight_layout()
        plt.savefig(out_png, dpi=150)
        plt.close()
        log(f"Saved preview plot: {out_png}")
    except Exception as e:
        log(f"Could not create preview plot: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python script/fetch_weekly_keyword.py \"keyword phrase\"")
        sys.exit(1)
    kw = sys.argv[1].strip()
    log(f"START weekly fetch for keyword: {kw}")

    today = date.today()
    start = date(2008, 1, 1)  # same historical start as monthly
    windows = generate_windows(start, today, WINDOW_MONTHS, STEP_MONTHS)
    log(f"Generated {len(windows)} windows from {start} to {today}")

    pytrends = TrendReq(hl="en-US", tz=TZ)
    windows_dfs = []
    for (s, e) in windows:
        tf = timeframe_str(s, e)
        log(f"Fetching window {s} → {e} timeframe='{tf}'")
        try:
            df = fetch_window(pytrends, kw, tf)
            # save per-window CSV
            safe = safe_name(kw)
            fname = f"{safe}_{s.strftime('%Y%m%d')}_{e.strftime('%Y%m%d')}.csv"
            fpath = os.path.join(RAW_WINDOWS_DIR, fname)
            save_df_csv(df, fpath)
            windows_dfs.append((s, e, df))
            # tiny polite pause between windows
            time.sleep(random.uniform(1, 4))
        except Exception as e:
            log(f"ERROR fetching window {s}→{e} for '{kw}': {e}")
            # continue to next window (we still try stitching what we have)
            continue

    if not windows_dfs:
        log("No windows downloaded successfully. Exiting.")
        return

    stitched = stitch_windows(windows_dfs)
    if stitched is None or stitched.empty:
        log("Stitching failed or produced empty series. Exiting.")
        return

    # Save final stitched weekly CSV
    safe = safe_name(kw)
    outcsv = os.path.join(RAW_WEEKLY_DIR, f"{safe}_weekly.csv")
    save_df_csv(stitched, outcsv)

    # Save preview plot
    preview_png = os.path.join(PREVIEW_DIR, f"{safe}_weekly_preview.png")
    try:
        make_preview_plot(windows_dfs, stitched, preview_png)
    except Exception as e:
        log(f"Preview plotting failed: {e}")

    log(f"FINISHED weekly fetch for keyword: {kw}")

if __name__ == "__main__":
    main()
