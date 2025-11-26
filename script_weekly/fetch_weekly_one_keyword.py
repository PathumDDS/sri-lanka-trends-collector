# script_weekly/fetch_weekly_one_keyword.py
# OECD-CORRECT WEEKLY FETCHER
# Accepts empty windows (fills with NaN)
# Fails only if ALL windows are empty
# 90-day windows, 30-day step, 60-day overlap, median scaling

import os, time, traceback, pandas as pd
from datetime import datetime, timedelta
from pytrends.request import TrendReq

# ----------------- Paths -----------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KW_DIR = os.path.join(ROOT, "keywords_weekly")
RAW_WINDOWS = os.path.join(ROOT, "data_weekly", "raw_windows")
RAW_WEEKLY = os.path.join(ROOT, "data_weekly", "raw_weekly")
LOGS = os.path.join(ROOT, "logs_weekly")

os.makedirs(LOGS, exist_ok=True)
os.makedirs(RAW_WINDOWS, exist_ok=True)
os.makedirs(RAW_WEEKLY, exist_ok=True)

UNPRO = os.path.join(KW_DIR, "unprocessed.txt")
PROCING = os.path.join(KW_DIR, "processing.txt")
PROCED = os.path.join(KW_DIR, "processed.txt")
FAILED = os.path.join(KW_DIR, "failed.txt")
RUN_LOG = os.path.join(LOGS, "runs.log")

MASTER_KEYWORDS = os.path.join(KW_DIR, "master_keywords.txt")

GEO = "LK"
TZ = 330
WINDOW_DAYS = 90
STEP_DAYS = 30

START_DATE = datetime(2015, 1, 1)
TODAY = datetime.utcnow().date()

MAX_RETRIES = 5
BACKOFF = 60

# ----------------- Logging helpers -----------------
def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(RUN_LOG, "a") as f:
        f.write(f"{ts} - {msg}\n")
    print(msg)

def append_line(path, line):
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def pop_keyword():
    """Pop the first non-empty line from unprocessed.txt"""
    if not os.path.exists(UNPRO):
        return None
    with open(UNPRO, "r", encoding="utf-8") as f:
        lines = [l.rstrip("\n") for l in f if l.strip()]
    if not lines:
        return None
    first = lines[0].strip()
    rest = lines[1:]
    with open(UNPRO, "w", encoding="utf-8") as f:
        for r in rest:
            f.write(r + "\n")
    return first

def save_status_move(keyword, target):
    """Move keyword to a target file and remove from processing"""
    if os.path.exists(PROCING):
        with open(PROCING, "r", encoding="utf-8") as f:
            lines = [l.rstrip("\n") for l in f if l.strip() and l.strip() != keyword]
        with open(PROCING, "w", encoding="utf-8") as f:
            for l in lines:
                f.write(l + "\n")
    append_line(target, keyword)

# ----------------- Fetch window -----------------
def fetch_window(pytrends, kw, start, end, colname):
    timeframe = f"{start.strftime('%Y-%m-%d')} {end.strftime('%Y-%m-%d')}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            pytrends.build_payload([kw], timeframe=timeframe, geo=GEO, tz=TZ)
            df = pytrends.interest_over_time()

            # Both None and empty → treat as empty window
            if df is None or df.empty:
                df = pd.DataFrame()

            # Remove isPartial if exists
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])

            # Rename first column if exists
            if not df.empty:
                first_col = df.columns[0]
                df = df.rename(columns={first_col: colname})

            return df

        except Exception:
            if attempt == MAX_RETRIES:
                return pd.DataFrame()  # return empty, not fail
            time.sleep(BACKOFF * attempt)

    return pd.DataFrame()

# ----------------- Compute windows -----------------
def compute_windows():
    windows = []
    cur = START_DATE
    while cur + timedelta(days=WINDOW_DAYS) <= datetime.utcnow():
        windows.append((cur, cur + timedelta(days=WINDOW_DAYS)))
        cur += timedelta(days=STEP_DAYS)   # FIXED
    return windows

# ----------------- Stitching -----------------
def stitch_windows(windows_list):
    if not windows_list:
        return None

    stitched = windows_list[0][0].copy()

    for i in range(1, len(windows_list)):
        prev_df, prev_s, prev_e = windows_list[i - 1]
        df, s, e = windows_list[i]

        overlap_start = max(prev_s, s)
        overlap_end = min(prev_e, e)

        overlap = stitched.loc[overlap_start:overlap_end]
        overlap_new = df.loc[overlap_start:overlap_end]

        # Scaling
        if len(overlap) > 0 and len(overlap_new) > 0 and overlap_new.median().iloc[0] > 0:
            scale = overlap.median().iloc[0] / overlap_new.median().iloc[0]
            df_scaled = df * scale
        else:
            df_scaled = df

        # Append only new (non-overlap) part
        tail = df_scaled.loc[prev_e + timedelta(days=1):]
        stitched = pd.concat([stitched, tail])

    return stitched

# ----------------- Main -----------------
def main():
    keyword = pop_keyword()
    if not keyword:
        log("No weekly keywords left.")
        return

    append_line(PROCING, keyword)
    safe_kw = sanitize_for_filename(keyword)
    log(f"Fetching weekly: {keyword}")

    win_list = compute_windows()
    pytrends = TrendReq(hl="en-US", tz=TZ)
    collected = []
    non_empty_count = 0  # track windows with actual data

    win_dir = os.path.join(RAW_WINDOWS, safe_kw)
    os.makedirs(win_dir, exist_ok=True)

    for (s, e) in win_list:

        df = fetch_window(pytrends, keyword, s, e, safe_kw)

        # Create a full weekly index (W-SAT → same as Google Trends output)
        full_idx = pd.date_range(s, e, freq="W-SAT")
        df = df.reindex(full_idx)

        # Count as non-empty only if at least one non-NaN exists
        if df[safe_kw].notna().sum() > 0:
            non_empty_count += 1

        # Save individual window
        fname = f"{safe_kw}_{s.strftime('%Y%m%d')}_{e.strftime('%Y%m%d')}.csv"
        df.to_csv(os.path.join(win_dir, fname))
        collected.append((df, s, e))

    # If ALL windows are empty → hard fail
    if non_empty_count == 0:
        log(f"Keyword has NO data in ANY window → FAIL: {keyword}")
        save_status_move(keyword, FAILED)
        return

    # Stitch
    stitched = stitch_windows(collected)
    if stitched is None:
        log("Stitching failed.")
        save_status_move(keyword, FAILED)
        return

    outpath = os.path.join(RAW_WEEKLY, f"{safe_kw}_weekly.csv")
    stitched.to_csv(outpath)
    log(f"Saved stitched weekly file: {outpath}")

    save_status_move(keyword, PROCED)
    log(f"SUCCESS weekly: {keyword}")

def sanitize_for_filename(name):
    s = "".join(c if c.isalnum() or c in (" ", "_") else "_" for c in name)
    s = s.strip()
    if not s:
        s = "keyword"
    return s.replace(" ", "_")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("Unexpected error: " + str(e))
        traceback.print_exc()
