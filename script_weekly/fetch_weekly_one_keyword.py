# script_weekly/fetch_weekly_one_keyword.py
# OECD-CORRECT WEEKLY FETCHER (FINAL CLEAN VERSION)
# Accepts empty windows (fills with NaN)
# Fails only if ALL windows are empty
# Robust column detection, robust stitching, no KeyErrors

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

GEO = "LK"
TZ = 330
WINDOW_DAYS = 90
STEP_DAYS = 30

START_DATE = datetime(2015, 1, 1)
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
    if not os.path.exists(UNPRO):
        return None
    with open(UNPRO, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]
    if not lines:
        return None

    first = lines[0]
    rest = lines[1:]
    with open(UNPRO, "w", encoding="utf-8") as f:
        for r in rest:
            f.write(r + "\n")
    return first


def save_status_move(keyword, target):
    if os.path.exists(PROCING):
        with open(PROCING, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip() and l.strip() != keyword]
        with open(PROCING, "w", encoding="utf-8") as f:
            for l in lines:
                f.write(l + "\n")
    append_line(target, keyword)


# ----------------- Filename sanitizing -----------------
def sanitize_for_filename(name):
    s = "".join(c if c.isalnum() or c in (" ", "_") else "_" for c in name)
    s = s.strip()
    return s.replace(" ", "_") if s else "keyword"


# ----------------- Fetch window -----------------
def fetch_window(pytrends, kw, start, end, safe_kw):
    timeframe = f"{start.strftime('%Y-%m-%d')} {end.strftime('%Y-%m-%d')}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            pytrends.build_payload([kw], timeframe=timeframe, geo=GEO, tz=TZ)
            df = pytrends.interest_over_time()

            # Treat None or empty as empty window
            if df is None or df.empty:
                return pd.DataFrame()

            # Clean isPartial
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])

            # Detect real column
            real_cols = [c for c in df.columns if c != "isPartial"]
            if not real_cols:
                return pd.DataFrame()

            real_col = real_cols[0]

            # Rename to safe kw
            df = df.rename(columns={real_col: safe_kw})

            return df

        except Exception:
            if attempt == MAX_RETRIES:
                return pd.DataFrame()
            time.sleep(BACKOFF * attempt)

    return pd.DataFrame()


# ----------------- Compute windows -----------------
def compute_windows():
    windows = []
    cur = START_DATE
    now = datetime.utcnow()
    while cur + timedelta(days=WINDOW_DAYS) <= now:
        windows.append((cur, cur + timedelta(days=WINDOW_DAYS)))
        cur += timedelta(days=STEP_DAYS)
    return windows


# ----------------- Stitch windows -----------------
def stitch_windows(windows_list):
    if not windows_list:
        return None

    stitched = windows_list[0][0].copy()

    for i in range(1, len(windows_list)):
        prev_df, prev_s, prev_e = windows_list[i - 1]
        df, s, e = windows_list[i]

        # Overlap range
        overlap_start = max(prev_s, s)
        overlap_end = min(prev_e, e)

        # Extract overlap
        overlap_old = stitched.loc[overlap_start:overlap_end]
        overlap_new = df.loc[overlap_start:overlap_end]

        # Scaling logic
        try:
            cond = (
                len(overlap_old) > 0
                and len(overlap_new) > 0
                and overlap_new.median().iloc[0] > 0
            )
        except:
            cond = False

        if cond:
            scale = overlap_old.median().iloc[0] / overlap_new.median().iloc[0]
            df_scaled = df * scale
        else:
            df_scaled = df

        # Append tail only (non-overlap)
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

    pytrends = TrendReq(hl="en-US", tz=TZ)
    win_list = compute_windows()
    collected = []
    non_empty_count = 0

    win_dir = os.path.join(RAW_WINDOWS, safe_kw)
    os.makedirs(win_dir, exist_ok=True)

    for (s, e) in win_list:
        df = fetch_window(pytrends, keyword, s, e, safe_kw)

        # Create complete weekly index
        full_idx = pd.date_range(s, e, freq="W-SAT")
        df = df.reindex(full_idx)

        # Count non-empty windows
        if safe_kw in df.columns and df[safe_kw].notna().sum() > 0:
            non_empty_count += 1

        # Save raw window
        fname = f"{safe_kw}_{s.strftime('%Y%m%d')}_{e.strftime('%Y%m%d')}.csv"
        df.to_csv(os.path.join(win_dir, fname))

        collected.append((df, s, e))

    # ALL windows empty → hard fail
    if non_empty_count == 0:
        log(f"Keyword has NO data in ANY window → FAIL: {keyword}")
        save_status_move(keyword, FAILED)
        return

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


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("Unexpected error: " + str(e))
        traceback.print_exc()
