# script_weekly/fetch_weekly_one_keyword.py
# OECD-CORRECT WEEKLY FETCHER
# 90-day windows, 30-day step, 60-day overlap, median scaling

import os, time, traceback, pandas as pd
from datetime import datetime, timedelta
from pytrends.request import TrendReq

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KW_DIR = os.path.join(ROOT, "keywords_weekly")
RAW_WINDOWS = os.path.join(ROOT, "data", "raw_windows")
RAW_WEEKLY = os.path.join(ROOT, "data", "raw_weekly")
LOGS = os.path.join(ROOT, "logs")

os.makedirs(RAW_WINDOWS, exist_ok=True)
os.makedirs(RAW_WEEKLY, exist_ok=True)
os.makedirs(LOGS, exist_ok=True)

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
TODAY = datetime.utcnow().date()

MAX_RETRIES = 5
BACKOFF = 60


# ----------------- Helper Functions -----------------

def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(RUN_LOG, "a") as f:
        f.write(f"{ts} - {msg}\n")
    print(msg)


def append_line(path, line):
    with open(path, "a") as f:
        f.write(line + "\n")


def pop_keyword():
    if not os.path.exists(UNPRO):
        return None
    with open(UNPRO, "r") as f:
        lines = [l.strip() for l in f if l.strip()]
    if not lines:
        return None
    kw = lines[0]
    rest = lines[1:]
    with open(UNPRO, "w") as f:
        for r in rest:
            f.write(r + "\n")
    return kw


def save_status_move(keyword, target):
    for file in [PROCING]:
        if os.path.exists(file):
            with open(file, "r") as f:
                lines = [l.strip() for l in f if l.strip() != keyword]
            with open(file, "w") as f:
                for l in lines:
                    f.write(l + "\n")
    append_line(target, keyword)


# ----------------- Window Fetching -----------------

def fetch_window(pytrends, kw, start, end):
    """Fetch one window."""
    timeframe = f"{start.strftime('%Y-%m-%d')} {end.strftime('%Y-%m-%d')}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            pytrends.build_payload([kw], timeframe=timeframe, geo=GEO, tz=TZ)
            df = pytrends.interest_over_time()
            if df is None or df.empty:
                return None
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])
            df = df.rename(columns={kw: kw.replace(" ", "_")})
            return df
        except Exception as e:
            if attempt == MAX_RETRIES:
                return None
            time.sleep(BACKOFF * attempt)
    return None


def compute_windows():
    windows = []
    cur = START_DATE
    while cur + timedelta(days=WINDOW_DAYS) <= datetime.utcnow():
        start = cur
        end = cur + timedelta(days=WINDOW_DAYS)
        windows.append((start, end))
        cur += timedelta(days=STEP_DAYS)
    return windows


# ----------------- Stitching (OECD Correct) -----------------

def stitch_windows(windows_list):
    """
    windows_list: list of (df window, start, end)
    """
    if not windows_list:
        return None

    stitched = windows_list[0][0].copy()

    for i in range(1, len(windows_list)):
        prev_df, prev_s, prev_e = windows_list[i - 1]
        df, s, e = windows_list[i]

        # Overlap region
        overlap_start = max(prev_s, s)
        overlap_end = min(prev_e, e)

        overlap = stitched.loc[overlap_start:overlap_end]
        overlap_new = df.loc[overlap_start:overlap_end]

        # Median scaling
        if len(overlap) > 0 and len(overlap_new) > 0 and overlap_new.median().iloc[0] > 0:
            scale = overlap.median().iloc[0] / overlap_new.median().iloc[0]
            df_scaled = df * scale
        else:
            df_scaled = df

        # Append ONLY the non-overlapping tail of df_scaled
        tail = df_scaled.loc[prev_e + timedelta(days=1):]
        stitched = pd.concat([stitched, tail])

    return stitched


# ----------------- Main Weekly Fetch -----------------

def main():
    kw = pop_keyword()
    if not kw:
        log("No weekly keywords left.")
        return

    append_line(PROCING, kw)
    safe_kw = kw.replace(" ", "_")

    log(f"Fetching weekly: {kw}")

    win_list = compute_windows()
    pytrends = TrendReq(hl="en-US", tz=TZ)

    collected = []

    # Create folder for window raw files
    win_dir = os.path.join(RAW_WINDOWS, safe_kw)
    os.makedirs(win_dir, exist_ok=True)

    # ---- Fetch windows ----
    for (s, e) in win_list:
        df = fetch_window(pytrends, kw, s, e)
        if df is None:
            log(f"FAILED window {s}–{e}")
            save_status_move(kw, FAILED)
            return

        fname = f"{safe_kw}_{s.strftime('%Y%m%d')}_{e.strftime('%Y%m%d')}.csv"
        df.to_csv(os.path.join(win_dir, fname))

        collected.append((df, s, e))

    # ---- Stitch windows ----
    stitched = stitch_windows(collected)
    if stitched is None:
        log("Stitching failed.")
        save_status_move(kw, FAILED)
        return

    outpath = os.path.join(RAW_WEEKLY, f"{safe_kw}_weekly.csv")
    stitched.to_csv(outpath)
    log(f"Saved stitched weekly file: {outpath}")

    save_status_move(kw, PROCED)
    log(f"SUCCESS weekly: {kw}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("Unexpected error: " + str(e))
        traceback.print_exc()
