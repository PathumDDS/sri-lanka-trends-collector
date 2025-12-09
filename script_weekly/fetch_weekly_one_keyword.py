# script_weekly/fetch_weekly_one_keyword.py
# WEEKLY FETCHER FOR NORMAL GOOGLE SEARCH TERMS (CORRECT WEEK ALIGNMENT)
# - Fetches each window sequentially with fresh pytrends session
# - Does NOT save window files when keyword FAILs
# - Adds verbose per-window logs
# - Ensures processing.txt is cleaned up on final move-to-failed/processed
# - Does median scaling & stitching unchanged

import os, time, traceback, pandas as pd, random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
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

# ----------------- Window size configuration -----------------
WINDOW_YEARS = 5
STEP_YEARS = 4
START_DATE = datetime(2015, 1, 1)
MAX_RETRIES = 5
BACKOFF = 20

# ----------------- Small session/jitter config -----------------
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
]
MIN_JITTER = 1.0
MAX_JITTER = 3.0

# ----------------- Logging helpers -----------------
def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(RUN_LOG, "a", encoding="utf-8") as f:
        f.write(f"{ts} - {msg}\n")
    print(msg)

def append_line(path, line):
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def read_lines(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [l.rstrip("\n") for l in f if l.strip()]

def pop_keyword():
    lines = read_lines(UNPRO)
    if not lines:
        return None
    first = lines[0]
    with open(UNPRO, "w", encoding="utf-8") as f:
        for l in lines[1:]:
            f.write(l + "\n")
    return first

def save_status_move(keyword, target):
    if os.path.exists(PROCING):
        lines = read_lines(PROCING)
        lines = [l for l in lines if l != keyword]
        with open(PROCING, "w", encoding="utf-8") as f:
            for l in lines:
                f.write(l + "\n")
    append_line(target, keyword)

def sanitize_for_filename(name):
    s = "".join(c if c.isalnum() or c in (" ", "_") else "_" for c in name)
    s = s.strip()
    return s.replace(" ", "_") if s else "keyword"

def _choose_user_agent():
    return random.choice(_USER_AGENTS)

def _sleep_jitter():
    time.sleep(random.uniform(MIN_JITTER, MAX_JITTER))

# ----------------- Fetch one window -----------------
def fetch_window(kw_search, start, end, safe_kw):
    start_adj = start - timedelta(days=(start.weekday() + 1) % 7)
    end_adj = end + timedelta(days=(6 - end.weekday()) % 7)
    timeframe = f"{start_adj:%Y-%m-%d} {end_adj:%Y-%m-%d}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Fresh pytrends session per window
            ua = _choose_user_agent()
            pytrends = TrendReq(hl="en-US", tz=TZ, requests_args={"headers": {"User-Agent": ua}})

            _sleep_jitter()
            pytrends.build_payload([kw_search], timeframe=timeframe, geo=GEO)
            df = pytrends.interest_over_time()

            full_idx = pd.date_range(start_adj, end_adj, freq="W-SUN")
            if df is None or df.empty:
                log(f"Window {start.date()}–{end.date()} empty")
                return pd.DataFrame(index=full_idx, columns=[safe_kw])

            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])

            df = df.rename(columns={df.columns[0]: safe_kw})
            df = df.reindex(full_idx)

            log(f"Window {start.date()}–{end.date()} fetched, shape {df.shape}, non-null {int(df[safe_kw].notna().sum())}")
            return df
        except Exception as ex:
            log(f"Exception fetching window {start.date()}–{end.date()} (attempt {attempt}): {ex}")
            time.sleep(BACKOFF * attempt)
    # final fallback
    full_idx = pd.date_range(start_adj, end_adj, freq="W-SUN")
    return pd.DataFrame(index=full_idx, columns=[safe_kw])

# ----------------- Compute windows -----------------
def compute_windows():
    windows = []
    cur = START_DATE
    now = datetime.utcnow() - timedelta(days=1)
    while cur + relativedelta(years=WINDOW_YEARS) <= now:
        windows.append((cur, cur + relativedelta(years=WINDOW_YEARS)))
        cur += relativedelta(years=STEP_YEARS)
    if (now - cur).days >= 7:
        windows.append((cur, now))
    return windows

# ----------------- Stitching -----------------
def stitch_windows(windows_list):
    if not windows_list:
        return None
    stitched = windows_list[0][0].copy().sort_index()
    for i in range(1, len(windows_list)):
        prev_df, prev_s, prev_e = windows_list[i - 1]
        df, s, e = windows_list[i]
        overlap_start = max(prev_s, s)
        overlap_end = min(prev_e, e)
        overlap_old = stitched.loc[overlap_start:overlap_end]
        overlap_new = df.loc[overlap_start:overlap_end]
        try:
            cond = len(overlap_old.dropna()) > 0 and len(overlap_new.dropna()) > 0 and overlap_new.median().iloc[0] > 0
        except Exception:
            cond = False
        if cond:
            scale = overlap_old.median().iloc[0] / overlap_new.median().iloc[0]
            df_scaled = df * scale
        else:
            df_scaled = df
        tail = df_scaled.loc[prev_e + timedelta(days=1):]
        stitched = pd.concat([stitched, tail])
        stitched = stitched[~stitched.index.duplicated(keep="last")]
    return stitched.sort_index()

# ----------------- Main -----------------
def main():
    keyword = pop_keyword()
    if not keyword:
        log("No weekly keywords left.")
        return

    append_line(PROCING, keyword)
    safe_kw = sanitize_for_filename(keyword)
    kw_search = keyword.strip()
    log(f"Fetching weekly keyword: {keyword}")

    win_list = compute_windows()
    if not win_list:
        log("No windows computed")
        save_status_move(keyword, FAILED)
        return

    collected = []
    non_empty_count = 0
    for (s, e) in win_list:
        df = fetch_window(kw_search, s, e, safe_kw)
        if int(df[safe_kw].notna().sum()) > 0:
            non_empty_count += 1
        collected.append((df, s, e))

    if non_empty_count == 0:
        log(f"Keyword has NO data → FAIL: {keyword}")
        save_status_move(keyword, FAILED)
        return

    # Save raw windows
    win_dir = os.path.join(RAW_WINDOWS, safe_kw)
    os.makedirs(win_dir, exist_ok=True)
    for (df, s, e) in collected:
        fname = f"{safe_kw}_{s.strftime('%Y%m%d')}_{e.strftime('%Y%m%d')}.csv"
        df.to_csv(os.path.join(win_dir, fname))

    # Stitch and save
    stitched = stitch_windows(collected)
    if stitched is None:
        log("Stitching failed")
        save_status_move(keyword, FAILED)
        return
    stitched.to_csv(os.path.join(RAW_WEEKLY, f"{safe_kw}_weekly.csv"))
    log(f"Saved stitched weekly file for {keyword}")
    save_status_move(keyword, PROCED)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("Unexpected error: " + str(e))
        traceback.print_exc()
