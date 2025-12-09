# script_weekly/fetch_weekly_one_window.py
import os, time, traceback, pandas as pd, random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pytrends.request import TrendReq

# Paths
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KW_DIR = os.path.join(ROOT, "keywords_weekly")
RAW_WINDOWS = os.path.join(ROOT, "data_weekly", "raw_windows")
LOGS = os.path.join(ROOT, "logs_weekly")
os.makedirs(LOGS, exist_ok=True)
os.makedirs(RAW_WINDOWS, exist_ok=True)

UNPRO = os.path.join(KW_DIR, "unprocessed.txt")
PROCING = os.path.join(KW_DIR, "processing.txt")
FAILED = os.path.join(KW_DIR, "failed.txt")
RUN_LOG = os.path.join(LOGS, "runs.log")

GEO = "LK"
TZ = 330
WINDOW_YEARS = 5
STEP_YEARS = 4
START_DATE = datetime(2015, 1, 1)
MAX_RETRIES = 5
BACKOFF = 20

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0) AppleWebKit/605.1.15 Version/16.0 Mobile/15E148 Safari/604.1"
]

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
    time.sleep(random.uniform(1.0, 3.0))

def fetch_window(kw_search, start, end, safe_kw):
    start_adj = start - timedelta(days=(start.weekday() + 1) % 7)
    end_adj = end + timedelta(days=(6 - end.weekday()) % 7)
    timeframe = f"{start_adj:%Y-%m-%d} {end_adj:%Y-%m-%d}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
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
    full_idx = pd.date_range(start_adj, end_adj, freq="W-SUN")
    return pd.DataFrame(index=full_idx, columns=[safe_kw])

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

def main():
    keyword = pop_keyword()
    if not keyword:
        log("No weekly keywords left.")
        return

    append_line(PROCING, keyword)
    safe_kw = sanitize_for_filename(keyword)
    kw_search = keyword.strip()
    log(f"Fetching weekly keyword: {keyword}")

    windows = compute_windows()
    # fetch only **one window per run**
    if not windows:
        log("No windows computed")
        save_status_move(keyword, FAILED)
        return

    # pick the first unprocessed window
    s, e = windows[0]
    df = fetch_window(kw_search, s, e, safe_kw)

    # Save raw window
    win_dir = os.path.join(RAW_WINDOWS, safe_kw)
    os.makedirs(win_dir, exist_ok=True)
    fname = f"{safe_kw}_{s.strftime('%Y%m%d')}_{e.strftime('%Y%m%d')}.csv"
    df.to_csv(os.path.join(win_dir, fname))

    log(f"Saved raw window for {keyword}: {fname}")
    save_status_move(keyword, PROCED)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("Unexpected error: " + str(e))
        traceback.print_exc()
