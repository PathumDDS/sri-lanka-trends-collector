# script/fetch_one_keyword.py
# Fetch one Google Trends keyword (safe for GitHub Actions)
# Requirements: pytrends, pandas

import csv, os, time, traceback
from datetime import datetime
from pytrends.request import TrendReq
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KEYWORDS_DIR = os.path.join(ROOT, "keywords_monthly")
DATA_DIR = os.path.join(ROOT, "data_monthly", "raw")
LOGS_DIR = os.path.join(ROOT, "logs")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

UNPROCESSED = os.path.join(KEYWORDS_DIR, "unprocessed.txt")
PROCESSING = os.path.join(KEYWORDS_DIR, "processing.txt")
PROCESSED = os.path.join(KEYWORDS_DIR, "processed.txt")
FAILED = os.path.join(KEYWORDS_DIR, "failed.txt")
RUN_LOG = os.path.join(LOGS_DIR, "runs.log")

# Config
GEO = "LK"
START_DATE = "2015-01-01"

from datetime import datetime, timedelta

def last_day_of_previous_month():
    today = datetime.utcnow().replace(day=1)
    last_month_end = today - timedelta(days=1)
    return last_month_end.strftime("%Y-%m-%d")

END_DATE = last_day_of_previous_month()
TIMEFRAME = f"{START_DATE} {END_DATE}"

TZ = 330  # Sri Lanka +5:30
MAX_RETRIES = 5
INITIAL_BACKOFF = 60

def append_line(path, line):
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.strip() + "\n")

def pop_first_unprocessed():
    # Atomically read unprocessed, pop first non-empty (ignore comment lines)
    if not os.path.exists(UNPROCESSED):
        return None
    with open(UNPROCESSED, "r", encoding="utf-8") as f:
        lines = [l.rstrip("\n") for l in f.readlines()]
    # find first valid
    idx = None
    for i, l in enumerate(lines):
        s = l.strip()
        if s and not s.startswith("#"):
            idx = i
            kw = s
            break
    if idx is None:
        return None
    # remove that line and write back remaining
    remaining = lines[:idx] + lines[idx+1:]
    with open(UNPROCESSED, "w", encoding="utf-8") as f:
        f.write("\n".join(remaining) + ("\n" if remaining else ""))
    return kw

def move_from_processing_to(target_file, keyword):
    # remove keyword from processing if present
    if os.path.exists(PROCESSING):
        with open(PROCESSING, "r", encoding="utf-8") as f:
            lines = [l.rstrip("\n") for l in f.readlines()]
        lines = [l for l in lines if l.strip() != keyword]
        with open(PROCESSING, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + ("\n" if lines else ""))
    append_line(target_file, keyword)

def fetch_keyword(kw):
    pytrends = TrendReq(hl="en-US", tz=TZ)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            pytrends.build_payload([kw], cat=0, timeframe=TIMEFRAME, geo=GEO, gprop="")
            df = pytrends.interest_over_time()
            if df is None or df.empty:
                return None, "empty"
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])
            # rename column to safe name
            safe_name = kw.replace(" ", "_").replace("/", "_")
            df = df.rename(columns={kw: safe_name})
            return df, "ok"
        except Exception as e:
            # log and retry with exponential backoff
            err = str(e)
            # If last attempt, return failure
            if attempt == MAX_RETRIES:
                return None, f"error_final: {err}"
            # Backoff
            backoff = INITIAL_BACKOFF * (2 ** (attempt - 1))
            log(f"Attempt {attempt} failed for '{kw}': {err}. Backing off {backoff}s")
            time.sleep(backoff)
    return None, "unknown"

def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(msg)
    with open(RUN_LOG, "a", encoding="utf-8") as f:
        f.write(f"{ts} - {msg}\n")

def main():
    try:
        kw = pop_first_unprocessed()
        if not kw:
            log("No unprocessed keywords remaining. Exiting.")
            return
        # mark processing
        append_line(PROCESSING, kw)
        log(f"Selected keyword: {kw}")

        # fetch
        df, status = fetch_keyword(kw)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M")
        if df is not None and status == "ok":
            safe_kw = kw.replace(" ", "_").replace("/", "_")
            outfile = os.path.join(DATA_DIR, f"{safe_kw}_{ts}.csv")
            df.to_csv(outfile)
            log(f"Saved file: {outfile}")
            move_from_processing_to(PROCESSED, kw)
            # add run summary
            log(f"SUCCESS: {kw}")
        else:
            # failure or empty
            reason = status
            log(f"FAILED for {kw}: {reason}")
            move_from_processing_to(FAILED, kw)
    except Exception as e:
        log("Unexpected exception: " + repr(e))
        traceback.print_exc()
    finally:
        # finishing
        log("Run finished.\n")

if __name__ == "__main__":
    main()

