# fetch_trends.py
# Simple Google Trends collector for GitHub Actions
# - reads keywords.csv
# - each run fetches K keywords (one-by-one requests)
# - saves CSVs under data/
# - designed to run inside GitHub Actions

import os, time, csv
from datetime import datetime
import pandas as pd

from pytrends.request import TrendReq

# CONFIG
GEO = "LK"  # Sri Lanka
START_DATE = "2008-01-01"
SLEEP_BETWEEN_KEYWORDS = 60  # seconds, set >= 60 to avoid 429
KEYWORDS_PER_RUN = 4         # how many keywords this run should fetch
TIMEFRAME = f"{START_DATE} {datetime.utcnow().strftime('%Y-%m-%d')}"

# helper: load keywords from keywords.csv (ignore lines starting with '#')
def load_keywords(path="keywords.csv"):
    kws = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            kw = r.get('keyword', '').strip()
            if kw and not kw.startswith('#'):
                kws.append(kw)
    return kws

def select_block(kws, k_per_run):
    # Choose a deterministic block based on current UTC time
    # Each 4-hour window corresponds to an increasing block index
    # but we will use the current 4-hour slot number:
    slot = int(time.time()) // (4 * 3600)   # increments every 4 hours
    start = (slot * k_per_run) % len(kws)
    block = []
    for i in range(k_per_run):
        block.append(kws[(start + i) % len(kws)])
    return block

def safe_fetch_keyword(pytrends, kw):
    try:
        pytrends.build_payload([kw], cat=0, timeframe=TIMEFRAME, geo=GEO, gprop="")
        df = pytrends.interest_over_time()
        if df is None or df.empty:
            print(f"Empty result for {kw}")
            return None
        # drop isPartial if present
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])
        return df.rename(columns={kw: kw.replace(" ", "_")})
    except Exception as e:
        print(f"Error fetching {kw}: {e}")
        return None

def main():
    if not os.path.exists("keywords.csv"):
        print("keywords.csv not found. Please add it to the repo root.")
        return

    kws = load_keywords("keywords.csv")
    if not kws:
        print("No keywords found in keywords.csv")
        return

    block = select_block(kws, KEYWORDS_PER_RUN)
    print("This run will fetch keywords:", block)

    pytrends = TrendReq(hl='en-US', tz=330)  # Sri Lanka tz +5:30

    os.makedirs("data", exist_ok=True)
    fetched_files = []
    for kw in block:
        print(f"Fetching keyword: {kw}")
        df = safe_fetch_keyword(pytrends, kw)
        if df is not None:
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M")
            safe_kw = kw.replace(" ", "_").replace("/", "_")
            outpath = os.path.join("data", f"{safe_kw}_{ts}.csv")
            df.to_csv(outpath)
            fetched_files.append(outpath)
            print("Saved:", outpath)
        else:
            print("No data for", kw)
        print(f"Sleeping {SLEEP_BETWEEN_KEYWORDS} seconds to avoid rate limits...")
        time.sleep(SLEEP_BETWEEN_KEYWORDS)

    # Print files created (Action will commit them)
    print("Files produced this run:", fetched_files)

if __name__ == "__main__":
    main()
