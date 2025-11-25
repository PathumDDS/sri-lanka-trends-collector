# script_weekly/merge_weekly.py
# Merge per-topic weekly CSVs into a single wide CSV (date x topic columns)
# Uses keywords_weekly/processed.txt to decide which topics to include.
# Handles either data_weekly/raw_weekly/ or data/raw_weekly/ (backwards compatibility).

import os, glob, pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KEYDIR = os.path.join(ROOT, "keywords_weekly")
PROCED = os.path.join(KEYDIR, "processed.txt")

RAW_WEEKLY = os.path.join(ROOT, "data_weekly", "raw_weekly")
MERGED_DIR = os.path.join(ROOT, "data_weekly", "merged")

os.makedirs(MERGED_DIR, exist_ok=True)

def read_lines(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip()]

def find_raw_weekly_dir():
    return RAW_WEEKLY   # always use the weekly folder

def main():
    raw_dir = find_raw_weekly_dir()
    processed = read_lines(PROCED)
    if not processed:
        print("No processed keywords. Removing merged dataset if exists.")
        out = os.path.join(MERGED_DIR, "weekly_dataset.csv")
        if os.path.exists(out):
            os.remove(out)
            print("Removed merged weekly dataset.")
        return

    dfs = []
    missing = []
    for pk in sorted(processed):
        safe_pk = pk.replace(" ", "_").replace("/", "_")
        # Search for files matching safe_pk in raw_dir (take latest file)
        pattern = os.path.join(raw_dir, f"{safe_pk}_*.csv")
        files = glob.glob(pattern)
        if not files:
            missing.append(pk)
            print("No raw weekly file found for processed keyword:", pk)
            continue
        # Choose most recent file by mtime
        latest = max(files, key=lambda p: os.path.getmtime(p))
        try:
            df = pd.read_csv(latest, index_col=0, parse_dates=True)
            df.columns = [safe_pk]
            dfs.append(df)
        except Exception as e:
            print("Skipping", latest, "due to error:", e)

    if not dfs:
        print("No dataframes to merge (all missing or unreadable).")
        return

    merged = pd.concat(dfs, axis=1)
    merged.sort_index(inplace=True)

    out = os.path.join(MERGED_DIR, "weekly_dataset.csv")
    merged.to_csv(out)
    print("Weekly merged saved to:", out)

    # Optionally also put a copy in legacy merged folder if it exists or is desired
    # This keeps parity with monthly conventions if some tools expect data/merged
    if os.path.exists(ALT_MERGED_DIR) or True:
        os.makedirs(ALT_MERGED_DIR, exist_ok=True)
        alt_out = os.path.join(ALT_MERGED_DIR, "weekly_dataset.csv")
        merged.to_csv(alt_out)
        print("Also wrote legacy merged copy to:", alt_out)

if __name__ == "__main__":
    main()
