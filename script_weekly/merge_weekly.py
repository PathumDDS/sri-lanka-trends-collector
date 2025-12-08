# script_weekly/merge_weekly.py
# Merge processed weekly CSVs into a single wide CSV (date x keyword columns)

import os
import glob
import pandas as pd

# ----------------------------
# PATH SETUP
# ----------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KEYDIR = os.path.join(ROOT, "keywords_weekly")
PROCED = os.path.join(KEYDIR, "processed.txt")

RAW_WEEKLY = os.path.join(ROOT, "data_weekly", "raw_weekly")
MERGED_DIR = os.path.join(ROOT, "data_weekly", "merged")
os.makedirs(MERGED_DIR, exist_ok=True)

# ----------------------------
# Helper Functions
# ----------------------------
def read_lines(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def safe_kw(kw):
    return kw.replace(" ", "_").replace("/", "_")

# ----------------------------
# Main Merge Logic
# ----------------------------
def main():
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

    for kw in sorted(processed):
        sk = safe_kw(kw)
        pattern = os.path.join(RAW_WEEKLY, f"{sk}_*.csv")
        files = glob.glob(pattern)

        if not files:
            missing.append(kw)
            print("No stitched weekly file found for keyword:", kw)
            continue

        # Use latest stitched file
        latest_file = max(files, key=lambda p: os.path.getmtime(p))

        try:
            df = pd.read_csv(latest_file, index_col=0, parse_dates=True)

            # ----------------------------
            # 🔥 Critical Fix: Drop duplicates
            # ----------------------------
            df = df[~df.index.duplicated(keep="last")]

            df.columns = [sk]
            dfs.append(df)

        except Exception as e:
            print("Skipping", latest_file, "due to error:", e)

    if not dfs:
        print("No dataframes to merge. All missing or unreadable.")
        return

    # ----------------------------
    # Merge all keywords into one wide dataset
    # ----------------------------
    merged = pd.concat(dfs, axis=1)

    # ----------------------------
    # 🔥 Global deduplication (safety net)
    # ----------------------------
    merged = merged[~merged.index.duplicated(keep="last")]

    # Sort index (should already be weekly Sundays)
    merged.sort_index(inplace=True)

    # ----------------------------
    # Save result
    # ----------------------------
    out_path = os.path.join(MERGED_DIR, "weekly_dataset.csv")
    merged.to_csv(out_path)
    print("Weekly merged dataset saved to:", out_path)

    if missing:
        print("\nMissing stitched files for:", missing)

if __name__ == "__main__":
    main()
