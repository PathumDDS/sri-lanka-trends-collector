
# script/merge_files.py
# Merge per-keyword trend CSVs into a single wide CSV (date x keyword columns)
import os, glob, pandas as pd
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT, "data", "raw")
MERGED_DIR = os.path.join(ROOT, "data", "merged")
os.makedirs(MERGED_DIR, exist_ok=True)

def latest_file_per_keyword():
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    latest = {}
    for f in files:
        name = os.path.basename(f)
        # expected: keyword_YYYYMMDD_HHMM.csv
        parts = name.rsplit("_", 2)
        if len(parts) >= 2:
            kw = "_".join(parts[:-2]) if len(parts) > 2 else parts[0].rsplit("_",1)[0]
        else:
            kw = name.split(".csv")[0]
        # naive: use file modification time
        mtime = os.path.getmtime(f)
        if kw not in latest or mtime > latest[kw][0]:
            latest[kw] = (mtime, f)
    return {k: v[1] for k, v in latest.items()}

def main():
    files = latest_file_per_keyword()
    if not files:
        print("No files to merge")
        return
    dfs = []
    for kw, fpath in files.items():
        try:
            df = pd.read_csv(fpath, index_col=0, parse_dates=True)
            # If single-key col, rename to safe col (filename-based)
            col = df.columns[0]
            safe_col = os.path.basename(fpath).split(".csv")[0].rsplit("_",1)[0]
            df.columns = [safe_col]
            dfs.append(df)
        except Exception as e:
            print("Skipping", fpath, e)
    if not dfs:
        print("No dataframes to merge")
        return
    merged = pd.concat(dfs, axis=1)
    merged.sort_index(inplace=True)
    out = os.path.join(MERGED_DIR, f"trends_merged_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv")
    merged.to_csv(out)
    print("Merged saved to:", out)

if __name__ == "__main__":
    main()
