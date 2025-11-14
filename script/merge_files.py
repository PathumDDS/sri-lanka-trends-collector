# script/merge_files.py
import os, glob, pandas as pd
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT, "data", "raw")
MERGED_DIR = os.path.join(ROOT, "data", "merged")
KEYDIR = os.path.join(ROOT, "keywords")
PROCED = os.path.join(KEYDIR, "processed.txt")
os.makedirs(MERGED_DIR, exist_ok=True)

def read_lines(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip()]

def latest_file_per_keyword():
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    latest = {}
    for f in files:
        name = os.path.basename(f)
        parts = name.rsplit("_", 2)
        if len(parts) >= 2:
            kw = parts[0]
        else:
            kw = name.replace(".csv", "")
        mtime = os.path.getmtime(f)
        if kw not in latest or mtime > latest[kw][0]:
            latest[kw] = (mtime, f)
    return {k: v[1] for k, v in latest.items()}

def main():
    processed = read_lines(PROCED)
    if not processed:
        print("No processed keywords. Removing merged dataset if exists.")
        mainf = os.path.join(MERGED_DIR, "main_dataset.csv")
        if os.path.exists(mainf):
            os.remove(mainf)
            print("Removed merged dataset.")
        return
    files_map = latest_file_per_keyword()
    dfs = []
    for pk in processed:
        safe_pk = pk.replace(" ", "_").replace("/", "_")
        fpath = files_map.get(safe_pk)
        if fpath:
            try:
                df = pd.read_csv(fpath, index_col=0, parse_dates=True)
                df.columns = [safe_pk]
                dfs.append(df)
            except Exception as e:
                print("Skipping", fpath, e)
        else:
            print("No raw file found for processed keyword:", pk)
    if not dfs:
        print("No dfs to merge.")
        return
    merged = pd.concat(dfs, axis=1)
    merged.sort_index(inplace=True)
    out = os.path.join(MERGED_DIR, "main_dataset.csv")
    merged.to_csv(out)
    print("Merged saved to:", out)

if __name__ == "__main__":
    main()
