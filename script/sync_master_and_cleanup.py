# script/sync_master_and_cleanup.py
# Sync master keyword file with status files and enforce deletions as specified.
# WARNING: This script WILL delete raw CSV files for keywords removed from processed.txt (no backup).

import os, glob, sys
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KEYDIR = os.path.join(ROOT, "keywords_monthly")
DATA_RAW = os.path.join(ROOT, "data_monthly", "raw")
DATA_MERGED_DIR = os.path.join(ROOT, "data_monthly", "merged")

MASTER = os.path.join(KEYDIR, "all_keywords.txt")
UNPRO = os.path.join(KEYDIR, "unprocessed.txt")
PROCING = os.path.join(KEYDIR, "processing.txt")
PROCED = os.path.join(KEYDIR, "processed.txt")
FAILED = os.path.join(KEYDIR, "failed.txt")

os.makedirs(DATA_RAW, exist_ok=True)
os.makedirs(DATA_MERGED_DIR, exist_ok=True)

def read_set(path):
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        items = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    return set(items)

def write_set(path, s):
    with open(path, "w", encoding="utf-8") as f:
        for kw in sorted(s):
            f.write(kw + "\n")

def delete_raw_files_for_keyword(keyword):
    safe_kw = keyword.replace(" ", "_").replace("/", "_")
    pattern = os.path.join(DATA_RAW, f"{safe_kw}_*.csv")
    files = glob.glob(pattern)
    deleted = []
    for p in files:
        try:
            os.remove(p)
            deleted.append(p)
        except Exception as e:
            print(f"ERROR deleting file {p}: {e}")
    return deleted

def rebuild_merged_from_processed():
    processed = read_set(PROCED)
    # remove existing merged file(s) and produce a stable main_dataset.csv from processed
    out_file = os.path.join(DATA_MERGED_DIR, "main_dataset.csv")
    # collect latest file per processed keyword
    files = glob.glob(os.path.join(DATA_RAW, "*.csv"))
    latest = {}
    for f in files:
        name = os.path.basename(f)
        # name format: safe_kw_YYYYMMDD_HHMM.csv; safe_kw may contain underscores
        parts = name.rsplit("_", 2)
        if len(parts) >= 2:
            safe_kw = parts[0]
        else:
            safe_kw = name.replace(".csv", "")
        mtime = os.path.getmtime(f)
        if safe_kw not in latest or mtime > latest[safe_kw][0]:
            latest[safe_kw] = (mtime, f)

    # build dfs for processed keywords only
    dfs = []
    for pk in sorted(processed):
        safe_pk = pk.replace(" ", "_").replace("/", "_")
        if safe_pk in latest:
            fpath = latest[safe_pk][1]
            try:
                df = pd.read_csv(fpath, index_col=0, parse_dates=True)
                colname = safe_pk
                df.columns = [colname]
                dfs.append(df)
            except Exception as e:
                print(f"Skipping {fpath} during merge: {e}")
        else:
            print(f"No raw file found for processed keyword: {pk} (expected safe name {safe_pk})")

    if not dfs:
        # remove existing merged if exists
        if os.path.exists(out_file):
            os.remove(out_file)
            print("Removed existing merged dataset (no processed keywords).")
        else:
            print("No processed keywords -> no merged dataset.")
        return

    merged = pd.concat(dfs, axis=1)
    merged.sort_index(inplace=True)
    merged.to_csv(out_file)
    print(f"Rebuilt merged dataset -> {out_file}")

def main():
    if not os.path.exists(MASTER):
        print(f"Master file not found: {MASTER}. Create it and add keywords one per line.")
        return

    master = read_set(MASTER)
    unpro = read_set(UNPRO)
    processing = read_set(PROCING)
    processed = read_set(PROCED)
    failed = read_set(FAILED)

    print("=== Sync report start ===")
    print(f"Master count: {len(master)}")
    print(f"unprocessed: {len(unpro)}, processing: {len(processing)}, processed: {len(processed)}, failed: {len(failed)}")

    # 1) Add new keywords from master -> unprocessed (only if not present elsewhere)
    added = []
    for kw in sorted(master):
        if kw not in unpro and kw not in processing and kw not in processed and kw not in failed:
            unpro.add(kw)
            added.append(kw)
    if added:
        print("Added to unprocessed (new in master):")
        for k in added:
            print(" +", k)

    # 2) Handle removals from master
    removed_from_unpro = []
    removed_from_failed = []
    removed_from_processed = []
    skipped_processing = []

    # Remove from unprocessed if not in master
    for kw in sorted(list(unpro)):
        if kw not in master:
            unpro.remove(kw)
            removed_from_unpro.append(kw)

    # Remove from failed if not in master
    for kw in sorted(list(failed)):
        if kw not in master:
            failed.remove(kw)
            removed_from_failed.append(kw)

    # For processed: if removed from master and NOT in processing -> delete raw CSVs and remove from processed
    for kw in sorted(list(processed)):
        if kw not in master:
            if kw in processing:
                skipped_processing.append(kw)
            else:
                # delete raw csv files for that keyword
                deleted_files = delete_raw_files_for_keyword(kw)
                print(f"Deleted {len(deleted_files)} raw files for removed processed keyword: {kw}")
                processed.remove(kw)
                removed_from_processed.append((kw, deleted_files))

    # write back status files
    write_set(UNPRO, unpro)
    write_set(FAILED, failed)
    write_set(PROCED, processed)

    # print summary
    if removed_from_unpro:
        print("Removed from unprocessed (deleted from master):")
        for k in removed_from_unpro:
            print(" -", k)
    if removed_from_failed:
        print("Removed from failed (deleted from master):")
        for k in removed_from_failed:
            print(" -", k)
    if removed_from_processed:
        print("Removed from processed (deleted from master) and deleted files:")
        for k, files in removed_from_processed:
            print(" *", k, "-> deleted files:", len(files))
            for p in files:
                print("    >", p)
    if skipped_processing:
        print("Skipped deletion because in processing (will keep until processing finishes):")
        for k in skipped_processing:
            print(" !", k)

    # rebuild merged dataset (based on updated processed.txt)
    rebuild_merged_from_processed()

    print("=== Sync report end ===")

if __name__ == "__main__":
    main()
