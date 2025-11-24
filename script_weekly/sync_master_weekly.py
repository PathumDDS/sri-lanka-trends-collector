# Weekly keyword sync & cleanup
# Mirrors the behaviour of monthly sync system (add/remove keywords, delete old raw files)
# but adapted for weekly Google Trends data structure.

import os, glob
import pandas as pd

# ----------------------------
# PATH SETUP
# ----------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KEYDIR = os.path.join(ROOT, "keywords_weekly")

MASTER = os.path.join(KEYDIR, "topics_master.txt")
UNPRO = os.path.join(KEYDIR, "unprocessed.txt")
PROCING = os.path.join(KEYDIR, "processing.txt")
PROCED = os.path.join(KEYDIR, "processed.txt")
FAILED = os.path.join(KEYDIR, "failed.txt")

RAW_WINDOWS = os.path.join(ROOT, "data_weekly", "raw_windows")
RAW_WEEKLY = os.path.join(ROOT, "data_weekly", "raw_weekly")
MERGED_DIR = os.path.join(ROOT, "data_weekly", "merged")

os.makedirs(RAW_WINDOWS, exist_ok=True)
os.makedirs(RAW_WEEKLY, exist_ok=True)
os.makedirs(MERGED_DIR, exist_ok=True)


# ----------------------------
# Helper Functions
# ----------------------------

def read_set(path):
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}

def write_set(path, s):
    with open(path, "w", encoding="utf-8") as f:
        for kw in sorted(s):
            f.write(kw + "\n")


def safe_kw(kw):
    return kw.replace(" ", "_").replace("/", "_")


def delete_raw_files_for_keyword(keyword):
    sk = safe_kw(keyword)

    # delete weekly_windows folder: data_weekly/raw_windows/<kw>/
    folder = os.path.join(RAW_WINDOWS, sk)
    deleted = []

    if os.path.exists(folder):
        for root, dirs, files in os.walk(folder, topdown=False):
            for name in files:
                p = os.path.join(root, name)
                os.remove(p)
                deleted.append(p)
        os.rmdir(folder)

    # delete weekly file: data_weekly/raw_weekly/<kw>_weekly.csv
    weekly_file = os.path.join(RAW_WEEKLY, f"{sk}_weekly.csv")
    if os.path.exists(weekly_file):
        os.remove(weekly_file)
        deleted.append(weekly_file)

    return deleted


# ----------------------------
# Merge Dataset Reconstruction
# ----------------------------

def rebuild_weekly_merged():
    out = os.path.join(MERGED_DIR, "weekly_dataset.csv")
    processed = read_set(PROCED)

    dfs = []

    for kw in sorted(processed):
        sk = safe_kw(kw)
        f = os.path.join(RAW_WEEKLY, f"{sk}_weekly.csv")
        if os.path.exists(f):
            try:
                df = pd.read_csv(f, index_col=0, parse_dates=True)
                df.columns = [sk]
                dfs.append(df)
            except Exception as e:
                print("Skipping", f, e)
        else:
            print("Missing weekly file for:", kw)

    if not dfs:
        if os.path.exists(out):
            os.remove(out)
            print("Removed weekly_dataset.csv (no processed keywords).")
        return

    merged = pd.concat(dfs, axis=1)
    merged.sort_index(inplace=True)
    merged.to_csv(out)
    print("Rebuilt weekly merged dataset ->", out)


# ----------------------------
# MAIN
# ----------------------------

def main():
    if not os.path.exists(MASTER):
        print("topics_master.txt not found — cannot sync.")
        return

    master = read_set(MASTER)
    unpro = read_set(UNPRO)
    processing = read_set(PROCING)
    processed = read_set(PROCED)
    failed = read_set(FAILED)

    print("=== Weekly Sync Report ===")
    print(f"Master: {len(master)} | unprocessed: {len(unpro)} | processing: {len(processing)} | processed: {len(processed)} | failed: {len(failed)}")

    # ----------------------------------
    # 1) Add new items from master → unprocessed
    # ----------------------------------
    added = []
    for kw in master:
        if kw not in unpro and kw not in processing and kw not in processed and kw not in failed:
            unpro.add(kw)
            added.append(kw)

    if added:
        print("Added to unprocessed:", added)

    # ----------------------------------
    # 2) Remove keywords not in master
    # ----------------------------------

    removed_unpro = [kw for kw in unpro if kw not in master]
    for kw in removed_unpro:
        unpro.remove(kw)

    removed_failed = [kw for kw in failed if kw not in master]
    for kw in removed_failed:
        failed.remove(kw)

    removed_processed = []
    for kw in list(processed):
        if kw not in master:
            if kw in processing:
                print("Skipping removal (still in processing):", kw)
                continue
            deleted_files = delete_raw_files_for_keyword(kw)
            processed.remove(kw)
            removed_processed.append((kw, deleted_files))

    # ----------------------------------
    # Save results
    # ----------------------------------

    write_set(UNPRO, unpro)
    write_set(FAILED, failed)
    write_set(PROCED, processed)

    if removed_unpro:
        print("Removed from unprocessed:", removed_unpro)

    if removed_failed:
        print("Removed from failed:", removed_failed)

    if removed_processed:
        print("Removed from processed and deleted raw files:")
        for kw, files in removed_processed:
            print(" *", kw)
            for p in files:
                print("    >", p)

    # ----------------------------------
    # Rebuild merged weekly dataset
    # ----------------------------------
    rebuild_weekly_merged()

    print("=== Weekly Sync Complete ===")


if __name__ == "__main__":
    main()
