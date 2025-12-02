# script_weekly/sync_master_weekly.py
# Weekly keyword sync & cleanup (master_keywords.txt)
# Prepares unprocessed/processing/processed/failed files for weekly fetching

import os

# ----------------------------
# PATH SETUP
# ----------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KEYDIR = os.path.join(ROOT, "keywords_weekly")

MASTER = os.path.join(KEYDIR, "master_keywords.txt")
UNPRO = os.path.join(KEYDIR, "unprocessed.txt")
print("DEBUG: unprocessed.txt path →", UNPRO)
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
    """Write set to file, ensuring file exists and flush to disk"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for kw in sorted(s):
            f.write(kw + "\n")
        f.flush()
        os.fsync(f.fileno())
    # DEBUG: confirm file contents
    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
    print(f"DEBUG: {path} written, {len(lines)} lines")

def safe_kw(kw):
    return kw.replace(" ", "_").replace("/", "_")

def delete_raw_files_for_keyword(keyword):
    sk = safe_kw(keyword)
    deleted = []

    # Delete raw_windows folder
    folder = os.path.join(RAW_WINDOWS, sk)
    if os.path.exists(folder):
        for root, dirs, files in os.walk(folder, topdown=False):
            for name in files:
                p = os.path.join(root, name)
                os.remove(p)
                deleted.append(p)
        os.rmdir(folder)

    # Delete stitched weekly file
    weekly_file = os.path.join(RAW_WEEKLY, f"{sk}_weekly.csv")
    if os.path.exists(weekly_file):
        os.remove(weekly_file)
        deleted.append(weekly_file)

    return deleted

# ----------------------------
# MAIN SYNC LOGIC
# ----------------------------

def main():
    if not os.path.exists(MASTER):
        print("master_keywords.txt not found — cannot sync.")
        return

    master = read_set(MASTER)
    unpro = read_set(UNPRO)
    processing = read_set(PROCING)
    processed = read_set(PROCED)
    failed = read_set(FAILED)

    print("=== Weekly Sync Report ===")
    print(f"Master: {len(master)} | unprocessed: {len(unpro)} | processing: {len(processing)} | processed: {len(processed)} | failed: {len(failed)}")

    # ----------------------------------
    # 1) Add new keywords from master → unprocessed
    # ----------------------------------
    added = []
    for kw in master:
        if kw not in unpro and kw not in processing and kw not in processed and kw not in failed:
            unpro.add(kw)
            added.append(kw)
    if added:
        print("Added to unprocessed:", added)

    # ----------------------------------
    # 2) Remove keywords no longer in master
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
    # 3) Save all sets at the end with flush
    # ----------------------------------
    write_set(UNPRO, unpro)
    write_set(PROCING, processing)
    write_set(PROCED, processed)
    write_set(FAILED, failed)

    if removed_unpro:
        print("Removed from unprocessed:", removed_unpro)
    if removed_failed:
        print("Removed from failed:", removed_failed)
    if removed_processed:
        print("Removed from processed and deleted raw files:")
        for kw, files in removed_processed:
            print(" *", kw)
            for f in files:
                print("    >", f)

    print("=== Weekly Sync Complete ===")

if __name__ == "__main__":
    main()
