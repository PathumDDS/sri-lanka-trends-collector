# script/plot_weekly_preview.py
# Recreate preview PNG for a given keyword from raw_windows and stitched weekly CSV.
# Usage: python script/plot_weekly_preview.py "keyword phrase"

import os, sys
from datetime import datetime
import pandas as pd
from glob import glob

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_WINDOWS_DIR = os.path.join(ROOT, "data", "raw_windows")
RAW_WEEKLY_DIR = os.path.join(ROOT, "data", "raw_weekly")
PREVIEW_DIR = os.path.join(ROOT, "data", "preview")
os.makedirs(PREVIEW_DIR, exist_ok=True)

def safe_name(kw):
    return kw.replace(" ", "_").replace("/", "_")

def main():
    if len(sys.argv) < 2:
        print("Usage: python script/plot_weekly_preview.py \"keyword phrase\"")
        sys.exit(1)
    kw = sys.argv[1].strip()
    safe = safe_name(kw)
    # gather window files
    pattern = os.path.join(RAW_WINDOWS_DIR, f"{safe}_*.csv")
    files = sorted(glob(pattern))
    windows = []
    for f in files:
        try:
            df = pd.read_csv(f, index_col=0, parse_dates=True)
            # extract dates from filename for title (optional)
            windows.append(df)
        except:
            continue
    stitched_file = os.path.join(RAW_WEEKLY_DIR, f"{safe}_weekly.csv")
    if not windows or not os.path.exists(stitched_file):
        print("No windows or stitched file found for", kw)
        return
    stitched = pd.read_csv(stitched_file, index_col=0, parse_dates=True)

    # plotting
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        plt.figure(figsize=(10,5))
        for df in windows:
            plt.plot(df.index, df.iloc[:,0], alpha=0.25)
        plt.plot(stitched.index, stitched.iloc[:,0], linewidth=1.2)
        plt.title(f"Weekly stitched preview: {kw}")
        plt.tight_layout()
        out = os.path.join(PREVIEW_DIR, f"{safe}_weekly_preview.png")
        plt.savefig(out, dpi=150)
        plt.close()
        print("Saved preview:", out)
    except Exception as e:
        print("Plot failed:", e)

if __name__ == "__main__":
    main()
