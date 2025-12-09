# script_weekly/stitch_weekly_keyword.py
import os, pandas as pd
from datetime import timedelta
from pathlib import Path

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_WINDOWS = os.path.join(ROOT, "data_weekly", "raw_windows")
RAW_WEEKLY = os.path.join(ROOT, "data_weekly", "raw_weekly")

def stitch_keyword(safe_kw):
    win_dir = os.path.join(RAW_WINDOWS, safe_kw)
    files = sorted(Path(win_dir).glob("*.csv"))
    if not files:
        return

    collected = []
    for f in files:
        df = pd.read_csv(f, index_col=0, parse_dates=True)
        start = df.index[0]
        end = df.index[-1]
        collected.append((df, start, end))

    # stitching logic same as before
    stitched = collected[0][0].copy().sort_index()
    for i in range(1, len(collected)):
        prev_df, prev_s, prev_e = collected[i-1]
        df, s, e = collected[i]
        overlap_start = max(prev_s, s)
        overlap_end = min(prev_e, e)
        overlap_old = stitched.loc[overlap_start:overlap_end]
        overlap_new = df.loc[overlap_start:overlap_end]
        try:
            cond = len(overlap_old.dropna())>0 and len(overlap_new.dropna())>0 and overlap_new.median().iloc[0]>0
        except:
            cond = False
        df_scaled = df * (overlap_old.median().iloc[0]/overlap_new.median().iloc[0]) if cond else df
        tail = df_scaled.loc[prev_e + timedelta(days=1):]
        stitched = pd.concat([stitched, tail])
        stitched = stitched[~stitched.index.duplicated(keep="last")]

    stitched = stitched.sort_index()
    stitched.to_csv(os.path.join(RAW_WEEKLY, f"{safe_kw}_weekly.csv"))

# Example usage for all keywords
if __name__ == "__main__":
    keywords_dir = os.path.join(ROOT, "keywords_weekly")
    processed_file = os.path.join(keywords_dir, "processed.txt")
    with open(processed_file, "r") as f:
        for kw in f.read().splitlines():
            safe_kw = kw.replace(" ", "_")
            stitch_keyword(safe_kw)
