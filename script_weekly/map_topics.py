# script_weekly/map_topics.py

import os
import csv
from pytrends.request import TrendReq

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KEYWORD_DIR = os.path.join(ROOT, "keywords_weekly")
RAW_FILE = os.path.join(KEYWORD_DIR, "raw_oecd_keywords.txt")
MASTER_FILE = os.path.join(KEYWORD_DIR, "topics_master.txt")
MAPPING_LOGS = os.path.join(KEYWORD_DIR, "mapping_logs")
MAPPING_CSV = os.path.join(MAPPING_LOGS, "topic_mapping.csv")
REJECTED_FILE = os.path.join(MAPPING_LOGS, "topic_rejected.txt")

os.makedirs(MAPPING_LOGS, exist_ok=True)

pytrends = TrendReq(hl="en-US", tz=330)  # Sri Lanka timezone if needed

def read_raw():
    with open(RAW_FILE, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    # dedupe
    return sorted(set(lines))

def map_keyword(term):
    """ Try to map a raw term to a Google Trends Topic via suggestions. """
    try:
        suggestions = pytrends.suggestions(term)
    except Exception as e:
        return None, None, str(e)
    for s in suggestions:
        # Each suggestion is a dict like {'mid': '/m/012345', 'title': 'Some Topic', ...}
        if 'mid' in s and s['mid'] and 'title' in s:
            return s['mid'], s['title'], None
    return None, None, "no_topic_match"

def main():
    raw = read_raw()
    mapped = []
    rejected = []

    with open(MAPPING_CSV, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["raw_term", "topic_id", "topic_title", "error"])
        for term in raw:
            topic_id, title, error = map_keyword(term)
            if topic_id:
                mapped.append((term, topic_id, title))
                writer.writerow([term, topic_id, title, ""])
            else:
                rejected.append((term, error))
                writer.writerow([term, "", "", error or "no match"])

    # Write master file (just topic ids)
    with open(MASTER_FILE, "w", encoding="utf-8") as f:
        for term, topic_id, title in mapped:
            f.write(f"{topic_id}\t{title}\n")

    # Write rejections
    with open(REJECTED_FILE, "w", encoding="utf-8") as f:
        for term, error in rejected:
            f.write(f"{term}\t{error}\n")

    print(f"Mapped {len(mapped)} topics, rejected {len(rejected)} items.")
    print(f"See mapping log: {MAPPING_CSV}")
    print(f"See rejected list: {REJECTED_FILE}")
    print(f"Master topics file: {MASTER_FILE}")

if __name__ == "__main__":
    main()
