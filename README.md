# Sri Lanka Google Trends Collector

This repo automatically collects Google Trends weekly series for keywords listed in `keywords.csv`.
- The GitHub Action runs every 4 hours and fetches `KEYWORDS_PER_RUN` keywords per run.
- Each keyword is fetched alone (single-keyword payload) to avoid cross-normalization issues.
- Fetched CSVs are saved under `data/` and committed to the repository.

Edit `keywords.csv` to add or remove keywords.
Edit `fetch_trends.py` to change settings (KEYWORDS_PER_RUN, SLEEP_BETWEEN_KEYWORDS, START_DATE).
