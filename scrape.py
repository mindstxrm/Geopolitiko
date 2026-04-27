#!/usr/bin/env python3
"""CLI to fetch news from all configured sources and store in the database."""
import sys
from pathlib import Path
from config import DATABASE_PATH
from app.scrapers.rss_scraper import scrape_all_sources
from app.models import init_db, get_articles_total_count


def main():
    Path(DATABASE_PATH).parent.mkdir(parents=True, exist_ok=True)
    print("Scraping geopolitical news sources...")
    print(f"Database: {DATABASE_PATH}")
    results = scrape_all_sources(DATABASE_PATH)
    total = 0
    for name, count in results.items():
        print(f"  {name}: {count} articles")
        total += count
    print(f"Total: {total} articles processed this run.")
    init_db(DATABASE_PATH)
    in_db = get_articles_total_count()
    print(f"Database now has {in_db} articles total.")
    if total > 0 and in_db == 0:
        print("Warning: no rows in DB. Check that the app uses the same DATABASE_PATH (project root/config.py).")
    return 0 if total >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
