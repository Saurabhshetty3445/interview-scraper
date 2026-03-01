"""
jobs/scheduler.py
- On startup: full historical scrape (new + old posts)
- Every 2 hours: new posts only (fast)
- Every Sunday: full historical re-scrape
"""
import schedule
import time
import threading
from datetime import datetime

from utils.logger import log


def run_all_scrapers(fetch_old: bool = False):
    log.info(f"🚀 Scrape starting (fetch_old={fetch_old}) at {datetime.utcnow().isoformat()}")
    try:
        from scrapers.leetcode_scraper import run_leetcode_scraper
        run_leetcode_scraper(fetch_old=fetch_old)
    except Exception as e:
        log.error(f"LeetCode scraper error: {e}")
    try:
        from scrapers.reddit_scraper import run_reddit_scraper
        run_reddit_scraper(fetch_old=fetch_old)
    except Exception as e:
        log.error(f"Reddit scraper error: {e}")
    log.info("✅ Scrape complete")


def start_scheduler(run_immediately: bool = True):
    log.info("Starting scheduler")

    if run_immediately:
        # First run: fetch everything including historical posts
        thread = threading.Thread(
            target=run_all_scrapers,
            kwargs={"fetch_old": True},
            daemon=True
        )
        thread.start()

    # Every 2 hours: fetch new posts only (fast)
    schedule.every(2).hours.do(run_all_scrapers, fetch_old=False)

    # Every Sunday at 03:00 UTC: full historical re-scrape
    schedule.every().sunday.at("03:00").do(run_all_scrapers, fetch_old=True)

    log.info("Scheduler: new posts every 2h, full history every Sunday")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    start_scheduler(run_immediately=True)
