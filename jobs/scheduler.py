"""
jobs/scheduler.py - Background job scheduler for Railway deployment
Runs scrapers on a daily schedule, with immediate first run option
"""
import schedule
import time
import threading
from datetime import datetime

from utils.logger import log
from scrapers.reddit_scraper import run_reddit_scraper
from scrapers.leetcode_scraper import run_leetcode_scraper


def run_all_scrapers():
    """Run all scrapers in sequence."""
    log.info(f"🚀 Scheduled scrape starting at {datetime.utcnow().isoformat()}")
    try:
        run_leetcode_scraper()
    except Exception as e:
        log.error(f"LeetCode scraper failed: {e}")
    try:
        run_reddit_scraper()
    except Exception as e:
        log.error(f"Reddit scraper failed: {e}")
    log.info("✅ Scheduled scrape complete")


def start_scheduler(run_immediately: bool = True):
    """Start the scheduler. Optionally run once immediately on startup."""
    log.info("Starting interview scraper scheduler")

    if run_immediately:
        log.info("Running initial scrape immediately...")
        thread = threading.Thread(target=run_all_scrapers, daemon=True)
        thread.start()

    # Schedule daily at 06:00 UTC
    schedule.every().day.at("06:00").do(run_all_scrapers)
    # Also run every 6 hours for more frequent updates
    schedule.every(6).hours.do(run_all_scrapers)

    log.info("Scheduler running: daily at 06:00 UTC + every 6 hours")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    start_scheduler(run_immediately=True)
