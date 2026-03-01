"""
scrapers/reddit_scraper.py - Scrapes Reddit for interview experience posts using PRAW
"""
from __future__ import annotations
import time
import praw
from datetime import datetime

from config import (
    REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT,
    REDDIT_SUBREDDITS, SCRAPE_LIMIT_REDDIT, RATE_LIMIT_DELAY, INTERVIEW_KEYWORDS,
)
from utils.logger import log
from utils.parser import parse_title
from database.db import get_client, upsert_company, url_exists, insert_post, start_scraper_log, finish_scraper_log
from ai.processor import process_post
from database.db import insert_questions, mark_post_processed


def get_reddit_client() -> praw.Reddit:
    return praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
        ratelimit_seconds=1,
    )


def is_interview_post(title: str, selftext: str) -> bool:
    """Quick check before expensive AI call."""
    combined = (title + " " + selftext[:200]).lower()
    return any(kw in combined for kw in INTERVIEW_KEYWORDS)


def scrape_subreddit(reddit: praw.Reddit, subreddit_name: str, db) -> dict:
    """Scrape a single subreddit. Returns stats dict."""
    stats = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}

    try:
        subreddit = reddit.subreddit(subreddit_name)
        log.info(f"Scraping r/{subreddit_name} (limit={SCRAPE_LIMIT_REDDIT})")

        for submission in subreddit.new(limit=SCRAPE_LIMIT_REDDIT):
            stats["found"] += 1
            post_url = f"https://www.reddit.com{submission.permalink}"

            # Skip if already scraped
            if url_exists(db, post_url):
                stats["skipped"] += 1
                continue

            # Quick relevance check
            if not is_interview_post(submission.title, submission.selftext or ""):
                stats["skipped"] += 1
                continue

            raw_content = submission.selftext or ""

            # Quick title parsing (fast, no AI)
            parsed = parse_title(submission.title, raw_content[:300])

            # Full AI processing
            ai_result = process_post({
                "id": "temp",
                "title": submission.title,
                "raw_content": raw_content,
            })

            # Merge: AI result overrides title parser where available
            company_name = (ai_result or {}).get("company") or parsed.get("company")
            category     = (ai_result or {}).get("category") or parsed.get("category", "other")
            role         = (ai_result or {}).get("role") or parsed.get("role")
            exp_level    = (ai_result or {}).get("experience_level") or parsed.get("experience_level")
            result       = (ai_result or {}).get("interview_result") or parsed.get("interview_result", "unknown")

            # Upsert company
            company_id = upsert_company(db, company_name) if company_name else None

            post_row = {
                "title":            submission.title,
                "company_id":       company_id,
                "category":         category,
                "source":           "reddit",
                "source_url":       post_url,
                "published_date":   submission.created_utc,
                "raw_content":      raw_content,
                "cleaned_content":  (ai_result or {}).get("cleaned_content", ""),
                "ai_summary":       (ai_result or {}).get("ai_summary", ""),
                "tags":             (ai_result or {}).get("tags", []),
                "subreddit":        subreddit_name,
                "role":             role,
                "experience_level": exp_level,
                "interview_result": result,
            }

            post_id = insert_post(db, post_row)
            if post_id:
                stats["inserted"] += 1
                log.success(f"[Reddit] Inserted: {submission.title[:60]}")

                # Insert extracted questions
                if ai_result and ai_result.get("questions"):
                    insert_questions(db, post_id, ai_result["questions"])
            else:
                stats["errors"] += 1

            time.sleep(RATE_LIMIT_DELAY)

    except Exception as e:
        log.error(f"Error scraping r/{subreddit_name}: {e}")
        stats["errors"] += 1

    return stats


def run_reddit_scraper():
    """Main entry point for Reddit scraping job."""
    log.info("=" * 60)
    log.info("Starting Reddit scraper")

    db = get_client()
    reddit = get_reddit_client()
    log_id = start_scraper_log(db, "reddit")

    total = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}

    for subreddit_name in REDDIT_SUBREDDITS:
        stats = scrape_subreddit(reddit, subreddit_name, db)
        for k in total:
            total[k] += stats[k]
        time.sleep(2)  # Be kind between subreddits

    total["status"] = "success"
    finish_scraper_log(db, log_id, total)
    log.info(f"Reddit scraper done: {total}")
    return total


if __name__ == "__main__":
    run_reddit_scraper()
