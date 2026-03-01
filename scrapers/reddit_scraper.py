"""
scrapers/reddit_scraper.py
Scrapes Reddit using httpx (no Playwright, no API key).
Uses Reddit's JSON API: reddit.com/r/subreddit.json
Fetches NEW posts + OLD posts (full history via after= pagination).
Full duplicate prevention via source_url uniqueness.
"""
from __future__ import annotations
import time
import asyncio
import httpx
from datetime import datetime, timezone

from config import REDDIT_SUBREDDITS, SCRAPE_LIMIT_REDDIT, RATE_LIMIT_DELAY, INTERVIEW_KEYWORDS
from utils.logger import log
from utils.parser import parse_title
from database.db import (
    get_client, upsert_company, url_exists,
    insert_post, start_scraper_log, finish_scraper_log
)

# Reddit has a public JSON API — no auth needed, much more reliable than Playwright
REDDIT_JSON_BASE = "https://www.reddit.com/r/{subreddit}/{sort}.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; InterviewBot/1.0; +https://github.com/interview-scraper)",
    "Accept":     "application/json",
}


def is_interview_post(title: str, body: str = "") -> bool:
    combined = (title + " " + body[:300]).lower()
    return any(kw in combined for kw in INTERVIEW_KEYWORDS)


def fetch_reddit_page(
    client:    httpx.Client,
    subreddit: str,
    sort:      str = "new",
    after:     str = None,
    limit:     int = 100,
) -> tuple[list[dict], str | None]:
    """
    Fetch one page of Reddit posts. Returns (posts, next_after_token).
    sort: new | top | hot | rising
    after: pagination token for fetching older posts
    """
    url    = REDDIT_JSON_BASE.format(subreddit=subreddit, sort=sort)
    params = {"limit": limit, "raw_json": 1}
    if after:
        params["after"] = after

    try:
        resp = client.get(url, params=params, headers=HEADERS, timeout=15)

        if resp.status_code == 429:
            log.warning(f"Reddit rate limited on r/{subreddit}, waiting 30s...")
            time.sleep(30)
            resp = client.get(url, params=params, headers=HEADERS, timeout=15)

        if resp.status_code == 403:
            log.warning(f"r/{subreddit} is private or banned")
            return [], None

        if resp.status_code == 404:
            log.warning(f"r/{subreddit} not found")
            return [], None

        resp.raise_for_status()
        data     = resp.json()
        children = data.get("data", {}).get("children", [])
        next_tok = data.get("data", {}).get("after")

        posts = []
        for child in children:
            p = child.get("data", {})
            # Skip non-text posts (images, links)
            if p.get("is_self") is False and not p.get("selftext"):
                continue
            posts.append(p)

        return posts, next_tok

    except Exception as e:
        log.error(f"Reddit fetch error r/{subreddit}: {e}")
        return [], None


def fetch_post_body(client: httpx.Client, permalink: str) -> str:
    """Fetch full post body via Reddit JSON API."""
    try:
        url  = f"https://www.reddit.com{permalink}.json"
        resp = client.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            post_data = data[0].get("data", {}).get("children", [{}])[0].get("data", {})
            return post_data.get("selftext", "")
    except Exception:
        pass
    return ""


def process_reddit_post(p: dict, client: httpx.Client, subreddit: str, db) -> bool:
    """
    Process a single Reddit post dict.
    Returns True if inserted, False if skipped/error.
    """
    title     = (p.get("title") or "").strip()
    post_url  = f"https://www.reddit.com{p.get('permalink', '')}"
    body      = p.get("selftext", "") or ""
    created   = p.get("created_utc", 0)

    if not title or not post_url or post_url == "https://www.reddit.com":
        return False

    # ── Duplicate prevention (URL-based) ──────────────────────────
    if url_exists(db, post_url):
        log.debug(f"Duplicate skipped: {title[:50]}")
        return False

    # Quick relevance filter
    if not is_interview_post(title, body):
        return False

    # Fetch full body if truncated
    if body in ("[removed]", "[deleted]", "") or (len(body) < 50 and p.get("permalink")):
        body = fetch_post_body(client, p["permalink"]) or body

    # Skip deleted/removed posts with no content
    if body in ("[removed]", "[deleted]") or (not body.strip() and not title):
        return False

    # Parse date
    published = None
    if created:
        try:
            published = datetime.fromtimestamp(created, tz=timezone.utc).isoformat()
        except Exception:
            pass

    parsed     = parse_title(title, body[:300])
    company_id = upsert_company(db, parsed.get("company")) if parsed.get("company") else None

    post_row = {
        "title":            title,
        "company_id":       company_id,
        "category":         parsed.get("category", "other"),
        "source":           "reddit",
        "source_url":       post_url,
        "published_date":   published,
        "raw_content":      body[:50000],
        "cleaned_content":  "",
        "ai_summary":       "",
        "tags":             [],
        "subreddit":        subreddit,
        "role":             parsed.get("role"),
        "experience_level": parsed.get("experience_level"),
        "interview_result": parsed.get("interview_result", "unknown"),
    }

    post_id = insert_post(db, post_row)
    if post_id:
        log.success(f"[Reddit/{subreddit}] ✅ {title[:65]}")
        return True
    return False


def scrape_subreddit(client: httpx.Client, subreddit: str, db, fetch_old: bool = True) -> dict:
    """
    Scrape a subreddit:
    - Always fetches /new (latest posts)
    - If fetch_old=True also paginates through /top?t=all for historical posts
    Full duplicate prevention via url_exists check on every post.
    """
    stats = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}

    def process_page(posts: list) -> int:
        inserted = 0
        for p in posts:
            stats["found"] += 1
            try:
                ok = process_reddit_post(p, client, subreddit, db)
                if ok:
                    inserted += 1
                    stats["inserted"] += 1
                else:
                    stats["skipped"] += 1
            except Exception as e:
                log.error(f"Post processing error: {e}")
                stats["errors"] += 1
            time.sleep(RATE_LIMIT_DELAY)
        return inserted

    # ── 1. Fetch NEW posts ────────────────────────────────────────
    log.info(f"r/{subreddit} → fetching NEW posts")
    after = None
    pages = 0
    while pages < 3:  # Up to 300 new posts
        posts, after = fetch_reddit_page(client, subreddit, sort="new", after=after)
        if not posts:
            break
        process_page(posts)
        pages += 1
        if not after:
            break
        time.sleep(2)

    # ── 2. Fetch OLD/historical posts via top?t=all ───────────────
    if fetch_old:
        log.info(f"r/{subreddit} → fetching OLD posts (top all-time)")
        after = None
        pages = 0
        max_old_pages = SCRAPE_LIMIT_REDDIT // 25  # respect limit

        while pages < max_old_pages:
            posts, after = fetch_reddit_page(client, subreddit, sort="top", after=after, limit=100)
            if not posts:
                break
            inserted = process_page(posts)
            pages += 1

            # Stop going deeper if mostly duplicates
            if inserted == 0 and pages > 2:
                log.info(f"r/{subreddit}: All old posts already in DB, stopping pagination")
                break
            if not after:
                break
            time.sleep(2)

    log.info(f"r/{subreddit} done: {stats}")
    return stats


def run_reddit_scraper(fetch_old: bool = True):
    """
    Main entry point.
    fetch_old=True  → also scrape historical posts (first run / weekly)
    fetch_old=False → only new posts (hourly runs)
    """
    log.info("=" * 60)
    log.info(f"Starting Reddit scraper (JSON API, fetch_old={fetch_old})")

    db     = get_client()
    log_id = start_scraper_log(db, "reddit")
    total  = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}

    with httpx.Client(follow_redirects=True, timeout=15) as client:
        for subreddit in REDDIT_SUBREDDITS:
            try:
                stats = scrape_subreddit(client, subreddit, db, fetch_old=fetch_old)
                for k in ("found", "inserted", "skipped", "errors"):
                    total[k] += stats[k]
            except Exception as e:
                log.error(f"Subreddit {subreddit} failed: {e}", exc_info=True)
                total["errors"] += 1
            time.sleep(3)  # polite pause between subreddits

    total["status"] = "success"
    finish_scraper_log(db, log_id, total)
    log.info(f"Reddit done → {total}")
    return total


if __name__ == "__main__":
    run_reddit_scraper(fetch_old=True)
