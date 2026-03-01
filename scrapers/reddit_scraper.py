"""
scrapers/reddit_scraper.py
Scrapes old.reddit.com using Playwright. No API key needed.
Inserts raw data immediately — AI is optional enrichment.
"""
from __future__ import annotations
import asyncio
import time
from datetime import datetime

from config import REDDIT_SUBREDDITS, SCRAPE_LIMIT_REDDIT, RATE_LIMIT_DELAY, INTERVIEW_KEYWORDS
from utils.logger import log
from utils.parser import parse_title
from database.db import (
    get_client, upsert_company, url_exists,
    insert_post, start_scraper_log, finish_scraper_log, insert_questions
)


def is_interview_post(title: str, content: str = "") -> bool:
    combined = (title + " " + content[:200]).lower()
    return any(kw in combined for kw in INTERVIEW_KEYWORDS)


async def get_post_content(page, url: str) -> str:
    """Visit a post and extract its text body."""
    try:
        old_url = url.replace("www.reddit.com", "old.reddit.com")
        await page.goto(old_url, wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(1000)
        content = await page.evaluate("""
            () => {
                const sel = [
                    '.usertext-body .md',
                    '[data-test-id="post-content"]',
                    '.Post__body'
                ];
                for (const s of sel) {
                    const el = document.querySelector(s);
                    if (el && el.innerText.trim()) return el.innerText.trim();
                }
                return '';
            }
        """)
        return (content or "").strip()
    except Exception as e:
        log.warning(f"Could not fetch content from {url}: {e}")
        return ""


async def scrape_subreddit(page, subreddit_name: str, db) -> dict:
    stats = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}
    url   = f"https://old.reddit.com/r/{subreddit_name}/new/"

    try:
        log.info(f"Scraping r/{subreddit_name}")
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(2000)

        scraped = 0
        while scraped < SCRAPE_LIMIT_REDDIT:
            posts = await page.evaluate("""
                () => Array.from(document.querySelectorAll('.thing.link')).map(el => ({
                    title:     (el.querySelector('a.title') || {}).innerText || '',
                    url:       'https://www.reddit.com' + (el.getAttribute('data-permalink') || ''),
                    timestamp: (el.querySelector('time') || {}).getAttribute?.('datetime') || null,
                })).filter(p => p.title && p.url !== 'https://www.reddit.com')
            """)

            if not posts:
                log.warning(f"No posts found on r/{subreddit_name}")
                break

            for post in posts:
                if scraped >= SCRAPE_LIMIT_REDDIT:
                    break

                stats["found"] += 1
                post_url = post["url"]

                if url_exists(db, post_url):
                    stats["skipped"] += 1
                    continue

                if not is_interview_post(post["title"]):
                    stats["skipped"] += 1
                    continue

                # Get full content
                raw_content = await get_post_content(page, post_url)

                # Parse date
                published = None
                if post.get("timestamp"):
                    try:
                        published = datetime.fromisoformat(post["timestamp"].replace("Z", "+00:00"))
                    except Exception:
                        pass

                # Fast regex parse — NO AI dependency for basic insert
                parsed       = parse_title(post["title"], raw_content[:300])
                company_name = parsed.get("company")
                company_id   = upsert_company(db, company_name) if company_name else None

                post_row = {
                    "title":            post["title"],
                    "company_id":       company_id,
                    "category":         parsed.get("category", "other"),
                    "source":           "reddit",
                    "source_url":       post_url,
                    "published_date":   published,
                    "raw_content":      raw_content[:50000],
                    "cleaned_content":  "",
                    "ai_summary":       "",
                    "tags":             [],
                    "subreddit":        subreddit_name,
                    "role":             parsed.get("role"),
                    "experience_level": parsed.get("experience_level"),
                    "interview_result": parsed.get("interview_result", "unknown"),
                }

                post_id = insert_post(db, post_row)
                if post_id:
                    stats["inserted"] += 1
                    scraped += 1
                    log.success(f"[Reddit/{subreddit_name}] ✅ {post['title'][:65]}")
                else:
                    stats["errors"] += 1

                await asyncio.sleep(RATE_LIMIT_DELAY)

            # Go to next page
            try:
                nxt = await page.query_selector('a[rel="next"]')
                if nxt:
                    await nxt.click()
                    await page.wait_for_timeout(2500)
                else:
                    break
            except Exception:
                break

    except Exception as e:
        log.error(f"r/{subreddit_name} error: {e}", exc_info=True)
        stats["errors"] += 1

    return stats


async def run_reddit_async():
    log.info("=" * 60)
    log.info("Starting Reddit scraper (Playwright)")

    db     = get_client()
    log_id = start_scraper_log(db, "reddit")
    total  = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}

    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 900},
            )
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = await context.new_page()

            for subreddit in REDDIT_SUBREDDITS:
                stats = await scrape_subreddit(page, subreddit, db)
                for k in total:
                    total[k] += stats[k]
                await asyncio.sleep(3)

            await browser.close()

    except Exception as e:
        log.error(f"Reddit scraper fatal: {e}", exc_info=True)
        total["errors"]       += 1
        total["status"]        = "failed"
        total["error_message"] = str(e)
        finish_scraper_log(db, log_id, total)
        return total

    total["status"] = "success"
    finish_scraper_log(db, log_id, total)
    log.info(f"Reddit done → {total}")
    return total


def run_reddit_scraper():
    return asyncio.run(run_reddit_async())


if __name__ == "__main__":
    run_reddit_scraper()
