"""
scrapers/reddit_scraper.py - Reddit scraper using Playwright (no API key needed)
Scrapes old.reddit.com which has simpler HTML and no JS rendering issues
"""
from __future__ import annotations
import asyncio
import time
from datetime import datetime
from playwright.async_api import async_playwright, Page, Browser

from config import (
    REDDIT_SUBREDDITS, SCRAPE_LIMIT_REDDIT, RATE_LIMIT_DELAY, INTERVIEW_KEYWORDS
)
from utils.logger import log
from utils.parser import parse_title
from database.db import (
    get_client, upsert_company, url_exists, insert_post,
    start_scraper_log, finish_scraper_log, insert_questions
)
from ai.processor import process_post


BASE_URL = "https://old.reddit.com/r/{subreddit}/new/"


def is_interview_post(title: str, content: str = "") -> bool:
    combined = (title + " " + content[:300]).lower()
    return any(kw in combined for kw in INTERVIEW_KEYWORDS)


async def scrape_post_content(page: Page, post_url: str) -> str:
    """Visit a post page and extract the full self-text."""
    try:
        old_url = post_url.replace("www.reddit.com", "old.reddit.com")
        await page.goto(old_url, wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(1500)
        content = await page.evaluate("""
            () => {
                const body = document.querySelector('.usertext-body .md');
                return body ? body.innerText : '';
            }
        """)
        return content.strip()
    except Exception as e:
        log.warning(f"Could not fetch post content from {post_url}: {e}")
        return ""


async def scrape_subreddit(page: Page, subreddit_name: str, db) -> dict:
    """Scrape a single subreddit listing page using Playwright."""
    stats = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}

    try:
        url = BASE_URL.format(subreddit=subreddit_name)
        log.info(f"Scraping r/{subreddit_name} → {url}")

        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(2000)

        posts_scraped = 0

        while posts_scraped < SCRAPE_LIMIT_REDDIT:
            # Extract post listing data from current page
            posts_data = await page.evaluate("""
                () => {
                    const posts = [];
                    document.querySelectorAll('.thing.link').forEach(el => {
                        const titleEl = el.querySelector('a.title');
                        const timeEl  = el.querySelector('time');
                        const permalink = el.getAttribute('data-permalink');
                        if (!titleEl || !permalink) return;
                        posts.push({
                            title:     titleEl.innerText.trim(),
                            url:       'https://www.reddit.com' + permalink,
                            timestamp: timeEl ? timeEl.getAttribute('datetime') : null,
                        });
                    });
                    return posts;
                }
            """)

            if not posts_data:
                log.warning(f"No posts found on r/{subreddit_name} — Reddit may be blocking")
                break

            for post in posts_data:
                if posts_scraped >= SCRAPE_LIMIT_REDDIT:
                    break

                stats["found"] += 1
                post_url = post["url"]

                # Deduplication
                if url_exists(db, post_url):
                    stats["skipped"] += 1
                    continue

                # Quick relevance filter on title alone first (cheap)
                if not is_interview_post(post["title"]):
                    stats["skipped"] += 1
                    continue

                # Fetch full content only for relevant posts
                raw_content = await scrape_post_content(page, post_url)

                # Parse published date
                published = None
                if post.get("timestamp"):
                    try:
                        published = datetime.fromisoformat(
                            post["timestamp"].replace("Z", "+00:00")
                        )
                    except Exception:
                        pass

                # Fast regex parse + AI enrichment
                parsed    = parse_title(post["title"], raw_content[:300])
                ai_result = process_post({
                    "id": "temp",
                    "title": post["title"],
                    "raw_content": raw_content,
                })

                # Merge: AI overrides regex where available
                company_name = (ai_result or {}).get("company") or parsed.get("company")
                category     = (ai_result or {}).get("category") or parsed.get("category", "other")
                role         = (ai_result or {}).get("role") or parsed.get("role")
                exp_level    = (ai_result or {}).get("experience_level") or parsed.get("experience_level")
                result       = (ai_result or {}).get("interview_result") or parsed.get("interview_result", "unknown")

                company_id = upsert_company(db, company_name) if company_name else None

                post_row = {
                    "title":            post["title"],
                    "company_id":       company_id,
                    "category":         category,
                    "source":           "reddit",
                    "source_url":       post_url,
                    "published_date":   published,
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
                    posts_scraped += 1
                    log.success(f"[Reddit/{subreddit_name}] {post['title'][:65]}")
                    if ai_result and ai_result.get("questions"):
                        insert_questions(db, post_id, ai_result["questions"])
                else:
                    stats["errors"] += 1

                await asyncio.sleep(RATE_LIMIT_DELAY)

            # Paginate: click the "next" button
            try:
                next_btn = await page.query_selector('a[rel="next"]')
                if next_btn:
                    await next_btn.click()
                    await page.wait_for_timeout(2500)
                else:
                    break
            except Exception:
                break

    except Exception as e:
        log.error(f"Error scraping r/{subreddit_name}: {e}")
        stats["errors"] += 1

    return stats


async def run_reddit_scraper_async():
    """Full async scraping run across all configured subreddits."""
    log.info("=" * 60)
    log.info("Starting Reddit scraper (Playwright — no API key needed)")

    db     = get_client()
    log_id = start_scraper_log(db, "reddit")
    total  = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )

        # Hide automation fingerprint
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )

        page = await context.new_page()

        for subreddit_name in REDDIT_SUBREDDITS:
            stats = await scrape_subreddit(page, subreddit_name, db)
            for k in total:
                total[k] += stats[k]
            await asyncio.sleep(3)  # Polite pause between subreddits

        await browser.close()

    total["status"] = "success"
    finish_scraper_log(db, log_id, total)
    log.info(f"Reddit scraper complete: {total}")
    return total


def run_reddit_scraper():
    """Synchronous wrapper — called from scheduler & FastAPI background tasks."""
    return asyncio.run(run_reddit_scraper_async())


if __name__ == "__main__":
    run_reddit_scraper()
