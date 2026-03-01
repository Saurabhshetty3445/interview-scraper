"""
scrapers/leetcode_scraper.py - Scrapes LeetCode Discuss for interview experience posts
Uses LeetCode's GraphQL API (public, no auth required) + httpx
"""
from __future__ import annotations
import time
import httpx
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential

from config import SCRAPE_LIMIT_LEETCODE, RATE_LIMIT_DELAY, INTERVIEW_KEYWORDS
from utils.logger import log
from utils.parser import parse_title
from database.db import get_client, upsert_company, url_exists, insert_post, start_scraper_log, finish_scraper_log
from ai.processor import process_post
from database.db import insert_questions


LEETCODE_GRAPHQL_URL = "https://leetcode.com/graphql"

DISCUSS_QUERY = """
query categoryTopicList($categories: [String!]!, $first: Int!, $skip: Int!, $tags: [String!]) {
  categoryTopicList(categories: $categories, first: $first, skip: $skip, tags: $tags) {
    edges {
      node {
        id
        title
        creationDate
        tags {
          name
        }
        post {
          content
        }
        urlKey
      }
    }
  }
}
"""

TOPIC_DETAIL_QUERY = """
query topicDetail($topicId: Int!) {
  topic(id: $topicId) {
    id
    title
    creationDate
    post {
      content
    }
    tags {
      name
    }
    urlKey
  }
}
"""

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; InterviewScraper/1.0)",
    "Referer": "https://leetcode.com/discuss/",
}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def fetch_discuss_posts(client: httpx.Client, skip: int = 0) -> list[dict]:
    """Fetch a page of LeetCode discussion posts about interview experiences."""
    payload = {
        "query": DISCUSS_QUERY,
        "variables": {
            "categories": ["interview-experience"],
            "first": 20,
            "skip": skip,
            "tags": [],
        }
    }
    resp = client.post(LEETCODE_GRAPHQL_URL, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    edges = data.get("data", {}).get("categoryTopicList", {}).get("edges", [])
    return [edge["node"] for edge in edges]


def parse_html_content(html: str) -> str:
    """Strip HTML tags from LeetCode post content."""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        return soup.get_text(separator="\n", strip=True)
    except Exception:
        # Fallback: basic tag stripping
        import re
        return re.sub(r'<[^>]+>', ' ', html).strip()


def is_interview_post(title: str, content: str) -> bool:
    """Quick relevance filter."""
    combined = (title + " " + content[:300]).lower()
    return any(kw in combined for kw in INTERVIEW_KEYWORDS)


def run_leetcode_scraper():
    """Main entry point for LeetCode scraping job."""
    log.info("=" * 60)
    log.info("Starting LeetCode scraper")

    db = get_client()
    log_id = start_scraper_log(db, "leetcode")
    stats = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}

    try:
        with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
            skip = 0
            fetched = 0

            while fetched < SCRAPE_LIMIT_LEETCODE:
                posts = fetch_discuss_posts(client, skip=skip)
                if not posts:
                    log.info("No more posts from LeetCode")
                    break

                for node in posts:
                    stats["found"] += 1
                    post_url = f"https://leetcode.com/discuss/interview-experience/{node['urlKey']}"

                    if url_exists(db, post_url):
                        stats["skipped"] += 1
                        continue

                    # Parse content
                    raw_html = (node.get("post") or {}).get("content") or ""
                    raw_text = parse_html_content(raw_html)

                    if not is_interview_post(node["title"], raw_text):
                        stats["skipped"] += 1
                        continue

                    # LeetCode timestamp is in seconds
                    published = node.get("creationDate")

                    # Quick title parse
                    parsed = parse_title(node["title"], raw_text[:300])

                    # AI processing
                    ai_result = process_post({
                        "id": "temp",
                        "title": node["title"],
                        "raw_content": raw_text,
                    })

                    company_name = (ai_result or {}).get("company") or parsed.get("company")
                    category     = (ai_result or {}).get("category") or parsed.get("category", "other")
                    role         = (ai_result or {}).get("role") or parsed.get("role")
                    exp_level    = (ai_result or {}).get("experience_level") or parsed.get("experience_level")
                    result       = (ai_result or {}).get("interview_result") or parsed.get("interview_result", "unknown")

                    company_id = upsert_company(db, company_name) if company_name else None

                    post_row = {
                        "title":            node["title"],
                        "company_id":       company_id,
                        "category":         category,
                        "source":           "leetcode",
                        "source_url":       post_url,
                        "published_date":   published,
                        "raw_content":      raw_text,
                        "cleaned_content":  (ai_result or {}).get("cleaned_content", ""),
                        "ai_summary":       (ai_result or {}).get("ai_summary", ""),
                        "tags":             (ai_result or {}).get("tags", []),
                        "subreddit":        None,
                        "role":             role,
                        "experience_level": exp_level,
                        "interview_result": result,
                    }

                    post_id = insert_post(db, post_row)
                    if post_id:
                        stats["inserted"] += 1
                        log.success(f"[LeetCode] Inserted: {node['title'][:60]}")
                        if ai_result and ai_result.get("questions"):
                            insert_questions(db, post_id, ai_result["questions"])
                    else:
                        stats["errors"] += 1

                    fetched += 1
                    time.sleep(RATE_LIMIT_DELAY)

                skip += 20
                time.sleep(2)

    except Exception as e:
        log.error(f"LeetCode scraper fatal error: {e}")
        stats["errors"] += 1
        stats["status"] = "failed"
        stats["error_message"] = str(e)
        finish_scraper_log(db, log_id, stats)
        return stats

    stats["status"] = "success"
    finish_scraper_log(db, log_id, stats)
    log.info(f"LeetCode scraper done: {stats}")
    return stats


if __name__ == "__main__":
    run_leetcode_scraper()
