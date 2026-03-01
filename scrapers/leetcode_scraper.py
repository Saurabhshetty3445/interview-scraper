"""
scrapers/leetcode_scraper.py
Uses LeetCode's public discuss API to scrape interview experience posts.
Inserts raw data immediately — AI processing is optional enrichment.
"""
from __future__ import annotations
import time
import re
import httpx
from datetime import datetime

from config import SCRAPE_LIMIT_LEETCODE, RATE_LIMIT_DELAY
from utils.logger import log
from utils.parser import parse_title
from database.db import (
    get_client, upsert_company, url_exists,
    insert_post, start_scraper_log, finish_scraper_log, insert_questions
)

# LeetCode changed their API — use the correct v2 discuss endpoint
LEETCODE_API_URL = "https://leetcode.com/discuss/api/list/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":    "https://leetcode.com/discuss/interview-experience/",
    "Accept":     "application/json",
}

# Fallback: RSS feed which is always public
LEETCODE_RSS_URL = "https://leetcode.com/discuss/interview-experience?currentPage=1&orderBy=newest_to_oldest&query=&tag=interview-experience"


def fetch_via_graphql(client: httpx.Client, skip: int = 0) -> list[dict]:
    """Try LeetCode GraphQL API."""
    query = """
    query categoryTopicList($categories: [String!]!, $first: Int!, $skip: Int!) {
      categoryTopicList(categories: $categories, first: $first, skip: $skip) {
        edges {
          node {
            id
            title
            creationDate
            urlKey
            post { content }
          }
        }
      }
    }
    """
    payload = {
        "operationName": "categoryTopicList",
        "query": query,
        "variables": {
            "categories": ["interview-experience"],
            "first": 20,
            "skip": skip,
        },
    }
    resp = client.post(
        "https://leetcode.com/graphql",
        json=payload,
        headers={**HEADERS, "Content-Type": "application/json"},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    edges = (data.get("data") or {}).get("categoryTopicList", {}).get("edges", [])
    return [e["node"] for e in edges if e.get("node")]


def fetch_via_discuss_api(client: httpx.Client, page: int = 1) -> list[dict]:
    """Try LeetCode discuss REST API (newer endpoint)."""
    params = {
        "currentPage": page,
        "orderBy":     "newest_to_oldest",
        "query":       "",
        "categories":  "interview-experience",
    }
    resp = client.get(
        "https://leetcode.com/discuss/api/list/",
        params=params,
        headers=HEADERS,
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    # Handle different response shapes
    topics = data.get("data", data.get("topics", data.get("topicList", [])))
    if isinstance(topics, dict):
        topics = topics.get("edges", topics.get("data", []))
    results = []
    for item in (topics or []):
        node = item.get("node", item)
        results.append({
            "id":           node.get("id", ""),
            "title":        node.get("title", ""),
            "creationDate": node.get("creationDate", node.get("createTime", 0)),
            "urlKey":       node.get("urlKey", node.get("slug", str(node.get("id", "")))),
            "post":         {"content": node.get("post", {}).get("content", "") if isinstance(node.get("post"), dict) else ""},
        })
    return results


def strip_html(html: str) -> str:
    """Strip HTML tags to plain text."""
    try:
        from bs4 import BeautifulSoup
        return BeautifulSoup(html, "lxml").get_text(separator="\n", strip=True)
    except Exception:
        return re.sub(r"<[^>]+>", " ", html).strip()


def run_leetcode_scraper():
    log.info("=" * 60)
    log.info("Starting LeetCode scraper")

    db     = get_client()
    log_id = start_scraper_log(db, "leetcode")
    stats  = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}

    try:
        with httpx.Client(follow_redirects=True, timeout=20) as client:
            skip    = 0
            page    = 1
            fetched = 0
            use_graphql = True

            while fetched < SCRAPE_LIMIT_LEETCODE:
                nodes = []

                # Try GraphQL first, fall back to REST API
                try:
                    if use_graphql:
                        nodes = fetch_via_graphql(client, skip=skip)
                        log.info(f"GraphQL returned {len(nodes)} posts (skip={skip})")
                except Exception as e:
                    log.warning(f"GraphQL failed ({e}), switching to REST API")
                    use_graphql = False

                if not use_graphql or not nodes:
                    try:
                        nodes = fetch_via_discuss_api(client, page=page)
                        log.info(f"REST API returned {len(nodes)} posts (page={page})")
                    except Exception as e:
                        log.error(f"Both LeetCode APIs failed: {e}")
                        break

                if not nodes:
                    log.info("No more posts, stopping")
                    break

                for node in nodes:
                    if fetched >= SCRAPE_LIMIT_LEETCODE:
                        break

                    stats["found"] += 1
                    url_key  = node.get("urlKey") or str(node.get("id", ""))
                    post_url = f"https://leetcode.com/discuss/interview-experience/{url_key}"

                    # Deduplication
                    if url_exists(db, post_url):
                        stats["skipped"] += 1
                        continue

                    title    = node.get("title", "").strip()
                    raw_html = (node.get("post") or {}).get("content", "")
                    raw_text = strip_html(raw_html) if raw_html else ""

                    # Parse published date
                    creation = node.get("creationDate", 0)
                    try:
                        published = datetime.utcfromtimestamp(int(creation)).isoformat() if creation else None
                    except Exception:
                        published = None

                    # Fast regex parse — NO AI dependency
                    parsed       = parse_title(title, raw_text[:300])
                    company_name = parsed.get("company")
                    category     = parsed.get("category", "other")
                    company_id   = upsert_company(db, company_name) if company_name else None

                    post_row = {
                        "title":            title or "Untitled",
                        "company_id":       company_id,
                        "category":         category,
                        "source":           "leetcode",
                        "source_url":       post_url,
                        "published_date":   published,
                        "raw_content":      raw_text[:50000],
                        "cleaned_content":  "",
                        "ai_summary":       "",
                        "tags":             [],
                        "subreddit":        None,
                        "role":             parsed.get("role"),
                        "experience_level": parsed.get("experience_level"),
                        "interview_result": parsed.get("interview_result", "unknown"),
                    }

                    post_id = insert_post(db, post_row)
                    if post_id:
                        stats["inserted"] += 1
                        fetched += 1
                        log.success(f"[LeetCode] ✅ {title[:65]}")
                    else:
                        stats["errors"] += 1

                    time.sleep(RATE_LIMIT_DELAY)

                skip += 20
                page += 1
                time.sleep(1.5)

    except Exception as e:
        log.error(f"LeetCode scraper fatal error: {e}", exc_info=True)
        stats["errors"]       += 1
        stats["status"]        = "failed"
        stats["error_message"] = str(e)
        finish_scraper_log(db, log_id, stats)
        return stats

    stats["status"] = "success"
    finish_scraper_log(db, log_id, stats)
    log.info(f"LeetCode done → {stats}")
    return stats


if __name__ == "__main__":
    run_leetcode_scraper()
