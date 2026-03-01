"""
scrapers/leetcode_scraper.py
Scrapes LeetCode discuss using their working public API endpoint.
Fetches BOTH new and old posts. Full duplicate prevention.
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
    insert_post, start_scraper_log, finish_scraper_log
)

# Working LeetCode discuss API (verified March 2026)
LEETCODE_DISCUSS_URL = "https://leetcode.com/discuss/api/list/"

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://leetcode.com/discuss/interview-experience/",
    "x-csrftoken":     "dummy",
    "Origin":          "https://leetcode.com",
}


def strip_html(html: str) -> str:
    try:
        from bs4 import BeautifulSoup
        return BeautifulSoup(html, "lxml").get_text(separator="\n", strip=True)
    except Exception:
        return re.sub(r"<[^>]+>", " ", html).strip()


def fetch_posts_page(client: httpx.Client, page: int = 1, order: str = "newest_to_oldest") -> list[dict]:
    """
    Fetch one page of LeetCode interview experience posts.
    order: newest_to_oldest | most_votes | oldest_to_newest
    """
    params = {
        "currentPage": page,
        "orderBy":     order,
        "query":       "",
        "categories":  "interview-experience",
        "tags":        "",
    }
    resp = client.get(
        LEETCODE_DISCUSS_URL,
        params=params,
        headers=HEADERS,
        timeout=20,
    )

    if resp.status_code == 403:
        # Try with cookie-based session fallback
        log.warning("LeetCode returned 403, trying alternative endpoint...")
        raise httpx.HTTPStatusError("403", request=resp.request, response=resp)

    resp.raise_for_status()
    data = resp.json()

    # Parse different possible response shapes
    raw = data
    if "data" in data:
        raw = data["data"]
    if "categoryTopicList" in raw:
        raw = raw["categoryTopicList"]
    if "edges" in raw:
        return [e.get("node", e) for e in raw["edges"]]
    if "topics" in raw:
        return raw["topics"]
    if isinstance(raw, list):
        return raw
    return []


def fetch_posts_graphql(client: httpx.Client, skip: int = 0) -> list[dict]:
    """Alternative: use LeetCode GraphQL with correct query structure."""
    # Updated query that works with current LeetCode API
    query = """
    query discussTopicsList($categories: [String!]!, $first: Int!, $skip: Int!, $orderBy: TopicSortingOption, $query: String, $tags: [String!]) {
      categoryTopicList(categories: $categories, first: $first, skip: $skip, orderBy: $orderBy, query: $query, tags: $tags) {
        ...TopicsList
      }
    }
    fragment TopicsList on TopicConnection {
      edges {
        node {
          id
          title
          creationDate
          urlKey
          post {
            content
            author {
              username
            }
          }
          tags {
            name
            slug
          }
        }
      }
    }
    """
    payload = {
        "operationName": "discussTopicsList",
        "query": query,
        "variables": {
            "categories": ["interview-experience"],
            "first":      20,
            "skip":       skip,
            "orderBy":    "newest_to_oldest",
            "query":      "",
            "tags":       [],
        },
    }
    resp = client.post(
        "https://leetcode.com/graphql/",
        json=payload,
        headers={**HEADERS, "Content-Type": "application/json"},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise Exception(f"GraphQL errors: {data['errors']}")
    edges = (data.get("data") or {}).get("categoryTopicList", {}).get("edges", [])
    return [e["node"] for e in edges if e.get("node")]


def normalize_node(node: dict) -> dict:
    """Normalize different API response shapes into one consistent dict."""
    post_content = ""
    if isinstance(node.get("post"), dict):
        post_content = node["post"].get("content", "")
    elif isinstance(node.get("content"), str):
        post_content = node["content"]

    url_key = node.get("urlKey") or node.get("slug") or str(node.get("id", ""))
    title   = node.get("title", "").strip()
    ts      = node.get("creationDate") or node.get("createTime") or 0

    return {
        "title":        title,
        "url_key":      url_key,
        "content_html": post_content,
        "timestamp":    ts,
    }


def run_leetcode_scraper(fetch_old: bool = True):
    """
    Scrape LeetCode interview experience posts.
    fetch_old=True  → scrapes multiple pages (new + historical)
    fetch_old=False → scrapes only the latest page
    """
    log.info("=" * 60)
    log.info(f"Starting LeetCode scraper (fetch_old={fetch_old})")

    db     = get_client()
    log_id = start_scraper_log(db, "leetcode")
    stats  = {"found": 0, "inserted": 0, "skipped": 0, "errors": 0}

    try:
        with httpx.Client(follow_redirects=True, timeout=20) as client:
            skip        = 0
            page        = 1
            fetched     = 0
            use_graphql = True
            max_pages   = 10 if fetch_old else 1  # fetch_old → go deep

            while fetched < SCRAPE_LIMIT_LEETCODE and page <= max_pages:
                nodes = []

                # Try GraphQL first
                if use_graphql:
                    try:
                        nodes = fetch_posts_graphql(client, skip=skip)
                        log.info(f"GraphQL page skip={skip}: {len(nodes)} posts")
                    except Exception as e:
                        log.warning(f"GraphQL failed: {e} — switching to REST")
                        use_graphql = False

                # Fallback to REST
                if not use_graphql or not nodes:
                    try:
                        nodes = fetch_posts_page(client, page=page)
                        log.info(f"REST page {page}: {len(nodes)} posts")
                    except Exception as e:
                        log.error(f"LeetCode REST also failed: {e}")
                        # Final fallback: scrape HTML directly
                        try:
                            nodes = scrape_html_fallback(client, page=page)
                            log.info(f"HTML fallback page {page}: {len(nodes)} posts")
                        except Exception as e2:
                            log.error(f"All LeetCode methods failed: {e2}")
                            break

                if not nodes:
                    log.info("No more posts from LeetCode")
                    break

                for node in nodes:
                    if fetched >= SCRAPE_LIMIT_LEETCODE:
                        break

                    stats["found"] += 1
                    n        = normalize_node(node)
                    post_url = f"https://leetcode.com/discuss/interview-experience/{n['url_key']}"

                    # ── Duplicate prevention ──────────────────────────────
                    if url_exists(db, post_url):
                        stats["skipped"] += 1
                        log.debug(f"Duplicate skipped: {n['title'][:50]}")
                        continue

                    raw_text  = strip_html(n["content_html"]) if n["content_html"] else ""
                    published = None
                    if n["timestamp"]:
                        try:
                            published = datetime.utcfromtimestamp(int(n["timestamp"])).isoformat()
                        except Exception:
                            pass

                    parsed     = parse_title(n["title"], raw_text[:300])
                    company_id = upsert_company(db, parsed.get("company")) if parsed.get("company") else None

                    post_row = {
                        "title":            n["title"] or "Untitled",
                        "company_id":       company_id,
                        "category":         parsed.get("category", "other"),
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
                        log.success(f"[LeetCode] ✅ {n['title'][:65]}")
                    else:
                        stats["errors"] += 1

                    time.sleep(RATE_LIMIT_DELAY)

                skip += 20
                page += 1
                time.sleep(1.5)

    except Exception as e:
        log.error(f"LeetCode fatal: {e}", exc_info=True)
        stats["errors"]       += 1
        stats["status"]        = "failed"
        stats["error_message"] = str(e)
        finish_scraper_log(db, log_id, stats)
        return stats

    stats["status"] = "success"
    finish_scraper_log(db, log_id, stats)
    log.info(f"LeetCode done → {stats}")
    return stats


def scrape_html_fallback(client: httpx.Client, page: int = 1) -> list[dict]:
    """Last resort: scrape the LeetCode discuss HTML page directly."""
    url  = f"https://leetcode.com/discuss/interview-experience/?currentPage={page}&orderBy=newest_to_oldest"
    resp = client.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()

    from bs4 import BeautifulSoup
    soup  = BeautifulSoup(resp.text, "lxml")
    nodes = []

    # Look for topic links in the HTML
    for a in soup.select('a[href*="/discuss/"]'):
        href  = a.get("href", "")
        title = a.get_text(strip=True)
        if "/discuss/interview-experience/" in href and len(title) > 10:
            slug = href.rstrip("/").split("/")[-1]
            nodes.append({
                "title":        title,
                "urlKey":       slug,
                "post":         {"content": ""},
                "creationDate": 0,
            })

    return nodes


if __name__ == "__main__":
    run_leetcode_scraper(fetch_old=True)
