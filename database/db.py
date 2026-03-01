"""
database/db.py - All Supabase operations.
Uses service role key which bypasses RLS completely.
"""
from __future__ import annotations
import uuid
from typing import Optional
from datetime import datetime

from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_SERVICE_KEY
from utils.logger import log


def get_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in environment")
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ── Company ───────────────────────────────────────────────────────────────────

def upsert_company(db: Client, name: str) -> Optional[str]:
    if not name or not name.strip():
        return None
    name = name.strip()
    try:
        # Try insert first
        res = db.table("companies").upsert(
            {"name": name},
            on_conflict="name",
            returning="representation",
        ).execute()
        if res.data:
            return res.data[0]["id"]
        # Fallback: select
        res2 = db.table("companies").select("id").eq("name", name).limit(1).execute()
        return res2.data[0]["id"] if res2.data else None
    except Exception as e:
        log.error(f"upsert_company error '{name}': {e}")
        try:
            res = db.table("companies").select("id").eq("name", name).limit(1).execute()
            return res.data[0]["id"] if res.data else None
        except Exception:
            return None


# ── Deduplication ─────────────────────────────────────────────────────────────

def url_exists(db: Client, url: str) -> bool:
    """Returns True if this URL has already been scraped."""
    try:
        res = db.table("posts").select("id").eq("source_url", url).limit(1).execute()
        return len(res.data) > 0
    except Exception as e:
        log.error(f"url_exists error: {e}")
        return False  # On error, allow insert attempt


# ── Posts ─────────────────────────────────────────────────────────────────────

def insert_post(db: Client, post: dict) -> Optional[str]:
    """
    Insert a post row. Returns UUID on success, None on failure.
    Handles deduplication via UNIQUE constraint on source_url.
    """
    try:
        # Normalize published_date to ISO string
        pd = post.get("published_date")
        if isinstance(pd, (int, float)):
            pd = datetime.utcfromtimestamp(pd).isoformat()
        elif isinstance(pd, datetime):
            pd = pd.isoformat()
        elif pd is not None and not isinstance(pd, str):
            pd = str(pd)

        row = {
            "title":             (post.get("title") or "Untitled")[:500],
            "company_id":        post.get("company_id"),          # UUID or None
            "category":          post.get("category") or "other",
            "source":            post["source"],
            "source_url":        post["source_url"],
            "published_date":    pd,
            "raw_content":       (post.get("raw_content") or "")[:50000],
            "cleaned_content":   (post.get("cleaned_content") or "")[:50000],
            "ai_summary":        post.get("ai_summary") or None,
            "tags":              post.get("tags") or [],
            "subreddit":         post.get("subreddit") or None,
            "role":              post.get("role") or None,
            "experience_level":  post.get("experience_level") or None,
            "interview_result":  post.get("interview_result") or "unknown",
            "is_processed":      False,
        }

        res = db.table("posts").insert(row).execute()
        if res.data:
            return res.data[0]["id"]
        return None

    except Exception as e:
        err_str = str(e)
        if "duplicate" in err_str.lower() or "unique" in err_str.lower() or "23505" in err_str:
            log.debug(f"Duplicate post skipped: {post.get('source_url', '')[:60]}")
        else:
            log.error(f"insert_post error '{post.get('title', '')[:40]}': {e}")
        return None


def get_unprocessed_posts(db: Client, limit: int = 20) -> list[dict]:
    try:
        res = (
            db.table("posts")
            .select("id, title, raw_content, source")
            .eq("is_processed", False)
            .not_.is_("raw_content", "null")
            .neq("raw_content", "")
            .limit(limit)
            .execute()
        )
        return res.data or []
    except Exception as e:
        log.error(f"get_unprocessed_posts error: {e}")
        return []


def mark_post_processed(db: Client, post_id: str, cleaned_content: str, ai_summary: str, tags: list):
    try:
        db.table("posts").update({
            "cleaned_content": cleaned_content[:50000],
            "ai_summary":      (ai_summary or "")[:2000],
            "tags":            tags or [],
            "is_processed":    True,
        }).eq("id", post_id).execute()
    except Exception as e:
        log.error(f"mark_post_processed error: {e}")


# ── Questions ─────────────────────────────────────────────────────────────────

def insert_questions(db: Client, post_id: str, questions: list[dict]):
    if not questions or not post_id:
        return
    rows = []
    for q in questions:
        text = (q.get("question_text") or "").strip()
        if not text or len(text) < 5:
            continue
        rows.append({
            "post_id":       post_id,
            "question_text": text[:2000],
            "question_type": q.get("question_type") or "unknown",
            "difficulty":    q.get("difficulty") or None,
            "tags":          q.get("tags") or [],
        })
    if rows:
        try:
            db.table("questions").insert(rows).execute()
            log.debug(f"Inserted {len(rows)} questions for post {post_id}")
        except Exception as e:
            log.error(f"insert_questions error: {e}")


# ── Scraper Logs ──────────────────────────────────────────────────────────────

def start_scraper_log(db: Client, source: str) -> Optional[str]:
    """Create a scraper run log entry. Returns log UUID or None."""
    try:
        res = db.table("scraper_logs").insert({
            "source": source,
            "status": "running",
        }).execute()
        if res.data:
            return res.data[0]["id"]
        return None
    except Exception as e:
        log.warning(f"start_scraper_log failed (non-critical): {e}")
        return None  # Non-fatal — scraping continues without log


def finish_scraper_log(db: Client, log_id: Optional[str], stats: dict):
    """Update scraper log on completion. Safe to call with None log_id."""
    if not log_id:
        return  # Log was never created, skip silently
    try:
        db.table("scraper_logs").update({
            "finished_at":    datetime.utcnow().isoformat(),
            "posts_found":    stats.get("found", 0),
            "posts_inserted": stats.get("inserted", 0),
            "posts_skipped":  stats.get("skipped", 0),
            "errors":         stats.get("errors", 0),
            "status":         stats.get("status", "success"),
            "error_message":  stats.get("error_message"),
        }).eq("id", log_id).execute()
    except Exception as e:
        log.warning(f"finish_scraper_log failed (non-critical): {e}")
