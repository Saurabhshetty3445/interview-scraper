"""
database/db.py - Supabase database operations
Handles: company upsert, post deduplication, question insertion, logging
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
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ── Company ───────────────────────────────────────────────────────────────────

def upsert_company(db: Client, name: str) -> Optional[str]:
    """Insert or get company by name. Returns company UUID."""
    if not name:
        return None
    try:
        res = db.table("companies").upsert(
            {"name": name},
            on_conflict="name"
        ).execute()
        if res.data:
            return res.data[0]["id"]
        # If upsert returned nothing, fetch it
        fetch = db.table("companies").select("id").eq("name", name).single().execute()
        return fetch.data["id"] if fetch.data else None
    except Exception as e:
        log.error(f"upsert_company error for '{name}': {e}")
        return None


# ── Posts ─────────────────────────────────────────────────────────────────────

def url_exists(db: Client, url: str) -> bool:
    """Check if a post URL has already been scraped (deduplication)."""
    try:
        res = db.table("posts").select("id").eq("source_url", url).execute()
        return len(res.data) > 0
    except Exception as e:
        log.error(f"url_exists error: {e}")
        return False


def insert_post(db: Client, post: dict) -> Optional[str]:
    """
    Insert a post. Returns the new post UUID or None if skipped/error.
    post dict keys:
      title, company_id, category, source, source_url, published_date,
      raw_content, cleaned_content, ai_summary, tags,
      subreddit, role, experience_level, interview_result
    """
    try:
        # Normalize published_date
        pd = post.get("published_date")
        if isinstance(pd, (int, float)):
            pd = datetime.utcfromtimestamp(pd).isoformat()
        elif isinstance(pd, datetime):
            pd = pd.isoformat()

        row = {
            "title":             post.get("title", "")[:500],
            "company_id":        post.get("company_id"),
            "category":          post.get("category", "other"),
            "source":            post["source"],
            "source_url":        post["source_url"],
            "published_date":    pd,
            "raw_content":       (post.get("raw_content") or "")[:50000],
            "cleaned_content":   (post.get("cleaned_content") or "")[:50000],
            "ai_summary":        post.get("ai_summary"),
            "tags":              post.get("tags", []),
            "subreddit":         post.get("subreddit"),
            "role":              post.get("role"),
            "experience_level":  post.get("experience_level"),
            "interview_result":  post.get("interview_result", "unknown"),
            "is_processed":      bool(post.get("cleaned_content")),
        }

        res = db.table("posts").insert(row).execute()
        if res.data:
            return res.data[0]["id"]
        return None
    except Exception as e:
        log.error(f"insert_post error for '{post.get('title', '')}': {e}")
        return None


def get_unprocessed_posts(db: Client, limit: int = 20) -> list[dict]:
    """Fetch posts that haven't been AI-processed yet."""
    try:
        res = db.table("posts")\
            .select("id, title, raw_content, source")\
            .eq("is_processed", False)\
            .not_.is_("raw_content", "null")\
            .limit(limit)\
            .execute()
        return res.data or []
    except Exception as e:
        log.error(f"get_unprocessed_posts error: {e}")
        return []


def mark_post_processed(db: Client, post_id: str, cleaned_content: str, ai_summary: str, tags: list[str]):
    """Update a post after AI processing."""
    try:
        db.table("posts").update({
            "cleaned_content": cleaned_content[:50000],
            "ai_summary":      ai_summary[:2000],
            "tags":            tags,
            "is_processed":    True,
        }).eq("id", post_id).execute()
    except Exception as e:
        log.error(f"mark_post_processed error for post {post_id}: {e}")


# ── Questions ─────────────────────────────────────────────────────────────────

def insert_questions(db: Client, post_id: str, questions: list[dict]):
    """
    Bulk-insert extracted questions for a post.
    Each question dict: question_text, question_type, difficulty, tags
    """
    if not questions:
        return
    rows = []
    for q in questions:
        if not q.get("question_text"):
            continue
        rows.append({
            "post_id":       post_id,
            "question_text": q["question_text"][:2000],
            "question_type": q.get("question_type", "unknown"),
            "difficulty":    q.get("difficulty"),
            "tags":          q.get("tags", []),
        })
    if rows:
        try:
            db.table("questions").insert(rows).execute()
            log.debug(f"Inserted {len(rows)} questions for post {post_id}")
        except Exception as e:
            log.error(f"insert_questions error: {e}")


# ── Scraper Logs ──────────────────────────────────────────────────────────────

def start_scraper_log(db: Client, source: str) -> Optional[str]:
    try:
        res = db.table("scraper_logs").insert({"source": source}).execute()
        return res.data[0]["id"] if res.data else None
    except Exception as e:
        log.error(f"start_scraper_log error: {e}")
        return None


def finish_scraper_log(db: Client, log_id: str, stats: dict):
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
        log.error(f"finish_scraper_log error: {e}")
