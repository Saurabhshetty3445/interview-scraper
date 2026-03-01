"""
main.py - FastAPI app for Railway deployment
"""
import threading
import os
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware


# ── App must be created BEFORE importing heavy modules ────────────────────────
app = FastAPI(
    title="Interview Scraper API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health check — must be fast, no DB, no imports ───────────────────────────
@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/")
async def root():
    return {"status": "online", "service": "Interview Scraper", "version": "1.0.0"}


# ── Lazy imports — only loaded after server is healthy ───────────────────────
def _load_and_start_scheduler():
    """Import heavy deps and start scheduler AFTER server is already up."""
    time.sleep(15)  # Give Railway health check time to pass first
    try:
        from utils.logger import log
        from jobs.scheduler import start_scheduler
        log.info("Starting scheduler...")
        start_scheduler(run_immediately=True)
    except Exception as e:
        print(f"Scheduler error: {e}")


@app.on_event("startup")
async def startup_event():
    """Fire and forget — start scheduler in background thread."""
    t = threading.Thread(target=_load_and_start_scheduler, daemon=True)
    t.start()


# ── Trigger endpoints ─────────────────────────────────────────────────────────
@app.post("/trigger/all")
async def trigger_all(background_tasks: BackgroundTasks):
    from scrapers.reddit_scraper import run_reddit_scraper
    from scrapers.leetcode_scraper import run_leetcode_scraper
    def run_both():
        run_leetcode_scraper()
        run_reddit_scraper()
    background_tasks.add_task(run_both)
    return {"status": "triggered"}


@app.post("/trigger/reddit")
async def trigger_reddit(background_tasks: BackgroundTasks):
    from scrapers.reddit_scraper import run_reddit_scraper
    background_tasks.add_task(run_reddit_scraper)
    return {"status": "triggered", "source": "reddit"}


@app.post("/trigger/leetcode")
async def trigger_leetcode(background_tasks: BackgroundTasks):
    from scrapers.leetcode_scraper import run_leetcode_scraper
    background_tasks.add_task(run_leetcode_scraper)
    return {"status": "triggered", "source": "leetcode"}


@app.get("/health/db")
async def health_db():
    try:
        from database.db import get_client
        db = get_client()
        db.table("companies").select("id").limit(1).execute()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/stats")
async def get_stats():
    try:
        from database.db import get_client
        db = get_client()
        companies = db.table("companies").select("id", count="exact").execute()
        posts     = db.table("posts").select("id", count="exact").execute()
        questions = db.table("questions").select("id", count="exact").execute()
        top       = db.table("companies").select("name, post_count").order("post_count", desc=True).limit(10).execute()
        logs      = db.table("scraper_logs").select("source, started_at, posts_inserted, status").order("started_at", desc=True).limit(5).execute()
        return {
            "totals": {"companies": companies.count, "posts": posts.count, "questions": questions.count},
            "top_companies": top.data,
            "recent_runs": logs.data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting server on 0.0.0.0:{port}")
    uvicorn.run(
        app,                   # Pass app object directly, not string
        host="0.0.0.0",        # MUST be 0.0.0.0 — not localhost or 127.0.0.1
        port=port,
        log_level="info",
    )
