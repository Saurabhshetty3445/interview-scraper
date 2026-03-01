"""
main.py - FastAPI app for Railway deployment
Exposes health check + manual trigger endpoints
Runs the scheduler in a background thread
"""
import threading
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from utils.logger import log
from jobs.scheduler import start_scheduler, run_all_scrapers
from scrapers.reddit_scraper import run_reddit_scraper
from scrapers.leetcode_scraper import run_leetcode_scraper
from database.db import get_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start scheduler in background thread when app starts."""
    log.info("Interview Scraper API starting...")
    scheduler_thread = threading.Thread(
        target=start_scheduler,
        kwargs={"run_immediately": True},
        daemon=True
    )
    scheduler_thread.start()
    log.info("Scheduler started in background")
    yield
    log.info("Interview Scraper API shutting down")


app = FastAPI(
    title="Interview Scraper API",
    description="Scrapes LeetCode & Reddit for interview experiences",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "Interview Scraper",
        "version": "1.0.0",
        "endpoints": ["/health", "/trigger/all", "/trigger/reddit", "/trigger/leetcode", "/stats"],
    }


@app.get("/health")
async def health():
    """Railway health check endpoint."""
    try:
        db = get_client()
        db.table("companies").select("id").limit(1).execute()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}")


@app.post("/trigger/all")
async def trigger_all(background_tasks: BackgroundTasks):
    """Manually trigger all scrapers."""
    background_tasks.add_task(run_all_scrapers)
    return {"status": "triggered", "message": "All scrapers started in background"}


@app.post("/trigger/reddit")
async def trigger_reddit(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_reddit_scraper)
    return {"status": "triggered", "message": "Reddit scraper started"}


@app.post("/trigger/leetcode")
async def trigger_leetcode(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_leetcode_scraper)
    return {"status": "triggered", "message": "LeetCode scraper started"}


@app.get("/stats")
async def get_stats():
    """Return high-level stats from the database."""
    try:
        db = get_client()

        companies_res = db.table("companies").select("id", count="exact").execute()
        posts_res = db.table("posts").select("id", count="exact").execute()
        questions_res = db.table("questions").select("id", count="exact").execute()

        # Top companies
        top_companies = db.table("companies")\
            .select("name, post_count")\
            .order("post_count", desc=True)\
            .limit(10)\
            .execute()

        # Recent scraper logs
        recent_logs = db.table("scraper_logs")\
            .select("source, started_at, posts_inserted, status")\
            .order("started_at", desc=True)\
            .limit(5)\
            .execute()

        return {
            "totals": {
                "companies": companies_res.count,
                "posts": posts_res.count,
                "questions": questions_res.count,
            },
            "top_companies": top_companies.data,
            "recent_runs": recent_logs.data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
