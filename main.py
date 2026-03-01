"""
main.py - FastAPI app for Railway deployment
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
    """Start scheduler AFTER server is ready — never block startup."""
    log.info("Interview Scraper API starting...")

    # Delay scheduler start by 10s so health check passes first
    def delayed_start():
        import time
        time.sleep(10)
        start_scheduler(run_immediately=True)

    scheduler_thread = threading.Thread(target=delayed_start, daemon=True)
    scheduler_thread.start()
    log.info("Scheduler will start in 10 seconds...")
    yield
    log.info("Shutting down...")


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
    }


@app.get("/health")
async def health():
    """
    Railway health check — must respond fast with 200.
    Does NOT check DB so startup is never blocked.
    """
    return {"status": "healthy"}


@app.get("/health/db")
async def health_db():
    """Deep health check including DB — call manually to verify DB connection."""
    try:
        db = get_client()
        db.table("companies").select("id").limit(1).execute()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}")


@app.post("/trigger/all")
async def trigger_all(background_tasks: BackgroundTasks):
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
    try:
        db = get_client()
        companies_res = db.table("companies").select("id", count="exact").execute()
        posts_res     = db.table("posts").select("id", count="exact").execute()
        questions_res = db.table("questions").select("id", count="exact").execute()
        top_companies = db.table("companies").select("name, post_count").order("post_count", desc=True).limit(10).execute()
        recent_logs   = db.table("scraper_logs").select("source, started_at, posts_inserted, status").order("started_at", desc=True).limit(5).execute()
        return {
            "totals": {
                "companies": companies_res.count,
                "posts":     posts_res.count,
                "questions": questions_res.count,
            },
            "top_companies": top_companies.data,
            "recent_runs":   recent_logs.data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    log.info(f"Starting server on 0.0.0.0:{port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
