"""
main.py - FastAPI app for Railway deployment
"""
import threading
import os
import time
import traceback

import uvicorn
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Interview Scraper API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── Health — instant response, no deps ───────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/")
async def root():
    return {"status": "online", "service": "Interview Scraper"}


# ── Debug — shows exactly what's broken ──────────────────────────────────────
@app.get("/debug")
async def debug():
    """Call this after deploy to diagnose issues."""
    results = {}

    # 1. Check env vars
    results["env"] = {
        "SUPABASE_URL":     "✅ set" if os.getenv("SUPABASE_URL") else "❌ MISSING",
        "SUPABASE_KEY":     "✅ set" if os.getenv("SUPABASE_SERVICE_KEY") else "❌ MISSING",
        "OPENROUTER_KEY":   "✅ set" if os.getenv("OPENROUTER_API_KEY") else "❌ MISSING",
        "PORT":             os.getenv("PORT", "8000 (default)"),
    }

    # 2. Check Supabase connection
    try:
        from database.db import get_client
        db = get_client()
        res = db.table("companies").select("id").limit(1).execute()
        results["supabase"] = "✅ connected"
    except Exception as e:
        results["supabase"] = f"❌ {str(e)}"

    # 3. Check Playwright / Chromium
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            browser.close()
        results["playwright"] = "✅ Chromium OK"
    except Exception as e:
        results["playwright"] = f"❌ {str(e)}"

    # 4. Check OpenRouter API key works
    try:
        import requests
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {os.getenv('OPENROUTER_API_KEY', '')}",
                     "Content-Type": "application/json"},
            json={"model": os.getenv("OPENROUTER_MODEL", "openai/gpt-oss-120b:free"),
                  "messages": [{"role": "user", "content": "Say OK"}],
                  "max_tokens": 5},
            timeout=10,
        )
        results["openrouter"] = "✅ API key valid" if resp.status_code == 200 else f"❌ HTTP {resp.status_code}: {resp.text[:100]}"
    except Exception as e:
        results["openrouter"] = f"❌ {str(e)}"

    # 5. Check LeetCode API
    try:
        import httpx
        r = httpx.get("https://leetcode.com/discuss/interview-experience/", timeout=10,
                      headers={"User-Agent": "Mozilla/5.0"}, follow_redirects=True)
        results["leetcode_reachable"] = f"✅ HTTP {r.status_code}"
    except Exception as e:
        results["leetcode_reachable"] = f"❌ {str(e)}"

    # 6. DB table row counts
    try:
        from database.db import get_client
        db = get_client()
        results["db_counts"] = {
            "companies": db.table("companies").select("id", count="exact").execute().count,
            "posts":     db.table("posts").select("id", count="exact").execute().count,
            "questions": db.table("questions").select("id", count="exact").execute().count,
        }
    except Exception as e:
        results["db_counts"] = f"❌ {str(e)}"

    return results


# ── Manual triggers ───────────────────────────────────────────────────────────
@app.post("/trigger/leetcode")
async def trigger_leetcode(background_tasks: BackgroundTasks):
    from scrapers.leetcode_scraper import run_leetcode_scraper
    background_tasks.add_task(run_leetcode_scraper)
    return {"status": "triggered", "source": "leetcode"}

@app.post("/trigger/reddit")
async def trigger_reddit(background_tasks: BackgroundTasks):
    from scrapers.reddit_scraper import run_reddit_scraper
    background_tasks.add_task(run_reddit_scraper)
    return {"status": "triggered", "source": "reddit"}

@app.post("/trigger/all")
async def trigger_all(background_tasks: BackgroundTasks):
    from scrapers.leetcode_scraper import run_leetcode_scraper
    from scrapers.reddit_scraper import run_reddit_scraper
    def run_both():
        run_leetcode_scraper()
        run_reddit_scraper()
    background_tasks.add_task(run_both)
    return {"status": "triggered", "source": "all"}


# ── Stats ─────────────────────────────────────────────────────────────────────
@app.get("/stats")
async def get_stats():
    try:
        from database.db import get_client
        db = get_client()
        return {
            "totals": {
                "companies": db.table("companies").select("id", count="exact").execute().count,
                "posts":     db.table("posts").select("id", count="exact").execute().count,
                "questions": db.table("questions").select("id", count="exact").execute().count,
            },
            "top_companies": db.table("companies").select("name, post_count").order("post_count", desc=True).limit(10).execute().data,
            "recent_runs":   db.table("scraper_logs").select("source, started_at, posts_inserted, status").order("started_at", desc=True).limit(5).execute().data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Scheduler ─────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    def delayed():
        time.sleep(15)
        try:
            from jobs.scheduler import start_scheduler
            start_scheduler(run_immediately=True)
        except Exception as e:
            print(f"Scheduler start error: {e}")
    threading.Thread(target=delayed, daemon=True).start()


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"🚀 Starting on 0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
