"""
main.py - FastAPI app for Railway deployment
"""
import threading
import os
import time
import uvicorn
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Interview Scraper API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/")
async def root():
    return {"status": "online", "service": "Interview Scraper", "version": "1.0.0"}


@app.get("/debug")
async def debug():
    results = {}
    results["env"] = {
        "SUPABASE_URL":   "✅ set" if os.getenv("SUPABASE_URL") else "❌ MISSING",
        "SUPABASE_KEY":   "✅ set" if os.getenv("SUPABASE_SERVICE_KEY") else "❌ MISSING",
        "OPENROUTER_KEY": "✅ set" if os.getenv("OPENROUTER_API_KEY") else "❌ MISSING",
        "PORT":           os.getenv("PORT", "8000"),
    }
    try:
        from database.db import get_client
        db  = get_client()
        res = db.table("posts").select("id", count="exact").execute()
        results["supabase"] = f"✅ connected — {res.count} posts in DB"
    except Exception as e:
        results["supabase"] = f"❌ {e}"
    try:
        import httpx
        r = httpx.get("https://www.reddit.com/r/cscareerquestions/new.json?limit=1",
                      headers={"User-Agent": "InterviewBot/1.0"}, timeout=10)
        results["reddit_api"] = f"✅ HTTP {r.status_code}"
    except Exception as e:
        results["reddit_api"] = f"❌ {e}"
    try:
        import httpx
        r = httpx.get("https://leetcode.com/discuss/interview-experience/",
                      headers={"User-Agent": "Mozilla/5.0"}, timeout=10, follow_redirects=True)
        results["leetcode_reachable"] = f"✅ HTTP {r.status_code}"
    except Exception as e:
        results["leetcode_reachable"] = f"❌ {e}"
    return results


@app.post("/trigger/all")
async def trigger_all(
    background_tasks: BackgroundTasks,
    fetch_old: bool = Query(default=False, description="Also scrape historical posts")
):
    from scrapers.leetcode_scraper import run_leetcode_scraper
    from scrapers.reddit_scraper import run_reddit_scraper
    def run_both():
        run_leetcode_scraper(fetch_old=fetch_old)
        run_reddit_scraper(fetch_old=fetch_old)
    background_tasks.add_task(run_both)
    return {"status": "triggered", "fetch_old": fetch_old}


@app.post("/trigger/reddit")
async def trigger_reddit(
    background_tasks: BackgroundTasks,
    fetch_old: bool = Query(default=False)
):
    from scrapers.reddit_scraper import run_reddit_scraper
    background_tasks.add_task(run_reddit_scraper, fetch_old)
    return {"status": "triggered", "source": "reddit", "fetch_old": fetch_old}


@app.post("/trigger/leetcode")
async def trigger_leetcode(
    background_tasks: BackgroundTasks,
    fetch_old: bool = Query(default=False)
):
    from scrapers.leetcode_scraper import run_leetcode_scraper
    background_tasks.add_task(run_leetcode_scraper, fetch_old)
    return {"status": "triggered", "source": "leetcode", "fetch_old": fetch_old}


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
            "recent_runs":   db.table("scraper_logs").select("source, started_at, posts_inserted, status, error_message").order("started_at", desc=True).limit(10).execute().data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.on_event("startup")
async def startup():
    def delayed():
        time.sleep(15)
        try:
            from jobs.scheduler import start_scheduler
            start_scheduler(run_immediately=True)
        except Exception as e:
            print(f"Scheduler error: {e}")
    threading.Thread(target=delayed, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"🚀 Starting on 0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
