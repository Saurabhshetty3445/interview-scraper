"""
config.py - Central configuration loaded from environment variables
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# ── Reddit ────────────────────────────────────────────────────────────────────
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "InterviewScraper/1.0")

# ── OpenRouter ────────────────────────────────────────────────────────────────
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-oss-120b:free")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# ── Scraper ───────────────────────────────────────────────────────────────────
SCRAPE_LIMIT_LEETCODE = int(os.getenv("SCRAPE_LIMIT_LEETCODE", "50"))
SCRAPE_LIMIT_REDDIT = int(os.getenv("SCRAPE_LIMIT_REDDIT", "25"))
AI_BATCH_SIZE = int(os.getenv("AI_BATCH_SIZE", "5"))
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "1.5"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ── Reddit subreddits to scrape ───────────────────────────────────────────────
REDDIT_SUBREDDITS = [
    "cscareerquestions",
    "leetcode",
    "ITCareerQuestions",
    "codinginterview",
    "InterviewCoderHQ",
]

# ── Interview categories ──────────────────────────────────────────────────────
CATEGORIES = [
    "coding",
    "system_design",
    "frontend",
    "backend",
    "devops",
    "behavioral",
    "data_structures",
    "other",
]

# ── Known companies for fast parsing ─────────────────────────────────────────
KNOWN_COMPANIES = [
    "Google", "Amazon", "Meta", "Facebook", "Apple", "Microsoft", "Netflix",
    "Yandex", "Uber", "Lyft", "Airbnb", "Stripe", "Twitter", "X",
    "LinkedIn", "Salesforce", "Oracle", "IBM", "Adobe", "Shopify",
    "Atlassian", "Twilio", "Datadog", "Snowflake", "Databricks",
    "Palantir", "Coinbase", "Robinhood", "DoorDash", "Instacart",
    "ByteDance", "TikTok", "Tencent", "Alibaba", "Baidu", "Samsung",
    "Intel", "Nvidia", "AMD", "Qualcomm", "Cisco", "VMware",
    "ServiceNow", "Workday", "SAP", "Zoom", "Slack", "HubSpot",
    "Square", "Block", "PayPal", "eBay", "Walmart", "Target",
    "JPMorgan", "Goldman Sachs", "Morgan Stanley", "Bloomberg",
    "Two Sigma", "Jane Street", "Citadel", "DE Shaw",
]

# Keywords that suggest interview experience posts
INTERVIEW_KEYWORDS = [
    "interview", "onsite", "phone screen", "offer", "rejected", "hired",
    "no hire", "hire", "oa", "online assessment", "virtual onsite",
    "loop", "system design", "coding round", "behavioral round",
    "yoe", "years of experience", "tc", "total compensation",
]
