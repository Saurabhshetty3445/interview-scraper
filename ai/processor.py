"""
ai/processor.py - OpenRouter AI integration
- Cleans messy scraped content
- Extracts structured questions
- Classifies category / company
- Generates summaries
- Filters spam
"""
from __future__ import annotations
import json
import time
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import OPENROUTER_API_KEY, OPENROUTER_MODEL, OPENROUTER_URL, RATE_LIMIT_DELAY
from utils.logger import log


HEADERS = {
    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
    "Content-Type": "application/json",
    "HTTP-Referer": "https://interview-scraper.railway.app",
    "X-Title": "Interview Scraper",
}


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
)
def _call_openrouter(messages: list[dict], max_tokens: int = 1000) -> str:
    """Raw call to OpenRouter API. Returns assistant message text."""
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": messages,
        "max_tokens": max_tokens,
        "reasoning": {"enabled": True},
    }
    resp = requests.post(OPENROUTER_URL, headers=HEADERS, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"].get("content") or ""


def process_post(post: dict) -> dict:
    """
    Full AI processing pipeline for a single post.
    Returns enriched dict with: cleaned_content, ai_summary, tags, questions, category, company
    """
    title = post.get("title", "")
    raw = (post.get("raw_content") or "")[:3000]  # Truncate for token limits

    if not raw.strip():
        return {"cleaned_content": "", "ai_summary": "", "tags": [], "questions": [], "category": "other", "company": None}

    prompt = f"""You are an expert interview experience analyst. Analyze this interview post and return ONLY valid JSON.

Post Title: {title}
Post Content: {raw}

Extract and return this exact JSON structure:
{{
  "is_interview_post": true/false,
  "company": "company name or null",
  "category": "one of: coding, system_design, frontend, backend, behavioral, devops, data_structures, other",
  "role": "job role or null",
  "experience_level": "e.g. '3 YOE', 'Senior', 'New Grad' or null",
  "interview_result": "hire/no_hire/unknown",
  "cleaned_content": "clean version of the post removing spam, ads, irrelevant text",
  "ai_summary": "2-3 sentence summary of the interview experience",
  "tags": ["tag1", "tag2"],
  "questions": [
    {{
      "question_text": "the actual interview question",
      "question_type": "coding/system_design/behavioral/other",
      "difficulty": "easy/medium/hard/unknown"
    }}
  ]
}}

Rules:
- Only include real interview questions in the questions array
- Remove any spam, self-promotion, or irrelevant content
- Set is_interview_post to false if this is not a genuine interview experience
- Keep questions array empty if no specific questions are mentioned
- Return ONLY the JSON, no markdown, no explanation"""

    try:
        time.sleep(RATE_LIMIT_DELAY)
        raw_response = _call_openrouter([{"role": "user", "content": prompt}], max_tokens=1000)

        # Strip potential markdown code fences
        clean = raw_response.strip()
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
            clean = clean.strip()

        result = json.loads(clean)

        # Validate it's actually an interview post
        if not result.get("is_interview_post", True):
            log.info(f"AI classified as non-interview post: {title[:60]}")
            return None

        return {
            "company":          result.get("company"),
            "category":         result.get("category", "other"),
            "role":             result.get("role"),
            "experience_level": result.get("experience_level"),
            "interview_result": result.get("interview_result", "unknown"),
            "cleaned_content":  result.get("cleaned_content", raw),
            "ai_summary":       result.get("ai_summary", ""),
            "tags":             result.get("tags", []),
            "questions":        result.get("questions", []),
        }

    except json.JSONDecodeError as e:
        log.warning(f"AI returned invalid JSON for '{title[:50]}': {e}")
        return {
            "company": None, "category": "other", "role": None,
            "experience_level": None, "interview_result": "unknown",
            "cleaned_content": raw, "ai_summary": "", "tags": [], "questions": [],
        }
    except Exception as e:
        log.error(f"AI processing failed for '{title[:50]}': {e}")
        return None


def batch_process_posts(posts: list[dict]) -> list[tuple[str, dict]]:
    """
    Process a batch of posts through AI.
    Returns list of (post_id, ai_result) tuples where ai_result may be None (spam).
    """
    results = []
    for post in posts:
        log.info(f"AI processing: {post.get('title', '')[:60]}")
        result = process_post(post)
        results.append((post["id"], result))
    return results
