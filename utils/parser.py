"""
utils/parser.py - Parse post titles and extract metadata intelligently
"""
import re
from typing import Optional
from config import KNOWN_COMPANIES, CATEGORIES


# Maps title keywords → internal category names
CATEGORY_KEYWORDS = {
    "frontend":      ["frontend", "front-end", "front end", "react", "vue", "angular", "css", "html", "javascript", "ui"],
    "backend":       ["backend", "back-end", "back end", "api", "server", "django", "node", "spring", "rails"],
    "system_design": ["system design", "design interview", "architecture", "distributed", "hld", "lld"],
    "coding":        ["coding", "leetcode", "dsa", "data structure", "algorithm", "oa", "online assessment"],
    "devops":        ["devops", "sre", "infrastructure", "kubernetes", "docker", "cloud", "aws", "gcp", "azure", "ci/cd"],
    "behavioral":    ["behavioral", "behaviour", "hr round", "culture fit", "leadership"],
    "data_structures": ["data structures", "graphs", "trees", "dp", "dynamic programming", "binary search"],
}

EXPERIENCE_PATTERNS = [
    r"(\d+)\s*(?:yoe|years? of exp|years? exp|yr exp)",
    r"(\d+)\s*\+?\s*years?",
    r"senior\+?",
    r"junior",
    r"mid[\s-]?level",
    r"staff",
    r"principal",
    r"lead",
    r"entry[\s-]?level",
    r"new grad",
    r"intern",
]

RESULT_KEYWORDS = {
    "hire":    ["hire", "hired", "got offer", "offer received", "accepted"],
    "no_hire": ["no hire", "rejected", "rejection", "failed", "didn't get", "did not get"],
}


def extract_company(title: str) -> Optional[str]:
    """
    Extract company name from a title.
    Priority:
      1. Known company list (case-insensitive exact match)
      2. Pattern: 'CompanyName Interview' at start of title
    """
    title_lower = title.lower()

    # Check known companies first
    for company in KNOWN_COMPANIES:
        if re.search(rf'\b{re.escape(company.lower())}\b', title_lower):
            return company

    # Fallback: grab the first word(s) before "interview"
    match = re.match(r'^([A-Z][a-zA-Z0-9]+(?:\s+[A-Z][a-zA-Z0-9]+)?)\s+interview', title, re.IGNORECASE)
    if match:
        candidate = match.group(1).strip()
        # Exclude generic words
        if candidate.lower() not in {"my", "the", "a", "an", "tech", "software", "it"}:
            return candidate

    return None


def extract_category(title: str, content: str = "") -> str:
    """Classify post into a category based on title + content keywords."""
    combined = (title + " " + content[:500]).lower()

    scores = {cat: 0 for cat in CATEGORIES}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in combined:
                scores[cat] += 1

    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "other"


def extract_experience(title: str) -> Optional[str]:
    """Extract YOE or seniority level from title."""
    title_lower = title.lower()

    yoe_match = re.search(r'(\d+)\s*(?:yoe|years? of exp|years? exp|yr exp|years?)', title_lower)
    if yoe_match:
        return f"{yoe_match.group(1)} YOE"

    for level in ["senior+", "senior", "staff", "principal", "lead", "junior", "mid-level", "new grad", "intern"]:
        if level in title_lower:
            return level.title()

    return None


def extract_role(title: str) -> Optional[str]:
    """Extract job role from title."""
    role_patterns = [
        r'\b(software engineer|swe|sde|senior engineer|staff engineer|principal engineer)\b',
        r'\b(frontend engineer|backend engineer|full[\s-]?stack)\b',
        r'\b(data engineer|ml engineer|ai engineer|devops engineer|sre)\b',
        r'\b(product manager|pm|tpm|engineering manager|em)\b',
        r'\b(data scientist|research scientist|research engineer)\b',
    ]
    title_lower = title.lower()
    for pattern in role_patterns:
        match = re.search(pattern, title_lower)
        if match:
            return match.group(1).strip().title()
    return None


def extract_result(title: str) -> str:
    """Detect interview result from title."""
    title_lower = title.lower()
    for result, keywords in RESULT_KEYWORDS.items():
        for kw in keywords:
            if kw in title_lower:
                return result
    return "unknown"


def parse_title(title: str, content: str = "") -> dict:
    """
    Full metadata extraction from a post title + optional content preview.
    Returns a dict with: company, category, role, experience_level, interview_result
    """
    return {
        "company":           extract_company(title),
        "category":          extract_category(title, content),
        "role":              extract_role(title),
        "experience_level":  extract_experience(title),
        "interview_result":  extract_result(title),
    }
