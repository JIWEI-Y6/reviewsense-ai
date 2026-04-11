"""Input and output guardrails for the query pipeline.

Input guardrails:
- Block prompt injection attempts
- Block toxic/harmful content requests
- Block off-topic questions (dynamic keywords from Snowflake + base list)
- Validate ASIN format
- Detect non-English input (warn, don't block)
- Enforce length limits

Output guardrails:
- Strip any leaked PII (emails, phone numbers)
- Prevent hallucinated URLs
"""

import re
import time
import logging

logger = logging.getLogger(__name__)

# ============================================
# INPUT: Prompt injection patterns
# ============================================
INJECTION_PATTERNS = [
    r'ignore (all |your |previous |the above)',
    r'forget (all |your |previous )',
    r'disregard (all |your |previous )',
    r'you are now',
    r'act as',
    r'pretend (to be|you)',
    r'new instructions',
    r'override',
    r'system prompt',
    r'</?(system|assistant|user)',
    r'<\|',
    r'\bDAN\b',
    r'jailbreak',
    r'bypass',
]

# ============================================
# INPUT: Toxicity patterns — block harmful requests
# ============================================
TOXICITY_PATTERNS = [
    r'(write|generate|create|make).*(fake|false|fabricat).*(review|complaint|rating)',
    r'(say|write|generate).*(offensive|racist|sexist|demeaning|hateful|discriminat)',
    r'(insult|attack|defame|slander|harass)',
    r'(hate speech|racial slur|ethnic slur)',
    r'(worst|terrible|stupid).*(race|gender|ethnicity|religion|nationality|people from)',
    r'(how to|ways to).*(manipulat|deceiv|trick|scam|cheat)',
    r'(fake|forge|fabricat).*(data|statistic|number|metric)',
]

# ============================================
# INPUT: Base on-topic keywords (always valid)
# These are generic review/product terms that don't depend on what's in the database
# ============================================
BASE_ON_TOPIC_KEYWORDS = [
    'review', 'rating', 'sentiment', 'product', 'category', 'brand',
    'complaint', 'theme', 'quality', 'customer', 'asin',
    'negative', 'positive', 'worst', 'best', 'recommend', 'buy', 'worth',
    'compare', 'comparison', 'analysis', 'trend', 'monthly',
    'average', 'count', 'total', 'price', 'value', 'feature',
    'people say', 'people think', 'what about', 'how is', 'tell me',
    'suggest', 'find', 'search', 'show', 'list',
    'electronic', 'device', 'accessory', 'wireless', 'bluetooth',
]

# ============================================
# INPUT: Dynamic keywords — loaded from Snowflake at startup
# Auto-updates when new categories/brands are added
# ============================================
_dynamic_keywords = {
    "categories": set(),
    "brands": set(),
    "themes": set(),
    "loaded_at": 0,
    "ttl": 3600,  # 1 hour cache
}


def load_dynamic_keywords():
    """Query Snowflake for current categories, brands, themes. Cache 1 hour."""
    now = time.time()
    if now - _dynamic_keywords["loaded_at"] < _dynamic_keywords["ttl"]:
        return  # Cache still valid

    try:
        from api.db import get_cursor

        with get_cursor() as cur:
            # Categories
            cur.execute("SELECT DISTINCT DERIVED_CATEGORY FROM GOLD.CATEGORY_SENTIMENT_SUMMARY")
            _dynamic_keywords["categories"] = {r[0].lower() for r in cur.fetchall() if r[0]}

            # Brands (top 1000 by review count to avoid loading 5K+ brands)
            cur.execute("""
                SELECT DISTINCT BRAND FROM CURATED.PRODUCT_METADATA
                WHERE BRAND IS NOT NULL AND BRAND != ''
                LIMIT 1000
            """)
            _dynamic_keywords["brands"] = {r[0].lower() for r in cur.fetchall() if r[0]}

            # Themes
            cur.execute("SELECT DISTINCT REVIEW_THEME FROM GOLD.THEME_CATEGORY_ANALYSIS")
            _dynamic_keywords["themes"] = {r[0].lower().replace('_', ' ') for r in cur.fetchall() if r[0]}

        _dynamic_keywords["loaded_at"] = now
        logger.info(
            f"Dynamic keywords loaded: {len(_dynamic_keywords['categories'])} categories, "
            f"{len(_dynamic_keywords['brands'])} brands, {len(_dynamic_keywords['themes'])} themes"
        )
    except Exception as e:
        logger.warning(f"Failed to load dynamic keywords: {e}. Using base keywords only.")


def _is_on_topic(question_lower: str) -> bool:
    """Check if question is on-topic using base + dynamic keywords."""
    # Load/refresh dynamic keywords
    load_dynamic_keywords()

    # Check base keywords
    if any(kw in question_lower for kw in BASE_ON_TOPIC_KEYWORDS):
        return True

    # Check dynamic categories (e.g., "headphones_earbuds" → also match "headphones" and "earbuds")
    for cat in _dynamic_keywords["categories"]:
        for part in cat.split('_'):
            if part in question_lower:
                return True

    # Check dynamic brands (e.g., "logitech", "sony")
    for brand in _dynamic_keywords["brands"]:
        if brand in question_lower and len(brand) > 2:  # Skip very short brand names
            return True

    # Check dynamic themes (e.g., "battery life", "sound quality")
    for theme in _dynamic_keywords["themes"]:
        if theme in question_lower:
            return True

    return False


# ============================================
# OUTPUT: PII patterns to strip
# ============================================
PII_PATTERNS = [
    (r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL_REDACTED]'),
    (r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '[PHONE_REDACTED]'),
    (r'\b\d{3}-\d{2}-\d{4}\b', '[SSN_REDACTED]'),
    (r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b', '[CARD_REDACTED]'),
]

URL_PATTERN = re.compile(r'https?://\S+')


# ============================================
# SAFETY INSTRUCTION — added to all COMPLETE prompts
# ============================================
SAFETY_INSTRUCTION = (
    "SAFETY: Refuse to generate content that is offensive, discriminatory, defamatory, "
    "or that targets individuals or groups based on race, gender, ethnicity, religion, "
    "or nationality. If asked to generate fake reviews, manipulate data, or produce "
    "harmful content, respond: 'I can only provide factual product review analysis.' "
    "If quoting reviews with offensive language, paraphrase rather than quote directly."
)


class GuardrailError(Exception):
    """Raised when a guardrail blocks the request."""
    def __init__(self, message: str, guardrail: str):
        self.message = message
        self.guardrail = guardrail
        super().__init__(message)


def check_input(question: str, has_conversation_history: bool = False) -> None:
    """Validate user input. Raises GuardrailError if blocked."""

    # Length check
    if len(question.strip()) < 3:
        raise GuardrailError("Question is too short.", "input_length")
    if len(question) > 1000:
        raise GuardrailError("Question exceeds 1000 character limit.", "input_length")

    q_lower = question.lower()

    # Prompt injection check (always runs, even mid-conversation)
    for pattern in INJECTION_PATTERNS:
        if re.search(pattern, q_lower):
            raise GuardrailError(
                "Your question could not be processed. Please ask a question about product reviews.",
                "prompt_injection"
            )

    # Toxicity check (always runs, even mid-conversation)
    for pattern in TOXICITY_PATTERNS:
        if re.search(pattern, q_lower):
            raise GuardrailError(
                "This request contains content that violates our usage policy. "
                "This system provides factual product review analysis only.",
                "toxicity"
            )

    # Language check (warn, don't block)
    if len(question) > 5:
        ascii_ratio = sum(1 for c in question if c.isascii()) / len(question)
        if ascii_ratio < 0.6:
            logger.warning(f"Non-English input detected (ASCII ratio: {ascii_ratio:.1%}): {question[:50]}")
            # Don't block — just log. Some valid questions have non-ASCII brand names.

    # Off-topic check — skip if mid-conversation (follow-ups are inherently on-topic)
    if has_conversation_history:
        return

    # ASIN check — any ASIN reference makes it on-topic
    has_asin = bool(re.search(r'\bB0[A-Z0-9]{8,}\b', question))
    if has_asin:
        return

    # Dynamic + base keyword check
    if not _is_on_topic(q_lower):
        raise GuardrailError(
            "This system answers questions about Amazon Electronics product reviews. "
            "Please ask about product categories, review themes, sentiment, ratings, or specific products.",
            "off_topic"
        )


def sanitize_output(text: str) -> str:
    """Clean the LLM output of PII and hallucinated URLs."""
    if not text:
        return text

    # Strip PII
    for pattern, replacement in PII_PATTERNS:
        text = re.sub(pattern, replacement, text)

    # Strip hallucinated URLs
    text = URL_PATTERN.sub('[URL_REMOVED]', text)

    return text
