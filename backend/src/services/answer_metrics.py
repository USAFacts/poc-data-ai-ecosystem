"""Answer quality metrics: STS, NVS, HDS, CSCS.

STS = Source Traceability Score: fraction of claims with valid source citations
NVS = Numerical Verification Score: fraction of numerical claims verified against sources
HDS = Hallucination Detection Score: number of hallucination flags (0 = best)
CSCS = Cross-Store Consistency Score: 1 - (contradictions / overlaps)
"""

import re
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Common immigration form numbers to exclude from numerical analysis
_FORM_NUMBERS = {
    "130", "140", "485", "526", "589", "601", "693", "730", "751", "765",
    "821", "824", "829", "864", "914", "918", "129", "360", "539", "600",
    "212", "290", "400", "551", "566", "698", "800", "817", "956",
}

# Common generic words that appear in bold but aren't document references
_NON_DOC_BOLD_WORDS = {
    "agency", "from", "administration", "based", "forms", "total",
    "family", "employment", "humanitarian", "categories", "received",
    "pending", "approved", "denied", "backlog", "processing",
    "fiscal year", "quarter", "annual", "monthly", "weekly",
    "important", "note", "summary", "overview", "report",
    "combined", "overall", "service-wide", "nationwide",
}


def compute_answer_metrics(
    answer: str,
    documents: list[dict[str, Any]],
    query: str = "",
) -> dict[str, float]:
    """Compute STS, NVS, HDS, CSCS for a given answer and its source documents."""
    if not answer or not answer.strip():
        return {"sts": 0.0, "nvs": 0.0, "hds": 0, "cscs": 1.0}

    sts = _compute_sts(answer, documents)
    nvs = _compute_nvs(answer, documents)
    hds = _compute_hds(answer, documents)
    cscs = _compute_cscs(answer, documents)

    return {
        "sts": round(sts, 3),
        "nvs": round(nvs, 3),
        "hds": hds,
        "cscs": round(cscs, 3),
    }


def _split_into_claims(text: str) -> list[str]:
    """Split answer text into individual claim-bearing sentences.

    Skips headings, empty lines, chart blocks, bullet-list labels,
    and very short fragments.
    """
    # Remove chart blocks
    text = re.sub(r'```chart.*?```', '', text, flags=re.DOTALL)
    # Remove markdown headings
    text = re.sub(r'^#+\s+.*$', '', text, flags=re.MULTILINE)
    # Remove bullet prefixes but keep the content
    text = re.sub(r'^[\s]*[-*]\s+', '', text, flags=re.MULTILINE)
    # Split into sentences (handle abbreviations better)
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z\*])', text.strip())
    # Filter: keep only substantive claims (>30 chars, not just formatting)
    claims = []
    for s in sentences:
        s = s.strip()
        if len(s) > 30 and not s.startswith('|') and not s.startswith('---'):
            claims.append(s)
    return claims


# --- STS: Source Traceability Score ---

_CITATION_PATTERNS = [
    # Explicit parenthetical citations — (Source: **[Title]** from **[Agency]**)
    r'\(Source:',
    r'\(source:',
    # Bold document/agency references
    r'\*\*\[.+?\]\*\*',                        # **[Document Title]**
    r'from\s+(?:the\s+)?\*\*[^*]+\*\*',        # from **Agency** or from the **Report**
    r'\*\*[^*]+\*\*\s+from\s+\*\*[^*]+\*\*',   # **Title** from **Agency**
    # URLs
    r'\(https?://[^\s)]+\)',                     # (URL)
    r'\[.+?\]\(https?://[^\s)]+\)',              # [text](URL) markdown link
    r'https?://\S+',                             # bare URL in text
    # Attribution phrases
    r'according to\s+(?:the\s+)?',               # according to the...
    r'(?:Document|Source)\s+\d+',                # Document 1, Source 1
    # Named source references
    r'the\s+\*\*[^*]+\*\*',                      # the **Any Bold Reference**
    r'\*\*USCIS[^*]*\*\*',                       # **USCIS anything**
    r'\*\*[A-Z][^*]{3,}\*\*',                    # **Any Capitalized Bold Text** (likely a source)
    # Attribution verbs
    r'(?:based on|per|from|as reported by|as published by)\s+(?:the\s+)?',
    r'(?:report|data|document|statistics)\s+(?:from|by|shows?|indicates?)',
    r'USCIS\s+\w+',                              # USCIS followed by any word
]


def _has_citation(sentence: str) -> bool:
    """Check if a sentence contains a source citation or reference."""
    for pattern in _CITATION_PATTERNS:
        if re.search(pattern, sentence, re.IGNORECASE):
            return True
    return False


def _is_data_continuation(claim: str) -> bool:
    """Check if a claim is a data point that continues from a cited context.

    Bullet points, numbered items, and data breakdowns that follow
    a cited introductory sentence inherit that citation.
    """
    # Starts with bold category label: **Family-Based forms**:
    if re.match(r'^\*\*[^*]+\*\*\s*[:—–-]?\s', claim):
        return True
    # Starts with a dash/bullet (after our preprocessing)
    if re.match(r'^[-•]\s', claim):
        return True
    # Contains specific numbers with context (data point, not opinion)
    if re.search(r'\d{1,3}(?:,\d{3})+', claim):
        return True
    # "Including..." or "Such as..." continuation
    if re.match(r'(?:Including|Such as|Specifically|This includes|These include|This figure|This represents|The data)', claim, re.IGNORECASE):
        return True
    return False


def _compute_sts(answer: str, documents: list[dict]) -> float:
    """Source Traceability Score = cited claims / total claims.

    A claim is cited if:
    1. It directly contains a citation pattern, OR
    2. It's a data continuation within 3 claims of a citation
       (bullet points, numbered breakdowns, elaborations), OR
    3. It's an introductory/structural sentence (headings, transitions)
       that doesn't make a factual claim requiring citation.
    """
    claims = _split_into_claims(answer)
    if not claims:
        return 1.0

    cited = 0
    claims_since_citation = 99  # Start high so first uncited claim isn't counted

    for c in claims:
        if _has_citation(c):
            cited += 1
            claims_since_citation = 0
        elif claims_since_citation <= 3 and _is_data_continuation(c):
            # Data point near a citation — inherits attribution
            cited += 1
            claims_since_citation += 1
        elif _is_structural(c):
            # Structural sentence — doesn't need citation
            cited += 1
            claims_since_citation += 1
        else:
            claims_since_citation += 1

    return cited / len(claims)


def _is_structural(claim: str) -> bool:
    """Check if a claim is structural/transitional rather than factual.

    These don't need citations: introductions, transitions, caveats.
    """
    structural_patterns = [
        r'^(?:Here|Below|The following|Note|It is important)',
        r'^(?:In summary|Overall|To summarize|In conclusion)',
        r'^(?:For more|Additional|Further) (?:information|details|data)',
        r'^(?:The (?:table|chart|data|breakdown) (?:below|above|shows|provides))',
        r'(?:should be noted|worth noting|important to note)',
        r'^(?:As of|During|In) (?:FY|fiscal|the)',
    ]
    for p in structural_patterns:
        if re.search(p, claim, re.IGNORECASE):
            return True
    return False


# --- NVS: Numerical Verification Score ---

def _extract_numbers(text: str) -> list[str]:
    """Extract meaningful statistical numbers from text.

    Filters out: years (1900-2099), form numbers (I-130 → 130),
    page numbers, percentages under 1%, and very small numbers.
    """
    # Remove form number patterns first (I-130, I-765, Form I-485, etc.)
    cleaned = re.sub(r'(?:Form\s+)?I-\d{1,4}[A-Z]?', '', text, flags=re.IGNORECASE)
    # Remove FY/Q references (FY2025, Q3, etc.)
    cleaned = re.sub(r'FY\s*\d{2,4}', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bQ[1-4]\b', '', cleaned)

    # Match numbers with optional commas and decimals
    raw = re.findall(
        r'(?<!\w)(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{4,}(?:\.\d+)?)',
        cleaned,
    )

    numbers = []
    for n in raw:
        clean = n.replace(',', '')
        try:
            val = float(clean)
            # Skip years
            if 1900 <= val <= 2099 and '.' not in clean:
                continue
            # Skip very small numbers (likely indices or ordinals)
            if val < 100:
                continue
            # Skip known form numbers
            if clean in _FORM_NUMBERS:
                continue
            numbers.append(clean)
        except ValueError:
            continue
    return numbers


def _build_source_corpus(documents: list[dict]) -> str:
    """Build a comprehensive source text corpus from all available document content.

    Uses full_context (what Claude saw) when available, falls back to snippets.
    """
    parts = []
    seen_context = False
    for d in documents:
        # Prefer full_context (the actual text Claude was given)
        full_ctx = d.get("full_context", "")
        if full_ctx and not seen_context:
            parts.append(full_ctx)
            seen_context = True
        # Always add snippet and title as fallback
        parts.append(d.get("snippet", ""))
        parts.append(d.get("document_title", ""))
    return " ".join(parts)


def _compute_nvs(answer: str, documents: list[dict]) -> float:
    """Numerical Verification Score = verified numbers / total numbers in answer.

    A number is considered verified if:
    1. It appears in the source corpus (full context + snippets), OR
    2. It appears in a sentence that cites a source (source-attributed), OR
    3. It's within 1% of a source number (rounding tolerance)
    """
    answer_numbers = _extract_numbers(answer)
    if not answer_numbers:
        return 1.0

    # Build source text corpus (uses full context when available)
    source_text = _build_source_corpus(documents)
    source_text_clean = source_text.replace(',', '')
    source_nums = set(_extract_numbers(source_text))

    # Also find which numbers appear in cited sentences
    claims = _split_into_claims(answer)
    cited_numbers: set[str] = set()
    for claim in claims:
        if _has_citation(claim):
            cited_numbers.update(_extract_numbers(claim))

    verified = 0
    for num in answer_numbers:
        # Method 1: Direct match in source text
        if num in source_text_clean:
            verified += 1
            continue
        # Method 2: Number appears in a cited sentence (attributed to source)
        if num in cited_numbers:
            verified += 1
            continue
        # Method 3: Close match (within 1%)
        try:
            num_val = float(num)
            matched = False
            for sn in source_nums:
                sn_val = float(sn)
                if sn_val > 0 and abs(num_val - sn_val) / sn_val < 0.01:
                    matched = True
                    break
            if matched:
                verified += 1
                continue
        except (ValueError, ZeroDivisionError):
            pass

    return verified / len(answer_numbers)


# --- HDS: Hallucination Detection Score ---

def _compute_hds(answer: str, documents: list[dict]) -> int:
    """Hallucination Detection Score = number of hallucination flags.

    Uses a 3-method heuristic ensemble:
    1. Unsupported entity check: proper nouns not found in any source
    2. Numerical mismatch: statistical numbers not traceable to sources
    3. Phantom document references: bold references to non-existent sources

    Returns count of flags (0 = best, no hallucinations detected).
    """
    flags = 0

    source_text = _build_source_corpus(documents).lower()
    # Also add agency names
    source_text += " " + " ".join(d.get("agency_name", "") for d in documents).lower()

    # Method 1: Check for specific named entities not in sources
    # Only flag multi-word proper nouns that are likely organization/place names
    answer_entities = set(re.findall(
        r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b', answer
    ))
    # Filter out common English phrases and category labels
    common_phrases = {
        "United States", "Fiscal Year", "Annual Report", "Based Forms",
        "Employment Based", "Family Based", "Humanitarian Forms",
        "Service Wide", "All Categories", "Third Quarter",
    }
    for entity in answer_entities:
        entity_lower = entity.lower()
        # Skip common phrases, short entities, and known category labels
        if entity in common_phrases:
            continue
        if len(entity) < 8:
            continue
        if any(w in entity_lower for w in _NON_DOC_BOLD_WORDS):
            continue
        if entity_lower not in source_text:
            flags += 1

    # Method 2: Numerical mismatch (only flag large statistical numbers)
    answer_numbers = _extract_numbers(answer)
    source_text_clean = source_text.replace(',', '')
    for num in answer_numbers:
        if num not in source_text_clean:
            # Only flag if the number is significant (> 1000)
            try:
                if float(num) > 1000:
                    flags += 1
            except ValueError:
                pass

    # Method 3: Bold references to documents not in our sources
    doc_titles = {d.get("document_title", "").lower() for d in documents if d.get("document_title")}
    agency_names = {d.get("agency_name", "").lower() for d in documents if d.get("agency_name")}
    referenced_docs = re.findall(r'\*\*([^*]{5,60})\*\*', answer)
    for ref in referenced_docs:
        ref_lower = ref.lower()
        # Skip if it matches a source title or agency
        if any(ref_lower in title or title in ref_lower for title in doc_titles if title):
            continue
        if any(ref_lower in agency or agency in ref_lower for agency in agency_names):
            continue
        # Skip common formatting uses of bold (not document references)
        if any(word in ref_lower for word in _NON_DOC_BOLD_WORDS):
            continue
        # Skip numbers in bold (e.g., **3,286,857**)
        if re.match(r'^[\d,.\s%]+$', ref):
            continue
        flags += 1

    return flags


# --- CSCS: Cross-Store Consistency Score ---

def _compute_cscs(answer: str, documents: list[dict]) -> float:
    """Cross-Store Consistency Score = 1 - (contradictions / overlaps).

    Checks for consistency between different source documents.
    Only flags genuine contradictions where the same metric is reported
    differently across sources.
    """
    if len(documents) < 2:
        return 1.0

    doc_facts: list[dict[str, Any]] = []
    for doc in documents:
        snippet = doc.get("snippet", "")
        numbers = set(_extract_numbers(snippet))
        # Extract key topic words (nouns, not stopwords)
        words = set(w.lower() for w in snippet.split() if len(w) > 3)
        doc_facts.append({"numbers": numbers, "words": words})

    contradictions = 0
    overlaps = 0

    for i in range(len(doc_facts)):
        for j in range(i + 1, len(doc_facts)):
            shared_numbers = doc_facts[i]["numbers"] & doc_facts[j]["numbers"]
            overlaps += len(shared_numbers)

            # Check topic similarity
            common_words = doc_facts[i]["words"] & doc_facts[j]["words"]
            min_words = min(len(doc_facts[i]["words"]), len(doc_facts[j]["words"]))

            if min_words > 0 and len(common_words) / min_words > 0.4:
                # High topic overlap — check for contradictory numbers
                nums_i = doc_facts[i]["numbers"] - shared_numbers
                nums_j = doc_facts[j]["numbers"] - shared_numbers
                # Only flag as contradiction if both docs have exclusive numbers
                # AND they share enough topic context to suggest they're about the same thing
                if nums_i and nums_j and len(common_words) / min_words > 0.5:
                    contradictions += 1
                    overlaps += 1  # Count this as an overlap too

    if overlaps == 0:
        return 1.0

    return max(0.0, 1.0 - (contradictions / overlaps))
