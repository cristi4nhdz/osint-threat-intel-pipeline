# processing/entity_extractor.py
"""Entity extraction for articles using SpaCy model and keyword matching."""

import logging
import re
from pathlib import Path
import spacy
import yaml

logger = logging.getLogger(__name__)

THREAT_ACTOR_PATTERN = re.compile(
    r"\b(apt\d+|unc\d+|uat[-\s]?\d+|uac[-\s]?\d+|ta\d+|g\d{4})\b", re.IGNORECASE
)

KEYWORDS_PATH = Path(__file__).parent / "keywords.yaml"

try:
    with open(KEYWORDS_PATH, encoding="utf-8") as f:
        keywords: dict = yaml.safe_load(f)

    THREAT_ACTOR_HINTS: list[str] = list(keywords["threat_actor_hints"])
    MALWARE_HINTS: list[str] = list(keywords["malware_hints"])
    ATTACK_HINTS: list[str] = list(keywords["attack_hints"])
    HIGH_SIGNAL_TERMS: list[str] = list(keywords["high_signal_terms"])

except (FileNotFoundError, KeyError) as e:
    logger.error("Failed to load keywords.yaml: %s", e)
    raise RuntimeError(f"Failed to load keywords.yaml: {e}") from e


def match_keywords(text: str, hints: list[str], extras: list[str]) -> list[str]:
    """Search raw text and SpaCy entities for matching keywords."""

    matched = [h for h in hints if h in text]
    matched += [e for e in extras if any(h in e.lower() for h in hints)]
    return matched


def score_relevance(text: str) -> float:
    """Return a relevance score between 0.0 and 1.0."""

    high_hits = sum(2 for t in HIGH_SIGNAL_TERMS if t in text)
    actor_hits = len(THREAT_ACTOR_PATTERN.findall(text)) * 2
    normal_hits = sum(1 for h in ATTACK_HINTS + MALWARE_HINTS if h in text)

    total = high_hits + actor_hits + normal_hits
    return round(min(total / 10, 1.0), 2)


class EntityExtractor:
    """Extract entities from raw article data."""

    def __init__(self) -> None:
        """Load the SpaCy transformer model."""

        logger.info("Loading SpaCy model")
        self.nlp = spacy.load("en_core_web_trf")
        logger.info("SpaCy model loaded")

    def extract(self, article: dict) -> dict:
        """Extract entities from an article and return a structured result dict."""

        text = f"{article.get('title', '')} {article.get('content', '')}"
        doc = self.nlp(text[:5000])

        orgs = [e.text for e in doc.ents if e.label_ == "ORG"]
        locs = [e.text for e in doc.ents if e.label_ in ("GPE", "LOC")]
        persons = [e.text for e in doc.ents if e.label_ == "PERSON"]

        text_lower = text.lower()

        actor_ids = [m.group(0).upper() for m in THREAT_ACTOR_PATTERN.finditer(text)]

        threat_actors = list(
            set(match_keywords(text_lower, THREAT_ACTOR_HINTS, orgs) + actor_ids)
        )
        malware = match_keywords(text_lower, MALWARE_HINTS, [])
        attack_techniques = match_keywords(text_lower, ATTACK_HINTS, [])
        relevance_score = score_relevance(text_lower)

        return {
            "title": article.get("title", ""),
            "source": article.get("source", ""),
            "original_url": article.get("url", ""),
            "published_at": article.get("published_at", ""),
            "threat_actors": threat_actors,
            "malware": list(set(malware)),
            "locations": list(set(locs)),
            "persons": list(set(persons)),
            "organizations": list(set(orgs)),
            "attack_techniques": list(set(attack_techniques)),
            "relevance_score": relevance_score,
        }
