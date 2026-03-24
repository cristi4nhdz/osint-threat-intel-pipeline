# processing/entity_extractor.py
"""Entity extraction for articles using SpaCy model and keyword matching."""

import logging
import re
from pathlib import Path
import spacy
import yaml
from processing.actor_data import (
    ACTOR_NORMALIZE,
    MITRE_ORIGINS,
    MALWARE_BLOCKLIST,
    ORG_BLOCKLIST,
    ACTOR_BLOCKLIST,
    LOCATION_NORMALIZE,
    LOCATION_BLOCKLIST,
)

logger = logging.getLogger(__name__)

THREAT_ACTOR_PATTERN = re.compile(
    r"\b(apt\d+|unc\d+|uat[-\s]?\d+|uac[-\s]?\d+|ta\d+|g\d{4})\b", re.IGNORECASE
)

HTML_TAG_RE = re.compile(r"<[^>]+>")
HTML_ENTITY_RE = re.compile(r"&#?\w+;")

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

ORIGIN_PATTERNS = re.compile(
    r"\b("
    r"chinese|russian|iranian|north korean|pakistani|indian|vietnamese|"
    r"israeli|lebanese|belarusian|turkish|south korean|american|british|"
    r"from|based in|linked to|attributed to|nexus|sponsored by|state[-\s]?sponsored|nation[-\s]?state|"
    r"foreign|domestic|national|transnational"
    r")\b",
    re.IGNORECASE,
)

TARGET_PATTERNS = re.compile(
    r"\b("
    r"target|attack|breach|compromise|espionage|intrusion|hack|exploit|"
    r"against|victim|infected|hit|struck|impacted|affected|campaign against|"
    r"data exfiltration|credential theft|denial of service|ransomware attack|"
    r"security incident|penetration|unauthorized access|system compromise"
    r")\b",
    re.IGNORECASE,
)


def _strip_html(text: str) -> str:
    """Remove HTML tags and entities from text."""
    text = HTML_TAG_RE.sub(" ", text)
    text = HTML_ENTITY_RE.sub(" ", text)
    return text


def _clean_name(name: str) -> str:
    """Strip trailing punctuation, HTML fragments, newlines, and whitespace."""
    name = HTML_TAG_RE.sub("", name)
    name = HTML_ENTITY_RE.sub("", name)
    name = name.replace("\n", " ").replace("\r", " ")
    name = re.sub(r"\s+", " ", name)
    return name.strip().rstrip("'\".,;:!?)(}{[]<>/")


def normalize_actor(name: str) -> str:
    """Normalize threat actor names using ACTOR_NORMALIZE mapping."""
    cleaned = _clean_name(name)
    return ACTOR_NORMALIZE.get(cleaned.lower().strip(), cleaned)


def normalize_location(loc: str) -> str | None:
    """Normalize location names using LOCATION_NORMALIZE, or skip if blocklisted."""
    cleaned = _clean_name(loc)
    lower = cleaned.lower()

    if lower in LOCATION_BLOCKLIST:
        return None

    # skip if still contains HTML fragments after cleaning
    if "<" in cleaned or ">" in cleaned or "&" in cleaned:
        return None

    return LOCATION_NORMALIZE.get(lower, cleaned)


def classify_locations(doc) -> tuple[list[str], list[str]]:
    """Classify locations as origin or target."""

    origins = []
    targets = []
    seen = set()

    for sent in doc.sents:

        sent_text = sent.text
        sent_locs = [e for e in sent.ents if e.label_ in ("GPE", "LOC")]

        for ent in sent_locs:

            normalized = normalize_location(ent.text)

            if not normalized or normalized.lower() in seen:
                continue

            seen.add(normalized.lower())

            start = max(0, ent.start_char - 50)
            end = min(len(sent_text), ent.end_char + 50)
            context = sent_text[start:end]

            has_origin = bool(ORIGIN_PATTERNS.search(context))
            has_target = bool(TARGET_PATTERNS.search(context))

            if has_origin and not has_target:
                origins.append(normalized)

            elif has_target:
                targets.append(normalized)

            else:
                if re.search(r"'s\s+\w+", context, re.IGNORECASE):
                    origins.append(normalized)
                else:
                    targets.append(normalized)

    return list(dict.fromkeys(origins)), list(dict.fromkeys(targets))


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

        raw_text = f"{article.get('title', '')} {article.get('content', '')}"

        text = _strip_html(raw_text)

        doc = self.nlp(text[:5000])

        orgs = [
            e.text
            for e in doc.ents
            if e.label_ == "ORG" and e.text.lower().strip() not in ORG_BLOCKLIST
        ]
        persons = [e.text for e in doc.ents if e.label_ == "PERSON"]

        text_lower = text.lower()

        actor_ids = [m.group(0).upper() for m in THREAT_ACTOR_PATTERN.finditer(text)]

        raw_actors = match_keywords(text_lower, THREAT_ACTOR_HINTS, orgs) + actor_ids

        raw_actors = [_clean_name(a) for a in raw_actors]

        threat_actors = [
            a
            for a in list(dict.fromkeys(normalize_actor(a) for a in raw_actors))
            if a.lower().strip() not in ACTOR_BLOCKLIST
            and len(a) <= 40  # reject parsing artifacts longer than 40 chars
        ]

        origin_locs, target_locs = classify_locations(doc)

        for actor in threat_actors:

            canonical = ACTOR_NORMALIZE.get(actor.lower(), actor)
            known_origin = MITRE_ORIGINS.get(canonical)

            if known_origin:

                norm = normalize_location(known_origin) or known_origin

                if norm in target_locs:
                    target_locs.remove(norm)

                if norm not in origin_locs:
                    origin_locs.append(norm)

        raw_malware = match_keywords(text_lower, MALWARE_HINTS, [])
        malware = [
            m
            for m in raw_malware
            if m.lower().strip() not in MALWARE_BLOCKLIST and len(m.strip()) > 2
        ]
        attack_techniques = match_keywords(text_lower, ATTACK_HINTS, [])
        relevance_score = score_relevance(text_lower)

        return {
            "title": article.get("title", ""),
            "source": article.get("source", ""),
            "original_url": article.get("url", ""),
            "published_at": article.get("published_at", ""),
            "threat_actors": threat_actors,
            "malware": list(dict.fromkeys(malware)),
            "locations": target_locs,
            "origin_locations": origin_locs,
            "persons": list(set(persons)),
            "organizations": list(set(orgs)),
            "attack_techniques": list(set(attack_techniques)),
            "relevance_score": relevance_score,
        }
