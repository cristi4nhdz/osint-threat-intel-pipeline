# services/llm_client.py
"""LLM client for grounded threat analysis via Ollama."""

import json
import logging
import re
from ollama import Client
from config.config_loader import load_config

logger = logging.getLogger(__name__)


class LLMClient:
    """Handles grounded generation with an Ollama-hosted model."""

    def __init__(self) -> None:
        """Initialize Ollama client with configuration."""
        config = load_config()
        self.host = config["ollama"]["host"]
        self.model = config["ollama"]["model"]
        self.client = Client(host=self.host)

    def _build_context(self, query: str, results: list[dict]) -> str:
        """Build a grounded context block from retrieved results."""
        blocks = []
        for idx, item in enumerate(results, start=1):
            blocks.append(f"""[Source {idx}]
Title/Content: {item.get("text", "")}
Publisher: {item.get("source", "Unknown")}
URL: {item.get("url", "")}
Threat Actors: {", ".join(item.get("actors", [])) or "None"}
Malware: {", ".join(item.get("malware", [])) or "None"}
Match Label: {item.get("match_label", "mentioned")}
Similarity: {item.get("similarity", 0.0)}
""")

        return f"""User Query:
{query}

Retrieved Threat Intelligence:
{chr(10).join(blocks)}
"""

    def _extract_json(self, text: str) -> dict:
        """Best-effort JSON extraction from model output."""
        text = text.strip()

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

        return {
            "summary": text,
            "recent_activity": [],
            "why_it_matters": "",
            "key_entities": {
                "actors": [],
                "malware": [],
                "locations": [],
                "sources": [],
            },
        }

    def _normalize_string_list(self, values: object) -> list[str]:
        """Normalize a mixed list into a list of strings."""
        if values is None:
            return []

        if not isinstance(values, list):
            values = [values]

        normalized: list[str] = []
        for value in values:
            if isinstance(value, str):
                clean = value.strip()
                if clean:
                    normalized.append(clean)
            elif isinstance(value, dict):
                clean = (
                    value.get("name")
                    or value.get("source")
                    or value.get("value")
                    or value.get("title")
                    or value.get("label")
                )
                if clean:
                    normalized.append(str(clean).strip())
            else:
                clean = str(value).strip()
                if clean:
                    normalized.append(clean)

        deduped: list[str] = []
        seen = set()
        for item in normalized:
            key = item.lower()
            if key not in seen:
                seen.add(key)
                deduped.append(item)

        return deduped

    def _normalize_key_entities(self, entities: object) -> dict[str, list[str]]:
        """Ensure key_entities is always a dict of string lists."""
        default_entities = {
            "actors": [],
            "malware": [],
            "locations": [],
            "sources": [],
        }

        if not isinstance(entities, dict):
            return default_entities

        normalized = {}
        for key in ["actors", "malware", "locations", "sources"]:
            normalized[key] = self._normalize_string_list(entities.get(key, []))

        return normalized

    def generate_analysis(self, query: str, results: list[dict]) -> dict:
        """Generate a grounded analysis from retrieved context only."""
        if not results:
            return {
                "title": "Threat Analysis",
                "status": "insufficient_context",
                "summary": "No relevant threat intelligence context was retrieved.",
                "recent_activity": [],
                "why_it_matters": "",
                "key_entities": {
                    "actors": [],
                    "malware": [],
                    "locations": [],
                    "sources": [],
                },
                "grounding_mode": "retrieved_context_only",
            }

        context = self._build_context(query, results)

        system_prompt = """
You are a senior cyber threat intelligence analyst.

Use ONLY the retrieved context provided by the user.
Do NOT add outside knowledge.
Do NOT guess.
Do NOT generalize beyond the retrieved context.
Do NOT infer relationships between entities unless explicitly stated in the retrieved context.
Do NOT merge different threat actors.
If multiple unrelated actors appear, treat them separately.
If the retrieved context is limited or weak, say so plainly.

Return valid JSON only with this exact shape:
{
  "summary": "3-5 sentence grounded threat summary",
  "recent_activity": ["bullet 1", "bullet 2"],
  "why_it_matters": "1-2 sentence impact statement",
  "key_entities": {
    "actors": ["..."],
    "malware": ["..."],
    "locations": ["..."],
    "sources": ["..."]
  }
}

All values in key_entities must be arrays of strings only.
Do NOT return objects inside key_entities.
"""

        response = self.client.chat(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": context},
            ],
        )

        content = response["message"]["content"]
        parsed = self._extract_json(content)

        normalized_entities = self._normalize_key_entities(
            parsed.get("key_entities", {})
        )

        return {
            "title": "Threat Analysis",
            "status": "generated",
            "summary": parsed.get("summary", "").strip(),
            "recent_activity": self._normalize_string_list(
                parsed.get("recent_activity", [])
            ),
            "why_it_matters": parsed.get("why_it_matters", "").strip(),
            "key_entities": normalized_entities,
            "grounding_mode": "retrieved_context_only",
        }
