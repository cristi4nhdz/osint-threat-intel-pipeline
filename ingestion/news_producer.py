# ingestion/news_producer.py
"""Checks NewsAPI for articles and forwards them to Kafka."""

import logging
from datetime import datetime
from newsapi import NewsApiClient
from config.decorator import retry
from config.config_loader import load_config
from ingestion.base_producer import BaseProducer

logger = logging.getLogger(__name__)

THREAT_KEYWORDS: tuple[str, ...] = (
    "cyberattack",
    "ransomware",
    "APT",
    "malware",
    '"data breach"',
    '"threat actor"',
    "vulnerability",
    "exploit",
    "phishing",
    "espionage",
    "botnet",
    "zero-day",
    "DDoS",
    "credential stuffing",
    "supply chain attack",
    "backdoor",
    "keylogger",
    "rootkit",
    "spyware",
    "cyber espionage",
)


class NewsProducer(BaseProducer):
    """NewsAPI producer for fetching and publishing articles."""

    client: NewsApiClient
    topic: str

    def __init__(self) -> None:
        """Initializes NewsAPI client and sets the target Kafka topic."""
        super().__init__()
        config = load_config()
        self.client = NewsApiClient(api_key=config["apis"]["newsapi_key"])
        self.topic = self.topics["news"]

    @retry(max_attempts=3, delay=1.0, backoff=2.0)
    def fetch_articles_from_api(self) -> list[dict]:
        """Fetches articles from NewsAPI with retry logic."""
        query = " OR ".join(THREAT_KEYWORDS)
        response = self.client.get_everything(
            q=query,
            language="en",
            sort_by="relevancy",
            page_size=50,
        )
        articles = response.get("articles", [])
        logger.info("Fetched %d articles from NewsAPI", len(articles))
        return articles

    def fetch_and_publish(self) -> int:
        """Fetches news from NewsAPI and publishes it to Kafka."""
        articles = self.fetch_articles_from_api()
        published_count = 0

        for article in articles:
            try:
                published_at = datetime.fromisoformat(
                    article.get("publishedAt", "").replace("Z", "+00:00")
                ).isoformat()
            except (ValueError, AttributeError) as e:
                logger.warning("Unparseable publishedAt: %s", e)
                published_at = ""

            message: dict[str, str] = {
                "title": article.get("title", ""),
                "source": article.get("source", {}).get("name", ""),
                "url": article.get("url", ""),
                "published_at": published_at,
                "content": article.get("content") or article.get("description", ""),
            }

            try:
                # use URL as key to route duplicates to the same partition
                self.publish(self.topic, message, key=message["url"])
                published_count += 1
            except Exception as e:
                logger.error("Failed to publish article %s: %s", message["url"], e)

        self.flush()
        return published_count
