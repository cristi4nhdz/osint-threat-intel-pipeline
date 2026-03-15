# ingestion/otx_producer.py
"""Ingests AlienVault OTX threat pulses into Kafka."""

import logging
from datetime import datetime, timezone, timedelta
from OTXv2 import OTXv2
from config.decorator import retry
from config.config_loader import load_config
from ingestion.base_producer import BaseProducer

logger = logging.getLogger(__name__)


class OTXProducer(BaseProducer):
    """Produces AlienVault OTX threat pulse data to Kafka."""

    otx: OTXv2
    topic: str

    def __init__(self) -> None:
        """Initialize Kafka producer and AlienVault OTX client."""
        super().__init__()
        config = load_config()
        self.otx = OTXv2(config["apis"]["alienvault_otx_key"])
        self.topic = self.topics["news"]

    @retry(max_attempts=3, delay=1, backoff=2.0)
    def fetch_pulses_from_api(self, max_pulses: int = 20) -> list[dict]:
        """Fetch OTX pulses from the AlienVault OTX API from the past 30 days with retry."""
        since = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        pulses: list = list(self.otx.getsince(since))[:max_pulses]
        return pulses

    def fetch_and_publish(self, max_pulses: int = 20) -> int:
        """Fetch recent OTX pulses and publish them to Kafka."""
        pulses = self.fetch_pulses_from_api(max_pulses)

        count = 0
        for pulse in pulses:
            tags: list = pulse.get("tags", []) or []
            indicators: list = pulse.get("indicators", []) or []

            message: dict = {
                "title": pulse.get("name", ""),
                "source": "AlienVault OTX",
                "url": f"https://otx.alienvault.com/pulse/{pulse.get('id', '')}",
                "published_at": pulse.get("created", ""),
                "content": pulse.get("description", ""),
                "tags": tags,
                "ioc_count": len(indicators),
                "malware_families": pulse.get("malware_families", []) or [],
                "targeted_countries": pulse.get("targeted_countries", []) or [],
            }
            try:
                self.publish(self.topic, message, key=message["url"])
                count += 1
            except Exception as e:
                logger.error("Failed to publish OTX pulse %s: %s", message["url"], e)

        logger.info("Published %d OTX pulses", count)
        return count
