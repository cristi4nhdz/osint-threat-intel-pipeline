# ingestion/otx_producer.py
"""Ingests AlienVault OTX threat pulses into Kafka."""

import logging
from datetime import datetime, timezone, timedelta
from OTXv2 import OTXv2
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

    def fetch_and_publish(self, max_pulses: int = 20) -> int:
        """Fetch recent OTX pulses and publish them to Kafka."""
        # only fetch pulses from the last 30 days
        since = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        pulses: list = list(self.otx.getsince(since))[:max_pulses]

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
            self.publish(self.topic, message, key=message["url"])
            count += 1

        logger.info("Published %d OTX pulses", count)
        return count
