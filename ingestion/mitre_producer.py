# ingestion/mitre_producer.py
"""Ingests MITRE ATT&CK threat actor and technique data into Kafka."""

import logging
from attackcti import attack_client
from config.decorator import retry
from ingestion.base_producer import BaseProducer

logger = logging.getLogger(__name__)


class MitreProducer(BaseProducer):
    """Produces MITRE ATT&CK threat group data to Kafka."""

    topic: str

    def __init__(self) -> None:
        """Initializes Kafka producer and loads the MITRE ATT&CK client."""
        super().__init__()
        logger.info("Loading MITRE ATT&CK data (could take a while)")
        self.client = attack_client()
        self.topic = self.topics["mitre"]

    @retry(max_attempts=3, delay=1.0, backoff=2.0)
    def fetch_groups_from_api(self) -> list[dict]:
        """Fetch all groups from the MITRE ATT&CK API with retry logic."""
        groups: list = self.client.get_groups()
        logger.info("Fetched %d threat groups from MITRE ATT&CK", len(groups))
        return groups

    def fetch_and_publish(self) -> int:
        """Fetches threat groups from MITRE ATT&CK and publishes them to Kafka."""
        groups: list = self.fetch_groups_from_api()

        for group in groups:
            aliases: list = group.get("aliases", []) or []
            external: list = group.get("external_references", []) or []

            message: dict = {
                "type": "threat_group",
                "name": group.get("name", ""),
                "aliases": aliases,
                "description": group.get("description", ""),
                "mitre_id": next(
                    (
                        r["external_id"]
                        for r in external
                        if r.get("source_name") == "mitre-attack"
                    ),
                    "",
                ),
                "url": next(
                    (
                        r["url"]
                        for r in external
                        if r.get("source_name") == "mitre-attack"
                    ),
                    "",
                ),
            }
            try:
                self.publish(self.topic, message, key=message["mitre_id"])
            except Exception as e:
                logger.error(
                    "Failed to publish MITRE group %s: %s", message["mitre_id"], e
                )

        return len(groups)
