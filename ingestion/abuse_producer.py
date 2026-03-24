# ingestion/abuse_producer.py
"""Ingests IOCs from Abuse.ch APIs (ThreatFox, URLhaus, MalwareBazaar) into Kafka."""

import logging
import requests
from config.decorator import retry
from config.config_loader import load_config
from ingestion.base_producer import BaseProducer

logger = logging.getLogger(__name__)

THREATFOX_API = "https://threatfox-api.abuse.ch/api/v1/"
URLHAUS_API = "https://urlhaus-api.abuse.ch/v1/"
MALWAREBAZAAR_API = "https://mb-api.abuse.ch/api/v1/"


class AbuseProducer(BaseProducer):
    """Produces IOC data from Abuse.ch APIs to Kafka."""

    auth_key: str
    topic: str

    def __init__(self) -> None:
        """Initialize Kafka producer and load Abuse.ch auth key."""
        super().__init__()
        config = load_config()
        self.auth_key = config["apis"]["abuse_ch"]
        self.topic = self.topics["iocs"]

    def http_headers(self) -> dict:
        """Return HTTP headers with Auth-Key for Abuse.ch APIs."""
        return {
            "Auth-Key": self.auth_key,
            "Content-Type": "application/json",
        }

    def http_headers_form(self) -> dict:
        """Return HTTP headers for form-encoded requests (MalwareBazaar)."""
        return {"Auth-Key": self.auth_key}

    # threat fox
    @retry(max_attempts=3, delay=2.0, backoff=2.0)
    def fetch_threatfox(self, days: int = 7) -> list[dict]:
        """Fetch recent IOCs from ThreatFox API."""
        resp = requests.post(
            THREATFOX_API,
            headers=self.http_headers(),
            json={"query": "get_iocs", "days": days},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("query_status") != "ok":
            logger.warning("ThreatFox query_status: %s", data.get("query_status"))
            return []
        iocs = data.get("data", []) or []
        logger.info("Fetched %d IOCs from ThreatFox", len(iocs))
        return iocs

    def parse_threatfox(self, ioc: dict) -> dict:
        """Parse a ThreatFox IOC into a normalized message."""
        tags = ioc.get("tags") or []
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]

        return {
            "id": f"tf-{ioc.get('id', '')}",
            "ioc_type": ioc.get("ioc_type", ""),
            "ioc_value": ioc.get("ioc", ""),
            "threat_type": ioc.get("threat_type", ""),
            "malware_family": ioc.get("malware_printable", ""),
            "malware_alias": ioc.get("malware_alias", ""),
            "confidence": ioc.get("confidence_level", 0),
            "tags": tags,
            "reporter": ioc.get("reporter", ""),
            "reference": ioc.get("reference", ""),
            "first_seen": ioc.get("first_seen", ""),
            "last_seen": ioc.get("last_seen", ""),
            "source": "ThreatFox",
        }

    # URLhaus
    @retry(max_attempts=3, delay=2.0, backoff=2.0)
    def fetch_urlhaus(self, limit: int = 1000) -> list[dict]:
        """Fetch recent malicious URLs from URLhaus API."""
        resp = requests.get(
            f"{URLHAUS_API}urls/recent/limit/{limit}/",
            headers=self.http_headers_form(),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        urls = data.get("urls", []) or []
        logger.info("Fetched %d URLs from URLhaus", len(urls))
        return urls

    def parse_urlhaus(self, entry: dict) -> dict:
        """Parse a URLhaus entry into a normalized message."""
        tags = entry.get("tags") or []
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]

        return {
            "id": f"uh-{entry.get('id', '')}",
            "ioc_type": "url",
            "ioc_value": entry.get("url", ""),
            "threat_type": entry.get("threat", ""),
            "malware_family": "",
            "malware_alias": "",
            "confidence": 75 if entry.get("url_status") == "online" else 50,
            "tags": tags,
            "reporter": entry.get("reporter", ""),
            "reference": entry.get("urlhaus_reference", ""),
            "first_seen": entry.get("date_added", ""),
            "last_seen": "",
            "source": "URLhaus",
        }

    # MalwareBazaar
    @retry(max_attempts=3, delay=2.0, backoff=2.0)
    def fetch_malwarebazaar(self, limit: int = 500) -> list[dict]:
        """Fetch recent malware samples from MalwareBazaar API."""
        resp = requests.post(
            MALWAREBAZAAR_API,
            headers=self.http_headers_form(),
            data={"query": "get_recent", "selector": "100"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("query_status") != "ok":
            logger.warning("MalwareBazaar query_status: %s", data.get("query_status"))
            return []
        samples = (data.get("data", []) or [])[:limit]
        logger.info("Fetched %d samples from MalwareBazaar", len(samples))
        return samples

    def parse_malwarebazaar(self, sample: dict) -> dict:
        """Parse a MalwareBazaar sample into a normalized message."""
        tags = sample.get("tags") or []
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]

        return {
            "id": f"mb-{sample.get('sha256_hash', '')[:16]}",
            "ioc_type": "sha256",
            "ioc_value": sample.get("sha256_hash", ""),
            "threat_type": "malware_sample",
            "malware_family": sample.get("signature") or "",
            "malware_alias": "",
            "confidence": 90,
            "tags": tags,
            "reporter": sample.get("reporter", ""),
            "reference": f"https://bazaar.abuse.ch/sample/{sample.get('sha256_hash', '')}/",
            "first_seen": sample.get("first_seen", ""),
            "last_seen": "",
            "source": "MalwareBazaar",
        }

    def fetch_and_publish(self) -> int:
        """Fetch IOCs from all Abuse.ch sources and publish to Kafka."""
        total = 0

        # ThreatFox
        try:
            tf_iocs = self.fetch_threatfox(days=7)
            for ioc in tf_iocs:
                msg = self.parse_threatfox(ioc)
                if msg["ioc_value"]:
                    self.publish(self.topic, msg, key=msg["id"])
                    total += 1
            logger.info("Published %d ThreatFox IOCs", len(tf_iocs))
        except Exception as e:
            logger.error("ThreatFox fetch failed: %s", e)

        # URLhaus
        try:
            uh_urls = self.fetch_urlhaus(limit=1000)
            for entry in uh_urls:
                msg = self.parse_urlhaus(entry)
                if msg["ioc_value"]:
                    self.publish(self.topic, msg, key=msg["id"])
                    total += 1
            logger.info("Published %d URLhaus IOCs", len(uh_urls))
        except Exception as e:
            logger.error("URLhaus fetch failed: %s", e)

        # MalwareBazaar
        try:
            mb_samples = self.fetch_malwarebazaar(limit=500)
            for sample in mb_samples:
                msg = self.parse_malwarebazaar(sample)
                if msg["ioc_value"]:
                    self.publish(self.topic, msg, key=msg["id"])
                    total += 1
            logger.info("Published %d MalwareBazaar IOCs", len(mb_samples))
        except Exception as e:
            logger.error("MalwareBazaar fetch failed: %s", e)

        self.flush()
        logger.info("Published %d total IOCs", total)
        return total
