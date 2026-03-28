# ingestion/rss_producer.py
"""Ingests cybersecurity RSS feeds into Kafka."""

import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from time import mktime
import urllib.request
import feedparser
from config.decorator import retry
from ingestion.base_producer import BaseProducer

logger = logging.getLogger(__name__)

# Cybersecurity RSS feeds
RSS_FEEDS: dict = {
    # Vendor research blogs
    "CrowdStrike": "https://www.crowdstrike.com/blog/feed/",
    "Mandiant": "https://cloud.google.com/feeds/blog-mandiant.xml",
    "Unit 42": "https://unit42.paloaltonetworks.com/feed/",
    "Cisco Talos": "https://blog.talosintelligence.com/rss/",
    "SentinelOne": "https://www.sentinelone.com/blog/feed/",
    "ESET Research": "https://www.welivesecurity.com/en/feed/",
    "Kaspersky SecureList": "https://securelist.com/feed/",
    "Sophos News": "https://news.sophos.com/en-us/feed/",
    "Trend Micro": "https://www.trendmicro.com/en_us/research.html/rss",
    "Check Point Research": "https://research.checkpoint.com/feed/",
    "Recorded Future": "https://www.recordedfuture.com/feed",
    "Proofpoint": "https://www.proofpoint.com/us/blog/threat-insight/feed",
    "Fortinet": "https://feeds.fortinet.com/fortinet/blog/threat-research",
    "Zscaler": "https://www.zscaler.com/blogs/security-research/rss",
    "Elastic Security Labs": "https://www.elastic.co/security-labs/rss/feed.xml",
    "Microsoft Security": "https://www.microsoft.com/en-us/security/blog/feed/",
    "Google TAG": "https://blog.google/threat-analysis-group/rss/",
    "Google Project Zero": "https://googleprojectzero.blogspot.com/feeds/posts/default",
    "Trellix": "https://www.trellix.com/blogs/research/rss/",
    "McAfee Labs": "https://www.mcafee.com/blogs/tag/advanced-threat-research/feed/",
    "Juniper Threat Labs": "https://blogs.juniper.net/threat-research/feed/",
    "Netskope": "https://www.netskope.com/blog/category/netskope-threat-labs/feed/",
    "Cyble": "https://cyble.com/feed/",
    "Sekoia": "https://blog.sekoia.io/feed/",
    "Seqrite": "https://www.seqrite.com/blog/tag/cybersecurity/feed/",
    "Group-IB": "https://www.group-ib.com/blog/feed/",
    "Secureworks": "https://www.secureworks.com/rss?feed=blog",
    "WithSecure": "https://labs.withsecure.com/publications/rss.xml",
    "Orange Cyberdefense": "https://www.orangecyberdefense.com/global/blog/rss",
    "Dragos": "https://www.dragos.com/feed/",
    "Claroty": "https://claroty.com/blog/feed",
    # Independent / community
    "Krebs on Security": "https://krebsonsecurity.com/feed/",
    "Schneier on Security": "https://www.schneier.com/feed/atom/",
    "The Hacker News": "https://feeds.feedburner.com/TheHackersNews",
    "BleepingComputer": "https://www.bleepingcomputer.com/feed/",
    "Dark Reading": "https://www.darkreading.com/rss.xml",
    "SecurityWeek": "https://www.securityweek.com/feed/",
    "SC Media": "https://www.scmagazine.com/feed",
    "Infosecurity Magazine": "https://www.infosecurity-magazine.com/rss/news/",
    "HackRead": "https://www.hackread.com/feed/",
    "SecurityAffairs": "https://securityaffairs.com/feed",
    "Graham Cluley": "https://grahamcluley.com/feed/",
    "Help Net Security": "https://www.helpnetsecurity.com/feed/",
    "Threatpost": "https://threatpost.com/feed/",
    "CSO Online": "https://www.csoonline.com/feed/",
    "The Record": "https://therecord.media/feed",
    "CyberScoop": "https://cyberscoop.com/feed/",
    "Security Magazine": "https://www.securitymagazine.com/rss/topic/2236",
    # Government / CERT
    "CISA Alerts": "https://www.cisa.gov/cybersecurity-advisories/all.xml",
    "SANS ISC": "https://isc.sans.edu/rssfeed.xml",
    "US-CERT NCAS": "https://www.cisa.gov/uscert/ncas/alerts.xml",
    "CERT-EU": "https://cert.europa.eu/publications/security-advisories/rss",
    # Malware / threat intel focused
    "MalwareBytes Labs": "https://www.malwarebytes.com/blog/feed/index.xml",
    "Sucuri Blog": "https://blog.sucuri.net/feed/",
    "Wordfence": "https://www.wordfence.com/blog/feed/",
    "VirusTotal Blog": "https://blog.virustotal.com/feeds/posts/default?alt=rss",
    "ANY.RUN Blog": "https://any.run/cybersecurity-blog/feed/",
    "Huntress Blog": "https://www.huntress.com/blog/rss.xml",
    "ReversingLabs": "https://www.reversinglabs.com/blog/rss.xml",
    "Volexity": "https://www.volexity.com/blog/feed/",
    "Malware Traffic Analysis": "http://www.malware-traffic-analysis.net/blog-entries.rss",
    "Avast Threat Labs": "https://blog.avast.com/rss.xml",
    "Cofense": "https://cofense.com/blog/feed/",
    "Abnormal Security": "https://abnormalsecurity.com/blog/rss.xml",
    "Intezer": "https://intezer.com/feed/",
    "SOC Prime": "https://socprime.com/blog/feed/",
    # ICS / OT security
    "Nozomi Networks": "https://www.nozominetworks.com/blog/rss.xml",
    "Otorio": "https://www.otorio.com/blog/feed/",
}


class RSSProducer(BaseProducer):
    """Produces cybersecurity RSS feed articles to Kafka."""

    topic: str

    def __init__(self) -> None:
        """Initialize Kafka producer and set the target topic."""
        super().__init__()
        self.topic = self.topics["news"]

    @retry(max_attempts=2, delay=2.0, backoff=2.0)
    def fetch_feed(self, url: str) -> list[dict]:
        """Parse a single RSS feed URL with retry logic."""

        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            resp = urllib.request.urlopen(req, timeout=10)
            feed = feedparser.parse(resp)
        except Exception:
            return []
        return feed.get("entries", [])

    def fetch_and_publish(self, max_per_feed: int = 20) -> int:
        """Fetch all RSS feeds and publish articles to Kafka."""

        total = 0
        failed_feeds = 0

        for source_name, feed_url in RSS_FEEDS.items():
            try:
                entries: list[dict] = self.fetch_feed(feed_url)
            except Exception as e:
                logger.warning("Failed to fetch %s: %s", source_name, e)
                failed_feeds += 1
                continue

            published = 0
            for entry in entries[:max_per_feed]:
                published_at = self.parse_date(entry)
                content = entry.get("summary", "") or entry.get("description", "") or ""
                link = entry.get("link", "")

                if not link:
                    continue

                message: dict = {
                    "title": entry.get("title", ""),
                    "source": source_name,
                    "url": link,
                    "published_at": published_at,
                    "content": content[:5000],
                }

                try:
                    self.publish(self.topic, message, key=link)
                    published += 1
                except Exception as e:
                    logger.error("Failed to publish RSS article %s: %s", link, e)

            total += published
            if published > 0:
                logger.info("Published %d articles from %s", published, source_name)

        self.flush()
        if failed_feeds > 0:
            logger.warning("%d feeds failed to fetch", failed_feeds)
        logger.info("Published %d total RSS articles", total)
        return total

    @staticmethod
    def parse_date(entry: dict) -> str:
        """Extract and normalize the publication date from an RSS entry."""

        # try structured date first
        if entry.get("published_parsed"):
            try:

                ts = mktime(entry["published_parsed"])
                return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            except (TypeError, ValueError, OverflowError):
                pass

        # try raw string
        raw = entry.get("published", "") or entry.get("updated", "")
        if raw:
            try:
                return parsedate_to_datetime(raw).isoformat()
            except (TypeError, ValueError):
                pass
            try:
                return datetime.fromisoformat(raw.replace("Z", "+00:00")).isoformat()
            except (TypeError, ValueError):
                pass

        return ""
