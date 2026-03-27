# tests/conftest.py
"""Pytest fixtures for testing OSINT pipeline."""

import pytest


@pytest.fixture
def sample_article():
    """Sample raw article for testing."""
    return {
        "title": "APT28 Launches Cyberattack on Ukraine Using Cobalt Strike",
        "content": "Russian threat actor APT28 (Fancy Bear) was observed targeting Ukrainian government agencies using Cobalt Strike malware. The attack originated from IP 185.220.101.34.",
        "source": "SecurityWeek",
        "url": "https://example.com/article123",
        "published_at": "2026-03-25T08:00:00Z",
    }


@pytest.fixture
def sample_enriched_article():
    """Sample enriched article with extracted entities."""
    return {
        "title": "APT28 Launches Cyberattack on Ukraine Using Cobalt Strike",
        "content": "Russian threat actor APT28...",
        "source": "SecurityWeek",
        "original_url": "https://example.com/article123",
        "published_at": "2026-03-25T08:00:00Z",
        "threat_actors": ["APT28", "Fancy Bear"],
        "malware": ["Cobalt Strike"],
        "locations": ["Ukraine", "Russia"],
        "persons": [],
        "organizations": ["Ukrainian government"],
        "attack_techniques": [],
        "relevance_score": 0.85,
        "_enriched_at": "2026-03-25T08:05:00Z",
    }


@pytest.fixture
def sample_ioc():
    """Sample IOC record."""
    return {
        "id": "12345",
        "ioc_type": "ip:port",
        "ioc_value": "185.220.101.34:443",
        "threat_type": "c2",
        "malware_family": "CobaltStrike",
        "malware_alias": "",
        "threat_actor": "",
        "confidence": 90,
        "tags": ["apt28", "ukraine"],
        "reporter": "abuse.ch",
        "reference": "https://threatfox.abuse.ch/ioc/12345",
        "first_seen": "2026-03-24T10:00:00Z",
        "last_seen": "2026-03-25T08:00:00Z",
        "source": "ThreatFox",
    }


@pytest.fixture
def mock_config_data():
    """Mock configuration data."""
    return {
        "kafka": {
            "bootstrap_servers": ["localhost:9092"],
            "topics": {
                "news": "osint.news",
                "enriched": "osint.enriched",
                "iocs": "osint.iocs",
                "mitre": "osint.mitre",
            },
        },
        "snowflake": {
            "account": "test",
            "user": "test",
            "password": "test",
            "warehouse": "test",
            "database": "test",
            "schema": "test",
        },
        "neo4j": {"uri": "bolt://localhost:7687", "user": "neo4j", "password": "test"},
        "newsapi": {"api_key": "test_key"},
    }
