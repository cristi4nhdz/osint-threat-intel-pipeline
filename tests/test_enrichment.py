# tests/test_enrichment.py
"""Tests for NLP entity extraction."""

import pytest
from processing.entity_extractor import EntityExtractor


class TestEntityExtractor:
    """Test entity extraction functionality."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Initialize extractor before each test."""
        self.extractor = EntityExtractor()

    def test_extract_threat_actors(self, sample_article):
        """Test that threat actors are correctly extracted."""
        result = self.extractor.extract(sample_article)

        assert "threat_actors" in result
        assert (
            "APT28" in result["threat_actors"]
            or "Fancy Bear" in result["threat_actors"]
        )

    def test_extract_malware(self, sample_article):
        """Test that malware families are correctly extracted."""
        result = self.extractor.extract(sample_article)

        assert "malware" in result
        # Check case-insensitive
        malware_lower = [m.lower() for m in result["malware"]]
        assert "cobalt strike" in malware_lower or "cobaltstrike" in malware_lower

    def test_extract_locations(self, sample_article):
        """Test that locations are correctly extracted."""
        result = self.extractor.extract(sample_article)

        assert "locations" in result
        assert "Ukraine" in result["locations"]

    def test_relevance_score_calculation(self, sample_article):
        """Test that relevance score is calculated."""
        result = self.extractor.extract(sample_article)

        assert "relevance_score" in result
        assert 0.0 <= result["relevance_score"] <= 1.0
        assert result["relevance_score"] > 0.5  # Should be high for cyber article

    def test_low_relevance_article(self):
        """Test that non-cyber articles get low relevance scores."""
        article = {
            "title": "Best Restaurants in New York City",
            "content": "Here are the top 10 places to eat in NYC...",
            "source": "FoodBlog",
            "url": "https://example.com/food",
            "published_at": "2026-03-25T08:00:00Z",
        }

        result = self.extractor.extract(article)
        assert result["relevance_score"] < 0.2

    def test_extract_preserves_original_fields(self, sample_article):
        """Test that original article fields are preserved."""
        result = self.extractor.extract(sample_article)

        assert result["title"] == sample_article["title"]
        assert result["source"] == sample_article["source"]
        assert "original_url" in result

    def test_extract_handles_empty_content(self):
        """Test extraction with empty content."""
        article = {
            "title": "Test Article",
            "content": "",
            "source": "Test",
            "url": "https://example.com/test",
            "published_at": "2026-03-25T08:00:00Z",
        }

        result = self.extractor.extract(article)
        assert "threat_actors" in result
        assert result["threat_actors"] == []

    def test_extract_handles_missing_fields(self):
        """Test extraction with missing fields."""
        article = {"title": "Test Article"}

        result = self.extractor.extract(article)
        assert "relevance_score" in result
        assert result["relevance_score"] == 0.0
