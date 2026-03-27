# tests/test_producers.py
"""Tests for Kafka producers."""

from unittest.mock import MagicMock, patch
import pytest
from ingestion.news_producer import NewsProducer


class TestNewsProducer:
    """Test NewsAPI producer."""

    @patch("ingestion.news_producer.load_config")
    @patch("ingestion.base_producer.KafkaProducer")
    @patch("ingestion.news_producer.NewsApiClient")
    def test_producer_initialization(
        self, mock_newsapi, mock_kafka_producer, mock_config
    ):
        """Test producer initializes correctly."""
        mock_config.return_value = {
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "topics": {"news": "osint.news"},
            },
            "apis": {"newsapi_key": "test_key"},
        }

        producer = NewsProducer()

        assert producer.topic == "osint.news"
        mock_kafka_producer.assert_called_once()
        mock_newsapi.assert_called_once()

    @patch("ingestion.news_producer.load_config")
    @patch("ingestion.base_producer.KafkaProducer")
    @patch("ingestion.news_producer.NewsApiClient")
    def test_producer_publishes_articles(
        self, mock_newsapi_class, mock_kafka_producer_class, mock_config
    ):
        """Test producer publishes articles to Kafka."""
        mock_config.return_value = {
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "topics": {"news": "osint.news"},
            },
            "apis": {"newsapi_key": "test_key"},
        }

        # Mock NewsAPI response
        mock_newsapi = MagicMock()
        mock_newsapi.get_everything.return_value = {
            "articles": [
                {
                    "title": "Test Article",
                    "description": "Test description",
                    "url": "https://example.com/test",
                    "publishedAt": "2026-03-25T08:00:00Z",
                    "source": {"name": "TestSource"},
                    "content": "Test content",
                }
            ]
        }
        mock_newsapi_class.return_value = mock_newsapi

        # Mock Kafka producer
        mock_kafka_producer = MagicMock()
        mock_kafka_producer.send.return_value.get.return_value = None
        mock_kafka_producer_class.return_value = mock_kafka_producer

        producer = NewsProducer()
        producer.fetch_and_publish()

        # Should publish at least one article
        assert mock_kafka_producer.send.call_count > 0

    @patch("ingestion.news_producer.load_config")
    @patch("ingestion.base_producer.KafkaProducer")
    @patch("ingestion.news_producer.NewsApiClient")
    def test_producer_handles_empty_response(
        self, mock_newsapi_class, mock_kafka_producer_class, mock_config
    ):
        """Test producer handles empty API response."""
        mock_config.return_value = {
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "topics": {"news": "osint.news"},
            },
            "apis": {"newsapi_key": "test_key"},
        }

        # Mock empty response
        mock_newsapi = MagicMock()
        mock_newsapi.get_everything.return_value = {"articles": []}
        mock_newsapi_class.return_value = mock_newsapi

        mock_kafka_producer = MagicMock()
        mock_kafka_producer_class.return_value = mock_kafka_producer

        producer = NewsProducer()
        producer.fetch_and_publish()

        # Should not publish anything
        mock_kafka_producer.send.assert_not_called()

    @patch("ingestion.news_producer.load_config")
    @patch("ingestion.base_producer.KafkaProducer")
    @patch("ingestion.news_producer.NewsApiClient")
    def test_producer_handles_api_errors(
        self, mock_newsapi_class, mock_kafka_producer_class, mock_config
    ):
        """Test producer handles API errors gracefully."""
        mock_config.return_value = {
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "topics": {"news": "osint.news"},
            },
            "apis": {"newsapi_key": "test_key"},
        }

        # Mock API error
        mock_newsapi = MagicMock()
        mock_newsapi.get_everything.side_effect = Exception("API Error")
        mock_newsapi_class.return_value = mock_newsapi

        mock_kafka_producer = MagicMock()
        mock_kafka_producer_class.return_value = mock_kafka_producer

        producer = NewsProducer()

        # Should handle exception gracefully
        # Retry decorator tries 3 times then raises
        with pytest.raises(Exception, match="API Error"):
            producer.fetch_and_publish()
