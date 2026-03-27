# tests/test_consumers.py
"""Tests for Kafka consumers."""

from unittest.mock import MagicMock, patch
from processing.enrichment_consumer import EnrichmentConsumer


class TestEnrichmentConsumer:
    """Test enrichment consumer."""

    @patch("processing.enrichment_consumer.load_config")
    @patch("processing.enrichment_consumer.KafkaConsumer")
    @patch("processing.enrichment_consumer.KafkaProducer")
    def test_consumer_initialization(
        self, mock_producer, mock_consumer, mock_config, mock_config_data
    ):
        """Test consumer initializes correctly."""
        mock_config.return_value = mock_config_data

        consumer = EnrichmentConsumer()

        assert consumer.min_relevance_score == 0.2
        assert consumer.input_topic == "osint.news"
        assert consumer.output_topic == "osint.enriched"
        mock_consumer.assert_called_once()
        mock_producer.assert_called_once()

    @patch("processing.enrichment_consumer.load_config")
    @patch("processing.enrichment_consumer.KafkaConsumer")
    @patch("processing.enrichment_consumer.KafkaProducer")
    def test_consumer_processes_messages(
        self,
        mock_producer_class,
        mock_consumer_class,
        mock_config,
        mock_config_data,
        sample_article,
    ):
        """Test consumer processes messages and publishes enriched data."""
        mock_config.return_value = mock_config_data

        # Setup mock consumer to return one message then timeout
        mock_consumer = MagicMock()
        mock_message = MagicMock()
        mock_message.value = sample_article

        mock_consumer.poll.side_effect = [
            {None: [mock_message]},  # First poll returns message
            {},  # Second poll times out
        ]
        mock_consumer_class.return_value = mock_consumer

        # Setup mock producer
        mock_producer = MagicMock()
        mock_producer.send.return_value.get.return_value = None
        mock_producer_class.return_value = mock_producer

        consumer = EnrichmentConsumer()
        count = consumer.run()

        # Should process 1 article (if relevance > 0.2)
        assert count >= 0
        mock_producer.send.assert_called()

    @patch("processing.enrichment_consumer.load_config")
    @patch("processing.enrichment_consumer.KafkaConsumer")
    @patch("processing.enrichment_consumer.KafkaProducer")
    def test_consumer_skips_low_relevance(
        self, mock_producer_class, mock_consumer_class, mock_config, mock_config_data
    ):
        """Test consumer skips articles with low relevance."""
        mock_config.return_value = mock_config_data

        low_relevance_article = {
            "title": "Cooking Tips",
            "content": "How to bake a cake",
            "source": "Food",
            "url": "https://example.com/food",
            "published_at": "2026-03-25T08:00:00Z",
        }

        mock_consumer = MagicMock()
        mock_message = MagicMock()
        mock_message.value = low_relevance_article

        mock_consumer.poll.side_effect = [{None: [mock_message]}, {}]
        mock_consumer_class.return_value = mock_consumer

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        consumer = EnrichmentConsumer()
        count = consumer.run()

        # Should skip low relevance article
        assert count == 0
        mock_producer.send.assert_not_called()

    @patch("processing.enrichment_consumer.load_config")
    @patch("processing.enrichment_consumer.KafkaConsumer")
    @patch("processing.enrichment_consumer.KafkaProducer")
    def test_consumer_closes_properly(
        self, mock_producer_class, mock_consumer_class, mock_config, mock_config_data
    ):
        """Test consumer closes connections properly."""
        mock_config.return_value = mock_config_data

        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        consumer = EnrichmentConsumer()
        consumer.close()

        mock_consumer.close.assert_called_once()
        mock_producer.close.assert_called_once()
