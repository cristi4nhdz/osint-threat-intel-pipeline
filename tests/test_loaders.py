# tests/test_loaders.py
"""Tests for data loaders (Snowflake and Neo4j)."""

from unittest.mock import MagicMock, patch
from storage.snowflake_loader import SnowflakeLoader
from storage.ioc_loader import IOCLoader


class TestSnowflakeLoader:
    """Test Snowflake loader."""

    @patch("storage.snowflake_loader.load_config")
    @patch("storage.snowflake_loader.snowflake.connector.connect")
    @patch("storage.snowflake_loader.KafkaConsumer")
    def test_loader_initialization(
        self, mock_consumer, mock_snowflake, mock_config, mock_config_data
    ):
        """Test loader initializes correctly."""
        mock_config.return_value = mock_config_data

        mock_conn = MagicMock()
        mock_snowflake.return_value = mock_conn

        loader = SnowflakeLoader()

        assert loader.input_topic == "osint.enriched"
        mock_snowflake.assert_called_once()
        mock_consumer.assert_called_once()

    @patch("storage.snowflake_loader.load_config")
    @patch("storage.snowflake_loader.snowflake.connector.connect")
    @patch("storage.snowflake_loader.KafkaConsumer")
    def test_loader_inserts_article(
        self,
        mock_consumer_class,
        mock_snowflake,
        mock_config,
        mock_config_data,
        sample_enriched_article,
    ):
        """Test loader inserts articles into Snowflake."""
        mock_config.return_value = mock_config_data

        # Setup mock cursor
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake.return_value = mock_conn

        # Setup mock consumer
        mock_consumer = MagicMock()
        mock_message = MagicMock()
        mock_message.value = sample_enriched_article

        mock_consumer.poll.side_effect = [{None: [mock_message]}, {}]
        mock_consumer_class.return_value = mock_consumer

        loader = SnowflakeLoader()
        count = loader.run()

        assert count == 1
        mock_cursor.execute.assert_called()

    @patch("storage.snowflake_loader.load_config")
    @patch("storage.snowflake_loader.snowflake.connector.connect")
    @patch("storage.snowflake_loader.KafkaConsumer")
    def test_loader_skips_duplicates(
        self,
        mock_consumer_class,
        mock_snowflake,
        mock_config,
        mock_config_data,
        sample_enriched_article,
    ):
        """Test loader skips duplicate articles."""
        mock_config.return_value = mock_config_data

        # rowcount = 0 means duplicate was skipped
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake.return_value = mock_conn

        mock_consumer = MagicMock()
        mock_message = MagicMock()
        mock_message.value = sample_enriched_article

        mock_consumer.poll.side_effect = [{None: [mock_message]}, {}]
        mock_consumer_class.return_value = mock_consumer

        loader = SnowflakeLoader()
        count = loader.run()

        # Processes message but doesn't count as inserted
        mock_cursor.execute.assert_called()


class TestIOCLoader:
    """Test IOC loader."""

    @patch("storage.ioc_loader.load_config")
    @patch("storage.ioc_loader.snowflake.connector.connect")
    @patch("storage.ioc_loader.GraphDatabase.driver")
    @patch("storage.ioc_loader.KafkaConsumer")
    def test_ioc_loader_initialization(
        self, mock_consumer, mock_neo4j, mock_snowflake, mock_config, mock_config_data
    ):
        """Test IOC loader initializes correctly."""
        mock_config.return_value = mock_config_data

        loader = IOCLoader()

        mock_snowflake.assert_called_once()
        mock_neo4j.assert_called_once()
        mock_consumer.assert_called_once()

    @patch("storage.ioc_loader.load_config")
    @patch("storage.ioc_loader.snowflake.connector.connect")
    @patch("storage.ioc_loader.GraphDatabase.driver")
    @patch("storage.ioc_loader.KafkaConsumer")
    def test_ioc_loader_processes_iocs(
        self,
        mock_consumer_class,
        mock_neo4j,
        mock_snowflake,
        mock_config,
        mock_config_data,
        sample_ioc,
    ):
        """Test IOC loader processes IOCs."""
        mock_config.return_value = mock_config_data

        # Setup mocks
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake.return_value = mock_conn

        mock_driver = MagicMock()
        mock_neo4j.return_value = mock_driver

        mock_consumer = MagicMock()
        mock_message = MagicMock()
        mock_message.value = sample_ioc

        mock_consumer.poll.side_effect = [{None: [mock_message]}, {}]
        mock_consumer_class.return_value = mock_consumer

        loader = IOCLoader()
        loader.load()

        # Should insert to Snowflake
        mock_cursor.execute.assert_called()
