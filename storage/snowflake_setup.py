# storage/snowflake_setup.py
"""Creates the Snowflake database, schema, and tables for the pipeline."""

import logging
import snowflake.connector
from config.config_loader import load_config

logger = logging.getLogger(__name__)


def setup() -> None:
    """Creates the Snowflake database, schema, and threat_articles table."""
    c = load_config()["snowflake"]
    conn = snowflake.connector.connect(
        account=c["account"],
        user=c["user"],
        password=c["password"],
        warehouse=c["warehouse"],
    )
    cur = conn.cursor()

    cur.execute("CREATE DATABASE IF NOT EXISTS THREAT_INTEL")
    logger.info("Database THREAT_INTEL ready.")

    cur.execute("USE DATABASE THREAT_INTEL")
    cur.execute("USE SCHEMA PUBLIC")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS threat_articles (
            id                VARCHAR PRIMARY KEY,
            title             VARCHAR,
            source            VARCHAR,
            original_url      VARCHAR UNIQUE,
            published_at      TIMESTAMP_TZ,
            threat_actors     ARRAY,
            malware           ARRAY,
            locations         ARRAY,
            persons           ARRAY,
            organizations     ARRAY,
            attack_techniques ARRAY,
            relevance_score   FLOAT,
            enriched_at       TIMESTAMP_TZ,
            inserted_at       TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        )
    """
    )
    logger.info("Table threat_articles ready.")

    cur.close()
    conn.close()
    logger.info("Snowflake setup complete.")


if __name__ == "__main__":
    setup()
