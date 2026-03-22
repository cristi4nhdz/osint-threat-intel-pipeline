# storage/neo4j_loader.py
"""Builds actor relationship graph in Neo4j from enriched Kafka messages and MITRE data."""

import json
import logging
from neo4j import GraphDatabase
from kafka import KafkaConsumer
from config.config_loader import load_config
from processing.actor_data import ACTOR_NORMALIZE, MITRE_ORIGINS

logger = logging.getLogger(__name__)


def create_mitre_group(tx, group: dict) -> None:
    """Merge a MITRE ATT&CK group node and set its properties."""

    raw_name = group.get("name", "")
    name = ACTOR_NORMALIZE.get(raw_name.lower(), raw_name)

    # create MITRE ATT&CK group
    tx.run(
        """
        MERGE (a:ThreatActor {mitre_id: $mitre_id})
        SET a.name = $name,
            a.description = $description,
            a.url = $url,
            a.aliases = $aliases
    """,
        mitre_id=group.get("mitre_id", ""),
        name=name,
        description=group.get("description", "")[:500],
        url=group.get("url", ""),
        aliases=group.get("aliases", []),
    )


def create_article_graph(tx, article: dict) -> None:
    """Merge an article node and link it to actors, malware, and locations."""

    url = article.get("original_url", "")
    actors = article.get("threat_actors", [])
    malware = article.get("malware", [])
    locs = article.get("locations", [])

    # create article node
    tx.run(
        """
        MERGE (art:Article {url: $url})
        SET art.title = $title,
            art.source = $source,
            art.published_at = $published_at,
            art.relevance_score = $relevance_score
    """,
        url=url,
        title=article.get("title", ""),
        source=article.get("source", ""),
        published_at=article.get("published_at", ""),
        relevance_score=article.get("relevance_score", 0.0),
    )

    # link threat actor
    for actor in actors:
        tx.run(
            """
            MERGE (a:ThreatActor {name: $name})
            MERGE (art:Article {url: $url})
            MERGE (art)-[:MENTIONS_ACTOR]->(a)
        """,
            name=actor,
            url=url,
        )

    # link malware
    for mal in malware:
        tx.run(
            """
            MERGE (m:Malware {name: $name})
            MERGE (art:Article {url: $url})
            MERGE (art)-[:MENTIONS_MALWARE]->(m)
        """,
            name=mal,
            url=url,
        )

    if len(actors) == 1:
        actor = actors[0]

        # link actor -> malware
        for mal in malware:
            tx.run(
                """
                MERGE (a:ThreatActor {name: $actor})
                MERGE (m:Malware {name: $malware})
                MERGE (a)-[:USES]->(m)
            """,
                actor=actor,
                malware=mal,
            )

        # link actor -> locations
        for loc in locs:
            tx.run(
                """
                MERGE (a:ThreatActor {name: $actor})
                MERGE (l:Location {name: $location})
                MERGE (a)-[:TARGETS]->(l)
            """,
                actor=actor,
                location=loc,
            )

    elif len(actors) > 1:
        # link actor -> locations (skip origin)
        for actor in actors:

            canonical = ACTOR_NORMALIZE.get(actor.lower(), actor)
            known_origin = MITRE_ORIGINS.get(canonical)

            for loc in locs:

                if known_origin and loc == known_origin:
                    continue

                tx.run(
                    """
                    MERGE (a:ThreatActor {name: $actor})
                    MERGE (l:Location {name: $location})
                    MERGE (a)-[:TARGETS]->(l)
                    """,
                    actor=actor,
                    location=loc,
                )

    # link actor -> origin country
    for actor in actors:

        canonical = ACTOR_NORMALIZE.get(actor.lower(), actor)
        known = MITRE_ORIGINS.get(canonical)

        if known:
            tx.run(
                """
                MERGE (a:ThreatActor {name: $actor})
                MERGE (c:Country {name: $country})
                MERGE (a)-[:ORIGINATES_FROM]->(c)
            """,
                actor=actor,
                country=known,
            )


class Neo4jLoader:
    """Consumes enriched articles and MITRE data from Kafka and writes them to Neo4j."""

    enriched_consumer: KafkaConsumer
    mitre_consumer: KafkaConsumer

    def __init__(self) -> None:
        """Initialise the Neo4j driver and Kafka consumers from config."""

        config = load_config()
        neo4j = config["neo4j"]
        kafka = config["kafka"]

        self.driver = GraphDatabase.driver(
            neo4j["uri"], auth=(neo4j["user"], neo4j["password"])
        )

        self.enriched_consumer = KafkaConsumer(
            kafka["topics"]["enriched"],
            bootstrap_servers=kafka["bootstrap_servers"],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="neo4j-enriched-group",
        )

        self.mitre_consumer = KafkaConsumer(
            kafka["topics"]["mitre"],
            bootstrap_servers=kafka["bootstrap_servers"],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="neo4j-mitre-group",
        )

        logger.info("Neo4j loader ready.")

    def build_graph(self) -> None:
        """Run the full pipeline: load MITRE groups then enriched articles."""

        logger.info("Loading MITRE ATT&CK groups into graph.")
        self.load_mitre()
        logger.info("Loading enriched articles into graph.")
        self.load_enriched()

    def load_mitre(self) -> None:
        """Poll the MITRE Kafka topic and write each group to Neo4j."""

        count = 0
        while True:
            # stops when no new messages for 5 seconds
            records = self.mitre_consumer.poll(timeout_ms=5000)
            if not records:
                break
            for tp, messages in records.items():
                for message in messages:
                    group = message.value
                    with self.driver.session() as session:
                        session.execute_write(create_mitre_group, group)
                    count += 1
                    logger.info(
                        "MITRE group: %s (%s)", group.get("name"), group.get("mitre_id")
                    )

        logger.info("Seeding origin relationships")

        with self.driver.session() as session:

            for actor_name, country in MITRE_ORIGINS.items():

                display_name = ACTOR_NORMALIZE.get(actor_name.lower(), actor_name)

                session.run(
                    """
                    MATCH (a:ThreatActor)
                    WHERE a.name = $name OR a.name = $display_name
                    MERGE (c:Country {name: $country})
                    MERGE (a)-[:ORIGINATES_FROM]->(c)
                    """,
                    name=actor_name,
                    display_name=display_name,
                    country=country,
                )

        logger.info("Loaded %d MITRE groups into Neo4j.", count)

    def load_enriched(self) -> None:
        """Poll the enriched Kafka topic and write each article to Neo4j."""

        count = 0
        while True:
            # stops when no new messages for 5 seconds
            records = self.enriched_consumer.poll(timeout_ms=5000)
            if not records:
                break
            for tp, messages in records.items():
                for message in messages:
                    article = message.value
                    with self.driver.session() as session:
                        session.execute_write(create_article_graph, article)
                    count += 1
                    logger.info("Graphed: %s", article.get("title", "")[:60])

        logger.info("Loaded %d articles into Neo4j.", count)

    def close(self) -> None:
        """Close the Neo4j driver and both Kafka consumers."""

        self.driver.close()
        self.enriched_consumer.close()
        self.mitre_consumer.close()
        logger.info("Neo4j loader closed.")
