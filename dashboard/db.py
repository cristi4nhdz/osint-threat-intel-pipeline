# dashboard/db.py
"""Database connection helpers for the Streamlit dashboard."""

import streamlit as st
import snowflake.connector
from neo4j import GraphDatabase
from config.config_loader import load_config
import pandas as pd


@st.cache_resource
def get_snowflake():
    """Create and cache snowflake connection"""

    config = load_config()
    sf = config["snowflake"]
    return snowflake.connector.connect(
        account=sf["account"],
        user=sf["user"],
        password=sf["password"],
        database=sf["database"],
        schema=sf["schema"],
        warehouse=sf["warehouse"],
    )


@st.cache_resource
def get_neo4j():
    """Create and cache Neo4j driver."""

    config = load_config()
    n = config["neo4j"]
    return GraphDatabase.driver(n["uri"], auth=(n["user"], n["password"]))


def get_rag_api_url() -> str:
    """Get RAG API URL from config."""
    config = load_config()
    return config["rag"]["api_url"]


def sf_query(sql: str) -> pd.DataFrame:
    """Run a Snowflake query and return a DataFrame."""
    conn = get_snowflake()
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def neo4j_query(cypher: str, **params) -> list[dict]:
    """Run a Cypher query and return a list of dicts."""
    driver = get_neo4j()
    with driver.session() as session:
        return session.run(cypher, **params).data()
