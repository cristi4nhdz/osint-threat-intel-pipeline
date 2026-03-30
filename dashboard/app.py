# dashboard/app.py
"""Streamlit entrypoint that renders the threat intelligence dashboard UI."""

import sys
import os
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard._sections import (
    overview,
    threat_map,
    actor_graph,
    actor_intel,
    raw_data,
    ioc_explorer,
    semantic,
)

st.set_page_config(
    page_title="OBSRV",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&family=Inter:wght@300;400;500&display=swap');

:root {
    --bg:          #0D0B14;
    --panel:       #130F1F;
    --border:      #2A1C38;
    --accent:      #B06EFF;
    --accent-dim:  rgba(176,110,255,0.08);
    --red:         #FF4D4D;
    --amber:       #FFB800;
    --green:       #00E676;
    --purple:      #A855F7;
    --text:        #E2EAF0;
    --text-dim:    #7A90A4;
    --text-mute:   #344858;
    --mono:        'IBM Plex Mono', monospace;
    --sans:        'Inter', sans-serif;
}

*, *::before, *::after { box-sizing: border-box; }

.stApp {
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
}

/* hide streamlit chrome */
[data-testid="stHeader"], #MainMenu,
footer, [data-testid="stToolbar"] { display: none !important; }

/* sidebar */
[data-testid="stSidebar"] {
    background: var(--panel) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="collapsedControl"] {
    display: block !important;
    visibility: visible !important;
    opacity: 1 !important;
}

/* headings */
h1 {
    font-family: var(--mono) !important;
    font-size: 1.2rem !important;
    font-weight: 600 !important;
    color: var(--text) !important;
    letter-spacing: 0.05em !important;
    text-transform: uppercase !important;
    margin-bottom: 4px !important;
}

/* metrics */
[data-testid="metric-container"] {
    background: var(--panel) !important;
    border: 1px solid var(--border) !important;
    border-top: 2px solid var(--accent) !important;
    border-radius: 4px !important;
    padding: 14px 16px !important;
}
[data-testid="metric-container"] label {
    font-family: var(--mono) !important;
    font-size: 0.62rem !important;
    color: var(--text-mute) !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: var(--mono) !important;
    font-size: 1.7rem !important;
    font-weight: 600 !important;
    color: var(--accent) !important;
}

/* sidebar nav */
[data-testid="stRadio"] label {
    font-family: var(--mono) !important;
    font-size: 0.72rem !important;
    color: var(--text-dim) !important;
    letter-spacing: 0.05em !important;
    text-transform: uppercase !important;
    padding: 3px 0 !important;
}
[data-testid="stRadio"] label:hover { color: var(--accent) !important; }

/* inputs */
[data-testid="stSelectbox"] > div > div,
.stTextInput > div > div > input,
.stMultiSelect > div > div {
    background: var(--panel) !important;
    border: 1px solid var(--border) !important;
    border-radius: 3px !important;
    font-family: var(--mono) !important;
    font-size: 0.78rem !important;
    color: var(--text) !important;
}
.stTextInput > div > div > input:focus {
    border-color: var(--accent) !important;
    box-shadow: none !important;
}

/* slider */
[data-testid="stSlider"] > div > div > div {
    background: var(--accent) !important;
}

/* toggle */
[data-testid="stToggle"] { accent-color: var(--accent); }

/* dataframe */
[data-testid="stDataFrame"] {
    border: 1px solid var(--border) !important;
    border-radius: 3px !important;
}

/* divider */
hr { border-color: var(--border) !important; margin: 1rem 0 !important; }

/* buttons */
[data-testid="stButton"] > button {
    background: var(--panel) !important;
    border: 1px solid var(--border) !important;
    color: var(--text-dim) !important;
    font-family: var(--mono) !important;
    font-size: 0.68rem !important;
    letter-spacing: 0.05em !important;
    border-radius: 2px !important;
}
[data-testid="stButton"] > button:hover {
    border-color: var(--accent) !important;
    color: var(--accent) !important;
}

/* download button */
[data-testid="stDownloadButton"] > button {
    background: var(--panel) !important;
    border: 1px solid var(--border) !important;
    color: var(--accent) !important;
    font-family: var(--mono) !important;
    font-size: 0.68rem !important;
    letter-spacing: 0.05em !important;
    border-radius: 2px !important;
}
[data-testid="stDownloadButton"] > button:hover {
    border-color: var(--accent) !important;
    background: rgba(176,110,255,0.08) !important;
}

/* reusable components */
.section-label {
    font-family: var(--mono);
    font-size: 0.6rem;
    color: var(--text-mute);
    text-transform: uppercase;
    letter-spacing: 0.14em;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 14px;
}

.stat-row {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 6px 0;
    border-bottom: 1px solid var(--border);
    font-family: var(--mono);
    font-size: 0.73rem;
}
.stat-row:last-child { border-bottom: none; }
.stat-name { color: var(--text-dim); flex-shrink: 0; }
.stat-val  { color: var(--text); font-weight: 500; margin-left: auto; }
.stat-bar-wrap {
    flex: 1; height: 3px;
    background: var(--border); border-radius: 2px;
}
.stat-bar { height: 100%; border-radius: 2px; background: var(--accent); }

.feed-item {
    display: flex;
    gap: 12px;
    padding: 10px 0;
    border-bottom: 1px solid var(--border);
    align-items: flex-start;
}
.feed-item:last-child { border-bottom: none; }
.feed-score {
    font-family: var(--mono);
    font-size: 0.68rem;
    font-weight: 600;
    padding: 2px 7px;
    border-radius: 2px;
    border: 1px solid;
    white-space: nowrap;
    margin-top: 2px;
}
.feed-title {
    font-family: var(--sans);
    font-size: 0.875rem;
    color: var(--text);
    line-height: 1.4;
}
.feed-title a { color: var(--text); text-decoration: none; }
.feed-title a:hover { color: var(--accent); }
.feed-meta {
    font-family: var(--mono);
    font-size: 0.6rem;
    color: var(--text-mute);
    margin-top: 4px;
}

.tag {
    display: inline-block;
    font-family: var(--mono);
    font-size: 0.6rem;
    padding: 1px 7px;
    border-radius: 2px;
    border: 1px solid;
    margin: 2px 2px 0 0;
}
.tag-accent { color: var(--accent); border-color: rgba(176,110,255,0.3);  background: rgba(176,110,255,0.06); }
.tag-red    { color: var(--red);    border-color: rgba(255,77,77,0.3);    background: rgba(255,77,77,0.06); }
.tag-amber  { color: var(--amber);  border-color: rgba(255,184,0,0.3);    background: rgba(255,184,0,0.06); }
.tag-purple { color: var(--purple); border-color: rgba(168,85,247,0.3);   background: rgba(168,85,247,0.06); }
.tag-dim    { color: var(--text-dim); border-color: var(--border); }

.status-dot {
    display: inline-block;
    width: 6px; height: 6px;
    border-radius: 50%;
    margin-right: 5px;
    vertical-align: middle;
}
</style>
""",
    unsafe_allow_html=True,
)


def get_pipeline_status() -> dict:
    """Ping each backend service and return a status/detail pair for each."""

    status = {}

    # Check Snowflake by counting total ingested articles
    try:
        from dashboard.db import sf_query

        r = sf_query("SELECT COUNT(*) as n FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES")
        status["snowflake"] = ("ok", f"{int(r['N'][0]):,} articles")
    except Exception:
        status["snowflake"] = ("err", "disconnected")

    # Check Neo4j by counting threat actor nodes
    try:
        from dashboard.db import get_neo4j

        driver = get_neo4j()
        driver.verify_connectivity()
        with driver.session() as s:
            n = s.run("MATCH (a:ThreatActor) RETURN count(a) as n").single()["n"]
        status["neo4j"] = ("ok", f"{n} actors")
    except Exception:
        status["neo4j"] = ("err", "disconnected")

    # Check Kafka by listing osint.* topics with a short timeout
    try:
        from kafka import KafkaConsumer
        from config.config_loader import load_config

        config = load_config()
        c = KafkaConsumer(
            bootstrap_servers=config["kafka"]["bootstrap_servers"],
            request_timeout_ms=2000,
            api_version_auto_timeout_ms=2000,
        )
        topics = [t for t in c.topics() if t.startswith("osint.")]
        c.close()
        status["kafka"] = ("ok", f"{len(topics)} topics")
    except Exception:
        status["kafka"] = ("err", "disconnected")

    # Check data freshness: warn if >2h old, error if >24h
    try:
        from dashboard.db import sf_query

        r = sf_query(
            "SELECT DATEDIFF('hour', MAX(INSERTED_AT), CURRENT_TIMESTAMP()) as h "
            "FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES"
        )
        h = int(r["H"][0])
        if h < 2:
            status["freshness"] = ("ok", f"{h}h ago")
        elif h < 24:
            status["freshness"] = ("warn", f"{h}h ago")
        else:
            status["freshness"] = ("err", f"{h}h ago")
    except Exception:
        status["freshness"] = ("err", "unknown")

    # IOC count
    try:
        from dashboard.db import sf_query

        r = sf_query("SELECT COUNT(*) as n FROM THREAT_INTEL.PUBLIC.THREAT_IOCS")
        status["iocs"] = ("ok", f"{int(r['N'][0]):,}")
    except Exception:
        status["iocs"] = ("err", "unknown")

    # RAG API check
    try:
        import requests

        response = requests.get("http://localhost:8000/stats", timeout=2)
        if response.ok:
            stats = response.json()
            status["rag"] = ("ok", f"{stats['total_chunks']} chunks")
        else:
            status["rag"] = ("err", "offline")
    except Exception:
        status["rag"] = ("err", "offline")

    return status


PAGES = {
    "🛡  Overview": overview,
    "🗺  Threat Map": threat_map,
    "◈  Actor Graph": actor_graph,
    "🎯  Actor Intelligence": actor_intel,
    "🗄  Raw Data": raw_data,
    "🧬 IOC Explorer": ioc_explorer,
    "🔍 Semantic Search": semantic,
}

with st.sidebar:
    st.markdown(
        """
    <div style='padding:20px 0 16px;border-bottom:1px solid #2A1C38;margin-bottom:8px;'>
        <div style='font-family:IBM Plex Mono,monospace;font-size:0.9rem;
                    font-weight:600;color:#B06EFF;letter-spacing:0.08em;'>OBSRV</div>
        <div style='font-family:IBM Plex Mono,monospace;font-size:0.58rem;
                    color:#344858;letter-spacing:0.08em;margin-top:2px;'>
            // THREAT INTELLIGENCE
        </div>
    </div>
    <div style='font-family:IBM Plex Mono,monospace;font-size:0.58rem;
                color:#344858;text-transform:uppercase;letter-spacing:0.14em;
                padding:8px 0 4px;'>Navigate</div>
    """,
        unsafe_allow_html=True,
    )

    selection = st.radio("", list(PAGES.keys()), label_visibility="collapsed")

    # Live status indicators for each backend service
    st.markdown(
        """
    <div style='margin-top:24px;padding-top:12px;border-top:1px solid #2A1C38;
                font-family:IBM Plex Mono,monospace;font-size:0.58rem;
                color:#344858;text-transform:uppercase;letter-spacing:0.14em;
                padding-bottom:6px;'>Pipeline</div>
    """,
        unsafe_allow_html=True,
    )

    status = get_pipeline_status()

    STATUS_COLORS = {
        "ok": ("#00E676", "0 0 5px #00E676"),
        "warn": ("#FFB800", "0 0 5px #FFB800"),
        "err": ("#FF4D4D", "0 0 5px #FF4D4D"),
    }

    STATUS_LABELS = {
        "snowflake": "Snowflake",
        "neo4j": "Neo4j",
        "kafka": "Kafka",
        "freshness": "Freshness",
        "iocs": "IOCs",
        "rag": "RAG API",
    }

    # Build status rows as inline HTML with colored indicator dots
    rows_html = ""
    for key, label in STATUS_LABELS.items():
        state, detail = status.get(key, ("err", "unknown"))
        color, shadow = STATUS_COLORS[state]
        rows_html += (
            "<div style='display:flex;justify-content:space-between;"
            "align-items:center;padding:4px 0;"
            "border-bottom:1px solid #1A1228;"
            "font-family:IBM Plex Mono,monospace;'>"
            "<div style='display:flex;align-items:center;gap:6px;'>"
            f"<span style='display:inline-block;width:5px;height:5px;"
            f"border-radius:50%;background:{color};box-shadow:{shadow};'></span>"
            f"<span style='font-size:0.6rem;color:#7A90A4;'>{label}</span>"
            "</div>"
            f"<span style='font-size:0.58rem;color:{color};'>{detail}</span>"
            "</div>"
        )

    st.markdown(rows_html, unsafe_allow_html=True)

PAGES[selection].show()
