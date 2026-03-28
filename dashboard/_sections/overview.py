# dashboard/_sections/overview.py
"""Overview Dashboard: Threat Intel summary and visualizations."""

import json
from datetime import datetime, timezone
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
from dashboard.db import sf_query

# Plot defaults
PLOT = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(family="IBM Plex Mono, monospace", color="#7A90A4", size=10),
    margin=dict(l=0, r=0, t=10, b=0),
    xaxis=dict(gridcolor="#2A1C38", zerolinecolor="#2A1C38"),
    yaxis=dict(gridcolor="#2A1C38", zerolinecolor="#2A1C38"),
)

# Generic malware list
MALWARE_GENERIC = {
    "malware",
    "backdoor",
    "trojan",
    "worm",
    "virus",
    "exploit",
    "spyware",
    "ransomware",
    "rootkit",
    "botnet",
    "keylogger",
    "dropper",
    "loader",
    "payload",
    "implant",
}

ACCENT = "#B06EFF"
ACCENT_DIM = "rgba(176,110,255,0.10)"
RED = "#FF4D4D"
AMBER = "#FFB800"
BORDER = "#2A1C38"


def score_style(score: float) -> tuple[str, str]:
    """Color/bg for relevance score."""
    if score >= 0.85:
        return RED, "rgba(255,77,77,0.10)"
    if score >= 0.65:
        return AMBER, "rgba(255,184,0,0.10)"
    return ACCENT, ACCENT_DIM


def build_stat_rows(
    df: pd.DataFrame, name_col: str, val_col: str, bar_color: str | None = None
) -> str:
    """HTML for horizontal bar stats."""
    if bar_color is None:
        bar_color = ACCENT
    html = ""
    max_n = df[val_col].max()
    for _, row in df.iterrows():
        pct = int(row[val_col] / max_n * 100)
        name = str(row[name_col])
        val = int(row[val_col])
        html += (
            "<div class='stat-row'>"
            f"<div class='stat-name'>{name}</div>"
            "<div class='stat-bar-wrap'>"
            f"<div class='stat-bar' style='width:{pct}%;background:{bar_color};'></div>"
            "</div>"
            f"<div class='stat-val'>{val}</div>"
            "</div>"
        )
    return html


def show() -> None:
    """Render overview dashboard."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    st_autorefresh(interval=5000, key="ioc_auto_refresh")

    # Header
    st.markdown(
        "<div style='display:flex;justify-content:space-between;"
        "align-items:flex-end;margin-bottom:20px;'>"
        "<div>"
        "<h1 style='margin:0;'>Threat Intelligence Overview</h1>"
        f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.6rem;"
        f"color:#344858;margin-top:4px;letter-spacing:0.08em;'>LAST UPDATED: {now}</div>"
        "</div>"
        f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.65rem;color:{ACCENT};"
        f"border:1px solid rgba(176,110,255,0.25);padding:3px 10px;border-radius:2px;'>"
        "<span style='display:inline-block;width:6px;height:6px;border-radius:50%;"
        "background:#00E676;box-shadow:0 0 5px #00E676;margin-right:5px;vertical-align:middle;'></span>LIVE"
        "</div></div>",
        unsafe_allow_html=True,
    )

    # Metrics queries
    total = sf_query("SELECT COUNT(*) as n FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES")
    actors_q = sf_query("""
        SELECT COUNT(DISTINCT f.value::STRING) as n
        FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES t,
        LATERAL FLATTEN(input => t.THREAT_ACTORS) f
    """)
    high_q = sf_query(
        "SELECT COUNT(*) as n FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES WHERE RELEVANCE_SCORE >= 0.8"
    )
    avg_q = sf_query(
        "SELECT ROUND(AVG(RELEVANCE_SCORE), 2) as n FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES"
    )

    # Display metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Articles", f"{int(total['N'][0]):,}")
    c2.metric("Threat Actors", int(actors_q["N"][0]))
    c3.metric("High Priority", int(high_q["N"][0]))
    c4.metric("Avg Relevance", float(avg_q["N"][0]))

    st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

    left, right = st.columns([3, 2])

    # Activity timeline chart
    with left:
        st.markdown(
            "<div class='section-label'>Activity Timeline</div>", unsafe_allow_html=True
        )
        timeline = sf_query("""
            SELECT DATE_TRUNC('day', PUBLISHED_AT) as day,
                   COUNT(*) as articles,
                   ROUND(AVG(RELEVANCE_SCORE), 2) as avg_score
            FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES
            WHERE PUBLISHED_AT IS NOT NULL
            GROUP BY day ORDER BY day
        """)
        if not timeline.empty:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=timeline["DAY"],
                    y=timeline["ARTICLES"],
                    mode="lines",
                    line=dict(color=ACCENT, width=1.5),
                    fill="tozeroy",
                    fillcolor="rgba(176,110,255,0.05)",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=timeline["DAY"],
                    y=timeline["AVG_SCORE"] * timeline["ARTICLES"].max(),
                    mode="lines",
                    line=dict(color=RED, width=1, dash="dot"),
                )
            )
            fig.update_layout(
                **PLOT, height=200, showlegend=False, hovermode="x unified"
            )
            st.plotly_chart(fig, use_container_width=True)

    # Top threat actors
    with right:
        st.markdown(
            "<div class='section-label'>Top Threat Actors</div>", unsafe_allow_html=True
        )
        actors_df = sf_query("""
            SELECT f.value::STRING as actor, COUNT(*) as n
            FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES t,
            LATERAL FLATTEN(input => t.THREAT_ACTORS) f
            GROUP BY f.value::STRING
            ORDER BY n DESC LIMIT 8
        """)
        if not actors_df.empty:
            st.markdown(
                build_stat_rows(actors_df, "ACTOR", "N"), unsafe_allow_html=True
            )

    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

    feed_col, side_col = st.columns([3, 2])

    # High-priority incidents feed
    with feed_col:
        st.markdown(
            "<div class='section-label'>Latest High-Priority Incidents</div>",
            unsafe_allow_html=True,
        )
        feed = sf_query("""
            SELECT TITLE, SOURCE, PUBLISHED_AT, RELEVANCE_SCORE,
                   THREAT_ACTORS, ORIGINAL_URL
            FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES
            WHERE RELEVANCE_SCORE >= 0.6
            ORDER BY PUBLISHED_AT DESC LIMIT 15
        """)

        feed_html = ""
        for _, row in feed.iterrows():
            score = float(row["RELEVANCE_SCORE"])
            color, bg = score_style(score)
            title = str(row["TITLE"]).replace("'", "&#39;").replace('"', "&quot;")
            url = str(row["ORIGINAL_URL"])
            source = str(row["SOURCE"])
            date = str(row["PUBLISHED_AT"])[:10]
            score_s = f"{score:.2f}"

            actor_tags = ""
            try:
                al = (
                    json.loads(row["THREAT_ACTORS"])
                    if isinstance(row["THREAT_ACTORS"], str)
                    else (row["THREAT_ACTORS"] or [])
                )
                actor_tags = "".join(
                    "<span class='tag tag-accent'>" + str(a) + "</span>" for a in al[:3]
                )
            except Exception:
                pass

            feed_html += (
                "<div class='feed-item'>"
                "<div class='feed-score' style='"
                f"color:{color};background:{bg};border-color:{color}44;'>"
                f"{score_s}</div>"
                "<div style='flex:1;min-width:0;'>"
                "<div class='feed-title'>"
                f"<a href='{url}' target='_blank'>{title}</a>"
                "</div>"
                "<div class='feed-meta'>"
                f"{source} &nbsp;·&nbsp; {date}"
                + (f" &nbsp;{actor_tags}" if actor_tags else "")
                + "</div></div></div>"
            )

        st.markdown(feed_html, unsafe_allow_html=True)

    # Side panel: Top malware & sources
    with side_col:
        st.markdown(
            "<div class='section-label'>Top Malware</div>", unsafe_allow_html=True
        )
        generic_list = ", ".join(f"'{m}'" for m in MALWARE_GENERIC)
        mal_df = sf_query(f"""
            SELECT f.value::STRING as malware, COUNT(*) as n
            FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES t,
            LATERAL FLATTEN(input => t.MALWARE) f
            WHERE f.value::STRING NOT IN ({generic_list})
            GROUP BY f.value::STRING
            ORDER BY n DESC LIMIT 8
        """)
        if not mal_df.empty:
            st.markdown(
                build_stat_rows(mal_df, "MALWARE", "N", RED), unsafe_allow_html=True
            )

        st.markdown(
            "<div class='section-label' style='margin-top:16px;'>Sources</div>",
            unsafe_allow_html=True,
        )
        src_df = sf_query("""
            SELECT SOURCE, COUNT(*) as n
            FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES
            GROUP BY SOURCE ORDER BY n DESC LIMIT 6
        """)
        if not src_df.empty:
            total_n = src_df["N"].sum()
            src_html = ""
            for _, row in src_df.iterrows():
                pct = int(row["N"] / total_n * 100)
                name = str(row["SOURCE"])
                src_html += (
                    "<div class='stat-row'>"
                    "<div class='stat-name' style='max-width:130px;overflow:hidden;"
                    f"white-space:nowrap;text-overflow:ellipsis;'>{name}</div>"
                    "<div class='stat-bar-wrap'>"
                    f"<div class='stat-bar' style='width:{pct}%;background:#7A90A4;'></div>"
                    "</div>"
                    f"<div class='stat-val'>{pct}%</div>"
                    "</div>"
                )
            st.markdown(src_html, unsafe_allow_html=True)
