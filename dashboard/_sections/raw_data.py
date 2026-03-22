# dashboard/_sections/raw_data.py
"""Raw Data Dashboard: view, filter, search, and export threat articles."""

import json
import streamlit as st
from dashboard.db import sf_query

# Generic malware labels too broad to be useful as tags
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


def show() -> None:
    """Render Raw Data dashboard with filters, search, tags, and CSV export."""

    st.markdown("<h1>Raw Data</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.6rem;"
        "color:#344858;letter-spacing:0.08em;margin-bottom:20px;'>"
        "ALL INGESTED THREAT ARTICLES — FILTER, SEARCH, EXPORT</div>",
        unsafe_allow_html=True,
    )

    # Filter row: free-text search, source dropdown, score threshold, high-priority toggle
    f1, f2, f3, f4 = st.columns([3, 2, 2, 1])

    with f1:
        search = st.text_input(
            "",
            placeholder="Search title, actor, malware...",
            label_visibility="collapsed",
        )
    with f2:
        sources_df = sf_query(
            "SELECT DISTINCT SOURCE FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES ORDER BY SOURCE"
        )
        sources = (
            ["All"] + sources_df["SOURCE"].tolist() if not sources_df.empty else ["All"]
        )
        source = st.selectbox("Source", sources, label_visibility="collapsed")
    with f3:
        score_min = st.slider("Min score", 0.0, 1.0, 0.0, 0.1)
    with f4:
        st.markdown("<div style='height:28px;'></div>", unsafe_allow_html=True)
        high_only = st.toggle("High only", value=False)

    # Build WHERE clause dynamically from whatever filters are active
    where = [f"RELEVANCE_SCORE >= {score_min}"]
    if source != "All":
        where.append(f"SOURCE = '{source}'")
    if high_only:
        where.append("RELEVANCE_SCORE >= 0.8")

    where_clause = " AND ".join(where)

    # Pull matching articles from Snowflake, newest first, capped at 500
    df = sf_query(
        f"""
        SELECT TITLE, SOURCE, PUBLISHED_AT, RELEVANCE_SCORE,
               THREAT_ACTORS, MALWARE, LOCATIONS, ORIGINAL_URL
        FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES
        WHERE {where_clause}
        ORDER BY PUBLISHED_AT DESC
        LIMIT 500
    """
    )

    if df.empty:
        st.warning("No articles match the current filters.")
        return

    # Narrow results further with client-side search across key text fields
    if search:
        term = search.lower()
        mask = df.apply(
            lambda row: (
                term in str(row["TITLE"]).lower()
                or term in str(row["THREAT_ACTORS"]).lower()
                or term in str(row["MALWARE"]).lower()
                or term in str(row["SOURCE"]).lower()
                or term in str(row["LOCATIONS"]).lower()
            ),
            axis=1,
        )
        df = df[mask]

    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.62rem;"
        "color:#344858;padding:6px 0 12px;'>"
        f"SHOWING <span style='color:#B06EFF;'>{len(df)}</span> ARTICLES"
        "</div>",
        unsafe_allow_html=True,
    )

    # Export the current filtered view as CSV
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇ Export CSV",
        data=csv,
        file_name="threat_articles.csv",
        mime="text/csv",
    )

    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

    # Render each article as a feed row with score badge, title, source, and tags
    feed_html = ""
    for _, row in df.iterrows():
        score = float(row["RELEVANCE_SCORE"])
        title = str(row["TITLE"]).replace("'", "&#39;").replace('"', "&quot;")
        url = str(row["ORIGINAL_URL"])
        source = str(row["SOURCE"])
        date = str(row["PUBLISHED_AT"])[:10]

        # Score badge color: red = high, amber = medium, purple = low
        if score >= 0.85:
            score_color = "#FF4D4D"
        elif score >= 0.65:
            score_color = "#FFB800"
        else:
            score_color = "#B06EFF"

        # Parse and render up to 3 threat actor tags
        actor_tags = ""
        try:
            al = (
                json.loads(row["THREAT_ACTORS"])
                if isinstance(row["THREAT_ACTORS"], str)
                else (row["THREAT_ACTORS"] or [])
            )
            actor_tags = "".join(
                "<span style='display:inline-block;font-family:IBM Plex Mono,monospace;"
                "font-size:0.58rem;padding:1px 6px;border-radius:2px;margin:2px 2px 0 0;"
                "color:#B06EFF;border:1px solid rgba(176,110,255,0.3);"
                "background:rgba(176,110,255,0.06);'>" + str(a) + "</span>"
                for a in al[:3]
            )
        except Exception:
            pass

        # Parse and render up to 3 malware tags, skipping generic names
        mal_tags = ""
        try:
            ml = (
                json.loads(row["MALWARE"])
                if isinstance(row["MALWARE"], str)
                else (row["MALWARE"] or [])
            )
            ml = [m for m in ml if m.lower() not in MALWARE_GENERIC]
            mal_tags = "".join(
                "<span style='display:inline-block;font-family:IBM Plex Mono,monospace;"
                "font-size:0.58rem;padding:1px 6px;border-radius:2px;margin:2px 2px 0 0;"
                "color:#FF4D4D;border:1px solid rgba(255,77,77,0.3);"
                "background:rgba(255,77,77,0.06);'>" + str(m) + "</span>"
                for m in ml[:3]
            )
        except Exception:
            pass

        feed_html += (
            "<div style='display:flex;gap:12px;padding:10px 0;"
            "border-bottom:1px solid #2A1C38;align-items:flex-start;'>"
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
            f"font-weight:600;padding:2px 7px;border-radius:2px;border:1px solid;"
            f"color:{score_color};border-color:{score_color}44;"
            f"background:{score_color}11;white-space:nowrap;margin-top:2px;'>"
            f"{score:.2f}</div>"
            "<div style='flex:1;min-width:0;'>"
            "<div style='font-family:Inter,sans-serif;font-size:0.875rem;"
            "color:#E2EAF0;line-height:1.4;'>"
            f"<a href='{url}' target='_blank' style='color:#E2EAF0;text-decoration:none;'>"
            f"{title}</a></div>"
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.6rem;"
            f"color:#344858;margin-top:4px;'>{source} &nbsp;·&nbsp; {date}</div>"
            + (
                f"<div style='margin-top:4px;'>{actor_tags}{mal_tags}</div>"
                if actor_tags or mal_tags
                else ""
            )
            + "</div></div>"
        )

    st.markdown(feed_html, unsafe_allow_html=True)
