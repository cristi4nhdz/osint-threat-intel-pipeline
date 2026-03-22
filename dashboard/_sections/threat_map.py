# dashboard/_sections/threat_map.py
"""Threat Map: Geo visualization of threat articles by location."""

import random
import plotly.graph_objects as go
import streamlit as st
from dashboard.db import sf_query

# Hard-coded lat/lon for common countries/regions
GEO = {
    "united states": (37.09, -95.71),
    "china": (35.86, 104.19),
    "russia": (61.52, 105.31),
    "ukraine": (48.37, 31.16),
    "iran": (32.42, 53.68),
    "israel": (31.04, 34.85),
    "north korea": (40.33, 127.51),
    "india": (20.59, 78.96),
    "pakistan": (30.37, 69.34),
    "europe": (54.52, 15.25),
    "united kingdom": (55.37, -3.43),
    "germany": (51.16, 10.45),
    "france": (46.22, 2.21),
    "south korea": (35.90, 127.76),
    "japan": (36.20, 138.25),
    "taiwan": (23.69, 120.96),
    "middle east": (29.31, 42.45),
    "southeast asia": (10.0, 106.0),
    "iraq": (33.22, 43.67),
    "saudi arabia": (23.88, 45.07),
    "united arab emirates": (23.42, 53.84),
    "canada": (56.13, -106.34),
    "brazil": (-14.23, -51.92),
    "australia": (-25.27, 133.77),
    "turkey": (38.96, 35.24),
    "poland": (51.91, 19.14),
    "jordan": (30.58, 36.23),
    "egypt": (26.82, 30.80),
    "south america": (-8.78, -55.49),
    "africa": (8.78, 34.50),
    "vietnam": (14.05, 108.27),
    "lebanon": (33.85, 35.86),
    "palestine": (31.95, 35.23),
    "colombia": (4.57, -74.29),
    "belarus": (53.71, 27.95),
    "italy": (41.87, 12.56),
    "spain": (40.46, -3.74),
    "netherlands": (52.13, 5.29),
    "sweden": (60.12, 18.64),
    "switzerland": (46.81, 8.22),
    "singapore": (1.35, 103.82),
    "hong kong": (22.30, 114.17),
    "indonesia": (-0.78, 113.92),
    "philippines": (12.87, 121.77),
    "thailand": (15.87, 100.99),
    "malaysia": (4.21, 101.97),
    "kenya": (-0.02, 37.90),
    "nigeria": (9.08, 8.67),
    "south africa": (-30.55, 22.93),
    "azerbaijan": (40.14, 47.57),
    "georgia": (42.31, 43.35),
    "moldova": (47.41, 28.37),
    "romania": (45.94, 24.96),
    "bulgaria": (42.73, 25.48),
    "czechia": (49.81, 15.47),
    "austria": (47.51, 14.55),
    "belgium": (50.50, 4.46),
    "norway": (60.47, 8.46),
    "finland": (61.92, 25.74),
    "denmark": (56.26, 9.50),
    "greece": (39.07, 21.82),
}

# Base layout for Plotly maps
PLOT_BASE = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(family="IBM Plex Mono, monospace", color="#7A90A4", size=10),
    margin=dict(l=0, r=0, t=0, b=0),
)


def show() -> None:
    """Query Snowflake, build map data, and render Plotly threat map."""

    st.markdown("<h1>Threat Map</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.6rem;"
        "color:#344858;letter-spacing:0.08em;margin-bottom:20px;'>"
        "GLOBAL THREAT ACTIVITY — GEOLOCATED EVENTS BY TARGETED LOCATION</div>",
        unsafe_allow_html=True,
    )

    # Fetch articles with location & relevance
    map_df = sf_query(
        """
        SELECT t.TITLE, t.SOURCE, t.RELEVANCE_SCORE, t.ORIGINAL_URL,
               f.value::STRING as location
        FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES t,
        LATERAL FLATTEN(input => t.LOCATIONS) f
        WHERE t.RELEVANCE_SCORE >= 0.3
        ORDER BY t.RELEVANCE_SCORE DESC
    """
    )

    # Prepare scattergeo data
    lats, lons, texts, scores, sizes = [], [], [], [], []
    for _, row in map_df.iterrows():
        loc = str(row["LOCATION"]).strip().lower()
        coords = GEO.get(loc)
        if not coords:
            continue  # skip unknown
        lat = coords[0] + random.uniform(-0.8, 0.8)
        lon = coords[1] + random.uniform(-0.8, 0.8)
        score = float(row["RELEVANCE_SCORE"])
        title = str(row["TITLE"])
        title_s = title[:65] + "…" if len(title) > 65 else title
        lats.append(lat)
        lons.append(lon)
        scores.append(score)
        sizes.append(6 + score * 16)
        texts.append(
            "<b>"
            + row["LOCATION"].title()
            + "</b><br>"
            + title_s
            + "<br>"
            + "<span style='color:#aaa'>Source: "
            + str(row["SOURCE"])
            + "</span><br>"
            + "Score: "
            + f"{score:.2f}"
        )

    fig = go.Figure()
    if lats:
        fig.add_trace(
            go.Scattergeo(
                lat=lats,
                lon=lons,
                mode="markers",
                marker=dict(
                    size=sizes,
                    color=scores,
                    colorscale=[
                        [0.0, "rgba(42,28,56,0.6)"],
                        [0.5, "rgba(176,110,255,0.8)"],
                        [1.0, "#FF4D4D"],
                    ],
                    cmin=0,
                    cmax=1,
                    colorbar=dict(
                        title=dict(text="Score", font=dict(color="#7A90A4", size=10)),
                        tickfont=dict(color="#7A90A4", size=9),
                        bgcolor="rgba(19,15,31,0.8)",
                        bordercolor="#2A1C38",
                        thickness=10,
                        len=0.5,
                    ),
                    line=dict(color="rgba(176,110,255,0.15)", width=0.5),
                    opacity=0.9,
                ),
                hovertemplate="%{text}<extra></extra>",
                text=texts,
            )
        )
    else:
        st.info("No location data yet. Run the pipeline to populate the map.")
        return

    # Map layout
    fig.update_layout(
        **PLOT_BASE,
        height=600,
        geo=dict(
            showframe=False,
            showcoastlines=True,
            coastlinecolor="rgba(42,28,56,0.8)",
            showland=True,
            landcolor="#110D1A",
            showocean=True,
            oceancolor="#0A0810",
            showlakes=False,
            showcountries=True,
            countrycolor="rgba(42,28,56,0.5)",
            bgcolor="rgba(0,0,0,0)",
            projection_type="natural earth",
        ),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Top targeted locations
    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='section-label'>Top Targeted Locations</div>",
        unsafe_allow_html=True,
    )

    loc_df = sf_query(
        """
        SELECT f.value::STRING as location, COUNT(*) as n
        FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES t,
        LATERAL FLATTEN(input => t.LOCATIONS) f
        GROUP BY f.value::STRING
        ORDER BY n DESC
        LIMIT 12
    """
    )

    # Render horizontal bars
    if not loc_df.empty:
        max_n = loc_df["N"].max()
        rows_html = ""
        for _, row in loc_df.iterrows():
            pct = int(row["N"] / max_n * 100)
            name = str(row["LOCATION"])
            rows_html += (
                "<div class='stat-row'>"
                f"<div class='stat-name'>{name}</div>"
                "<div class='stat-bar-wrap'>"
                f"<div class='stat-bar' style='width:{pct}%;background:#FF4D4D;'></div>"
                "</div>"
                f"<div class='stat-val'>{int(row['N'])}</div>"
                "</div>"
            )
        st.markdown(rows_html, unsafe_allow_html=True)
