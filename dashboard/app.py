# dashboard/app.py
"""Streamlit entrypoint that renders the threat intelligence dashboard UI."""

import sys
import os
import streamlit as st
from dashboard._sections import overview, threat_map, actor_intel, raw_data

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

/* ── reusable components ── */
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

PAGES = {
    "🛡  Overview": overview,
    "🗺  Threat Map": threat_map,
    "🎯  Actor Intelligence": actor_intel,
    "🗄  Raw Data": raw_data,
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

PAGES[selection].show()
