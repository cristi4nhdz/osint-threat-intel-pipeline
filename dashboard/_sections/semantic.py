# dashboard/_sections/semantic.py
"""Semantic Search: Threat Analysis page with RAG-powered semantic search."""

import re
import time
import json
import requests
import streamlit as st
from dashboard.db import get_rag_api_url

RAG_API_URL = get_rag_api_url()


def highlight_keywords(text: str, query: str) -> str:
    """Highlight query keywords in text."""
    highlighted = text
    for keyword in query.lower().split():
        if len(keyword) > 3:
            pattern = re.compile(f"({re.escape(keyword)})", re.IGNORECASE)
            highlighted = pattern.sub(r"**\1**", highlighted)
    return highlighted


def parse_result_text(text: str) -> dict:
    """Parse enriched result text into structured fields."""
    parsed = {
        "title": text.strip(),
        "source": "",
        "actors": [],
        "malware": [],
        "locations": [],
    }

    for line in text.splitlines():
        line = line.strip()
        if line.startswith("Title:"):
            parsed["title"] = line.replace("Title:", "", 1).strip()
        elif line.startswith("Source:"):
            parsed["source"] = line.replace("Source:", "", 1).strip()
        elif line.startswith("Threat Actors:"):
            actors = line.replace("Threat Actors:", "", 1).strip()
            parsed["actors"] = [a.strip() for a in actors.split(",") if a.strip()]
        elif line.startswith("Malware:"):
            malware = line.replace("Malware:", "", 1).strip()
            parsed["malware"] = [m.strip() for m in malware.split(",") if m.strip()]
        elif line.startswith("Locations:"):
            locations = line.replace("Locations:", "", 1).strip()
            parsed["locations"] = [l.strip() for l in locations.split(",") if l.strip()]

    return parsed


def match_badge(label: str) -> str:
    """Return colored text label instead of emojis."""
    styles = {
        "strong match": '<span style="color:#16a34a; font-weight:600;">strong match</span>',
        "relevant": '<span style="color:#ca8a04; font-weight:600;">relevant</span>',
        "mentioned": '<span style="color:#6b7280; font-weight:500;">mentioned</span>',
    }
    return styles.get(label, label)


def render_analysis(analysis: dict) -> None:
    """Render generated threat analysis."""
    if not analysis:
        return

    st.divider()
    st.markdown("### Threat Analysis")

    status = analysis.get("status", "")
    summary = analysis.get("summary", "").strip()
    why_it_matters = analysis.get("why_it_matters", "").strip()
    recent_activity = analysis.get("recent_activity", []) or []
    key_entities = analysis.get("key_entities", {}) or {}

    if status == "unavailable":
        st.warning(summary or "Threat analysis is currently unavailable.")
        return

    if status == "low_confidence":
        st.warning(summary or "The retrieved intelligence is low confidence.")
    elif summary:
        st.markdown(summary)

    if why_it_matters:
        st.markdown("#### Why It Matters")
        st.markdown(why_it_matters)

    if recent_activity:
        st.markdown("#### Recent Activity")
        for item in recent_activity:
            st.markdown(f"- {item}")

    actors = key_entities.get("actors", []) or []
    malware = key_entities.get("malware", []) or []
    locations = key_entities.get("locations", []) or []
    sources = key_entities.get("sources", []) or []

    if actors or malware or locations or sources:
        st.markdown("#### Key Entities")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown("**Actors**")
            if actors:
                for actor in actors:
                    st.markdown(f"- {actor}")
            else:
                st.caption("None identified")

        with col2:
            st.markdown("**Malware**")
            if malware:
                for item in malware:
                    st.markdown(f"- {item}")
            else:
                st.caption("None identified")

        with col3:
            st.markdown("**Locations**")
            if locations:
                for item in locations:
                    st.markdown(f"- {item}")
            else:
                st.caption("None identified")

        with col4:
            st.markdown("**Sources**")
            if sources:
                for item in sources:
                    st.markdown(f"- {item}")
            else:
                st.caption("None identified")


def render_results(data: dict, query: str) -> None:
    """Render supporting intelligence results."""
    results = data.get("results", [])
    if not results:
        st.info("No supporting intelligence found.")
        return

    st.divider()
    st.markdown("### Supporting Intelligence")

    for i, result in enumerate(results, 1):
        parsed = parse_result_text(result["text"])
        label = match_badge(result.get("match_label", "mentioned"))
        source = result.get("source") or parsed.get("source") or "Unknown"

        with st.expander(
            f"Result {i} - {source}",
            expanded=(i == 1),
        ):
            st.markdown(f"**Match:** {label}", unsafe_allow_html=True)
            st.markdown(f"**Title:** {highlight_keywords(parsed['title'], query)}")

            meta_col1, meta_col2, meta_col3 = st.columns(3)

            with meta_col1:
                st.markdown("**Threat Actors**")
                actors = result.get("actors") or parsed.get("actors") or []
                if actors:
                    for actor in actors:
                        st.markdown(f"- {actor}")
                else:
                    st.caption("None identified")

            with meta_col2:
                st.markdown("**Malware**")
                malware = result.get("malware") or parsed.get("malware") or []
                if malware:
                    for item in malware:
                        st.markdown(f"- {item}")
                else:
                    st.caption("None identified")

            with meta_col3:
                st.markdown("**Source**")
                st.markdown(source)
                if result.get("url"):
                    st.markdown(f"[View Article]({result['url']})")

            locations = parsed.get("locations") or []
            if locations:
                st.markdown("**Locations**")
                for location in locations:
                    st.markdown(f"- {location}")

    displayed = data.get("displayed_count", len(results))
    st.info(f"Showing {displayed} supporting matches")


def render():
    """Render the semantic search page with RAG-powered query interface."""
    st.title("Threat Analysis")
    st.markdown(
        "**Grounded threat analysis across threat intelligence articles using RAG**"
    )

    if "query_history" not in st.session_state:
        st.session_state.query_history = []

    st.markdown("**Example queries:**")
    st.caption("What is APT28? • Ransomware campaigns • North Korean threat actors")

    query = st.text_input(
        "Ask a question about threat intelligence:",
        placeholder="e.g., What is APT28 doing? Tell me about North Korean threat actors.",
        key="query_input",
    )

    col1, col2 = st.columns([3, 1])
    with col1:
        search_clicked = st.button("Search", type="primary", use_container_width=True)
    with col2:
        if st.session_state.query_history and st.button(
            "Clear", use_container_width=True
        ):
            st.session_state.query_history = []

    if search_clicked and query:
        with st.spinner("Analyzing threat intelligence"):
            try:
                start_time = time.time()
                response = requests.post(
                    f"{RAG_API_URL}/search",
                    json={"question": query, "n_results": 10},
                    timeout=60,
                )

                # Check for 503 before raise_for_status
                if response.status_code == 503:
                    st.warning(
                        "RAG API is loading embeddings from Snowflake. Please try again in a minute."
                    )
                    return

                response.raise_for_status()
                data = response.json()
                latency = time.time() - start_time

                # Update history
                if query not in st.session_state.query_history:
                    st.session_state.query_history.insert(0, query)
                    st.session_state.query_history = st.session_state.query_history[:10]

                st.success(
                    f"Found {data.get('displayed_count', data.get('total_results', 0))} results in {latency:.2f}s"
                )

                # Export buttons
                _, col2, col3 = st.columns([2, 1, 1])
                with col2:
                    st.download_button(
                        "Export JSON",
                        json.dumps(data, indent=2),
                        file_name=f"search_results_{int(time.time())}.json",
                        mime="application/json",
                        use_container_width=True,
                    )
                with col3:
                    csv_lines = [
                        "Query,Source,MatchLabel,Similarity,Actors,Malware,URL"
                    ]
                    for r in data.get("results", []):
                        actors = "|".join(r["actors"]) if r.get("actors") else "None"
                        malware = "|".join(r["malware"]) if r.get("malware") else "None"
                        csv_lines.append(
                            f'"{query}","{r["source"]}","{r.get("match_label", "")}",{r["similarity"]},"{actors}","{malware}","{r["url"]}"'
                        )
                    st.download_button(
                        "Export CSV",
                        "\n".join(csv_lines),
                        file_name=f"search_results_{int(time.time())}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )

                render_analysis(data.get("analysis"))
                render_results(data, query)

            except requests.exceptions.ConnectionError:
                st.error(f"Cannot connect to RAG API at {RAG_API_URL}")
            except requests.exceptions.Timeout:
                st.error("Analysis timed out")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 503:
                    st.warning(
                        "RAG API is loading embeddings from Snowflake. Please try again in a minute."
                    )
                else:
                    st.error(f"Search failed: {str(e)}")
            except Exception as e:
                st.error(f"Search failed: {str(e)}")

    if st.session_state.query_history:
        st.divider()
        st.markdown("### Recent Searches")
        st.caption("Click to search again:")
        for q in st.session_state.query_history[:5]:
            st.code(q, language=None)


def show():
    """Entry point for the semantic search dashboard section."""
    render()
