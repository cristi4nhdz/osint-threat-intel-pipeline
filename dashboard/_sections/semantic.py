# dashboard/_sections/semantic.py
"""Semantic Search: AI Analyst page with RAG-powered semantic search."""

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


def render():
    """Render the semantic search page with RAG-powered query interface."""
    st.title("Threat Analyst")
    st.markdown("**Semantic search across threat intelligence articles using RAG**")

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
        with st.spinner("Searching knowledge base"):
            try:
                start_time = time.time()
                response = requests.post(
                    f"{RAG_API_URL}/search",
                    json={"question": query, "n_results": 10},
                    timeout=30,
                )

                # Check for 503 before raise_for_status
                if response.status_code == 503:
                    st.warning(
                        "⏳ RAG API is loading embeddings from Snowflake. This happens automatically when data becomes available. Please try again in a minute."
                    )
                    return

                response.raise_for_status()
                data = response.json()
                latency = time.time() - start_time

                # Update history
                if query not in st.session_state.query_history:
                    st.session_state.query_history.insert(0, query)
                    st.session_state.query_history = st.session_state.query_history[:10]

                st.success(f"Found {data['total_results']} results in {latency:.2f}s")

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
                    csv_lines = ["Query,Source,Similarity,Actors,Malware,URL"]
                    for r in data["results"]:
                        actors = "|".join(r["actors"]) if r["actors"] else "None"
                        malware = "|".join(r["malware"]) if r["malware"] else "None"
                        csv_lines.append(
                            f'"{query}","{r["source"]}",{r["similarity"]},"{actors}","{malware}","{r["url"]}"'
                        )
                    st.download_button(
                        "Export CSV",
                        "\n".join(csv_lines),
                        file_name=f"search_results_{int(time.time())}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )

                st.divider()
                st.markdown("### Search Results")

                for i, result in enumerate(data["results"], 1):
                    with st.expander(
                        f"Result {i} ({result['similarity']:.1%} relevant) - {result['source']}",
                        expanded=(i == 1),
                    ):
                        st.markdown(highlight_keywords(result["text"], query))
                        st.divider()

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.markdown("**Threat Actors:**")
                            if result["actors"]:
                                for actor in result["actors"]:
                                    st.markdown(f"- {actor}")
                            else:
                                st.markdown("None identified")
                        with col2:
                            st.markdown("**Malware:**")
                            if result["malware"]:
                                for mal in result["malware"]:
                                    st.markdown(f"- {mal}")
                            else:
                                st.markdown("None identified")
                        with col3:
                            st.markdown("**Source:**")
                            st.markdown(result["source"])
                            if result["url"]:
                                st.markdown(f"[View Article]({result['url']})")

                st.info(f"Showing top {len(data['results'])} most relevant results")

            except requests.exceptions.ConnectionError:
                st.error(f"Cannot connect to RAG API at {RAG_API_URL}")
            except requests.exceptions.Timeout:
                st.error("Search timed out")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 503:
                    st.warning(
                        "⏳ RAG API is loading embeddings from Snowflake. This happens automatically when data becomes available. Please try again in a minute."
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
