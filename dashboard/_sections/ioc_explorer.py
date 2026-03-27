# dashboard/_sections/ioc_explorer.py
"""IOC Explorer dashboard page — search IOCs, view stats, cross-reference with articles."""

import streamlit as st
from dashboard.db import sf_query
from streamlit_autorefresh import st_autorefresh


def show() -> None:
    """Render the IOC Explorer page."""
    st_autorefresh(interval=5000, key="ioc_auto_refresh")
    st.markdown("## IOC Explorer")

    # Search bar & filters
    col_search, col_type, col_actor, col_limit, col_btn = st.columns([4, 1, 1, 1, 1])
    with col_search:
        search_val = st.text_input(
            "Search",
            placeholder="IP, domain, hash, URL, or malware name...",
            label_visibility="collapsed",
        )
    with col_type:
        ioc_type_filter = st.selectbox(
            "Type",
            ["All", "ip:port", "url", "sha256", "md5", "domain"],
            label_visibility="collapsed",
        )
    with col_actor:
        actor_filter = st.selectbox(
            "Linked Actor",
            ["All", "Has Actor", "No Actor"],
            label_visibility="collapsed",
        )
    with col_limit:
        result_limit = st.selectbox(
            "Results Limit",
            [10, 25, 50, 100, 200],
            index=2,
            label_visibility="collapsed",
        )
    with col_btn:
        search_clicked = st.button("Search", use_container_width=True)

    # Stats cards
    try:
        stats = sf_query(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN ioc_type LIKE 'ip%' THEN 1 ELSE 0 END) as ips,
                SUM(CASE WHEN ioc_type IN ('url', 'domain') THEN 1 ELSE 0 END) as urls,
                SUM(CASE WHEN ioc_type IN ('sha256', 'md5') THEN 1 ELSE 0 END) as hashes,
                COUNT(DISTINCT source) as sources
            FROM THREAT_INTEL.PUBLIC.THREAT_IOCS
        """
        )
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total IOCs", f"{int(stats['TOTAL'][0]):,}")
        c2.metric("IPs", f"{int(stats['IPS'][0]):,}")
        c3.metric("URLs/Domains", f"{int(stats['URLS'][0]):,}")
        c4.metric("Hashes", f"{int(stats['HASHES'][0]):,}")
    except Exception:
        st.info(
            "No IOC data yet. Run `python -m ingestion.run_abuse` then `python -m storage.run_ioc_loader`."
        )
        return

    st.divider()

    # Search results
    if search_clicked:
        type_clause = ""
        if ioc_type_filter != "All":
            type_clause = f"AND ioc_type = '{ioc_type_filter}'"

        actor_clause = ""
        if actor_filter == "Has Actor":
            actor_clause = "AND threat_actor IS NOT NULL AND threat_actor != ''"
        elif actor_filter == "No Actor":
            actor_clause = "AND (threat_actor IS NULL OR threat_actor = '')"

        search_clause = ""
        if search_val:
            search_clause = f"""
                AND (
                    ioc_value ILIKE '%{search_val}%'
                    OR malware_family ILIKE '%{search_val}%'
                    OR threat_actor ILIKE '%{search_val}%'
                )
            """

        results = sf_query(
            f"""
            SELECT *
            FROM THREAT_INTEL.PUBLIC.THREAT_IOCS
            WHERE 1=1
            {type_clause}
            {actor_clause}
            {search_clause}
            ORDER BY first_seen DESC
            LIMIT {result_limit}
        """
        )

        if results.empty:
            st.warning("No IOCs found with the selected filters.")
        else:
            st.success(f"Found {len(results)} IOC(s)")
            for _, row in results.iterrows():
                with st.container(border=True):
                    cols = st.columns([1, 1, 1, 2])
                    cols[0].markdown(f"**{row.get('IOC_TYPE', '')}**")
                    malware = row.get("MALWARE_FAMILY", "")
                    actor = row.get("THREAT_ACTOR", "")
                    if malware:
                        cols[1].markdown(f"🦠 `{malware}`")
                    if actor:
                        cols[1].markdown(f"🎯 `{actor}`")
                    cols[2].markdown(f"📡 {row.get('SOURCE', '')}")
                    cols[3].markdown(f"Confidence: **{row.get('CONFIDENCE', 0)}%**")

                    st.code(row.get("IOC_VALUE", ""), language=None)

                    detail_cols = st.columns(3)
                    detail_cols[0].caption(f"Threat: {row.get('THREAT_TYPE', '')}")
                    detail_cols[1].caption(
                        f"First seen: {str(row.get('FIRST_SEEN', ''))[:19]}"
                    )
                    detail_cols[2].caption(f"Reporter: {row.get('REPORTER', '')}")

                    # Cross-reference with articles
                    cross_ref_terms = []
                    if malware:
                        cross_ref_terms.append(malware)
                    if actor:
                        cross_ref_terms.append(actor)

                    for term in cross_ref_terms:
                        related = sf_query(
                            f"""
                            SELECT t.TITLE, t.SOURCE, t.PUBLISHED_AT, t.RELEVANCE_SCORE
                            FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES t,
                            LATERAL FLATTEN(input => ARRAY_CAT(
                                COALESCE(t.MALWARE, ARRAY_CONSTRUCT()),
                                COALESCE(t.THREAT_ACTORS, ARRAY_CONSTRUCT())
                            )) f
                            WHERE LOWER(f.value::STRING) = LOWER('{term}')
                            ORDER BY t.PUBLISHED_AT DESC
                            LIMIT 5
                        """
                        )
                        if not related.empty:
                            st.caption(f"📰 Related articles mentioning `{term}`:")
                            for _, art in related.iterrows():
                                st.markdown(
                                    f"- **{art['TITLE'][:80]}** — "
                                    f"{art['SOURCE']} · {str(art['PUBLISHED_AT'])[:10]} · "
                                    f"Score: {art['RELEVANCE_SCORE']}"
                                )

    # Top malware by IOC count
    st.markdown("### Top malware families by IOC count")
    top_malware = sf_query(
        """
        SELECT malware_family, COUNT(*) as n
        FROM THREAT_INTEL.PUBLIC.THREAT_IOCS
        WHERE malware_family != '' AND malware_family IS NOT NULL
        GROUP BY malware_family
        ORDER BY n DESC
        LIMIT 15
    """
    )
    if not top_malware.empty:
        st.bar_chart(top_malware.set_index("MALWARE_FAMILY")["N"])

    # Top actors by IOC attribution
    st.markdown("### Top threat actors by IOC attribution")
    top_actors = sf_query(
        """
        SELECT threat_actor, COUNT(*) as n
        FROM THREAT_INTEL.PUBLIC.THREAT_IOCS
        WHERE threat_actor != '' AND threat_actor IS NOT NULL
        GROUP BY threat_actor
        ORDER BY n DESC
        LIMIT 15
    """
    )
    if not top_actors.empty:
        st.bar_chart(top_actors.set_index("THREAT_ACTOR")["N"])

    # Recent IOCs table
    st.markdown("### Recent IOCs")
    recent = sf_query(
        f"""
        SELECT ioc_type, ioc_value, malware_family, threat_actor, source, confidence, first_seen
        FROM THREAT_INTEL.PUBLIC.THREAT_IOCS
        ORDER BY first_seen DESC
        LIMIT {result_limit}
    """
    )
    if not recent.empty:
        st.dataframe(
            recent,
            use_container_width=True,
            hide_index=True,
            column_config={
                "IOC_VALUE": st.column_config.TextColumn("IOC", width="large"),
                "MALWARE_FAMILY": st.column_config.TextColumn("Malware"),
                "THREAT_ACTOR": st.column_config.TextColumn("Actor"),
                "SOURCE": st.column_config.TextColumn("Source"),
                "CONFIDENCE": st.column_config.NumberColumn("Conf %"),
                "FIRST_SEEN": st.column_config.DatetimeColumn(
                    "First seen", format="YYYY-MM-DD HH:mm"
                ),
            },
        )

    # Source breakdown
    st.markdown("### IOC sources")
    sources = sf_query(
        """
        SELECT source, COUNT(*) as n
        FROM THREAT_INTEL.PUBLIC.THREAT_IOCS
        GROUP BY source
        ORDER BY n DESC
    """
    )
    if not sources.empty:
        st.bar_chart(sources.set_index("SOURCE")["N"])
