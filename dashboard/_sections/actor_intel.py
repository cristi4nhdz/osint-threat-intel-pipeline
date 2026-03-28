# dashboard/_sections/actor_intel.py
"""Actor Intelligence: MITRE ATT&CK + live threat overlay."""

import json
import re
import streamlit as st
import streamlit.components.v1 as components
from dashboard.db import neo4j_query, sf_query

# Generic malware class names to filter out — too vague to be useful
MALWARE_GENERIC = {
    "malware",
    "backdoor",
    "trojan",
    "worm",
    "exploit",
    "spyware",
    "ransomware",
    "loader",
    "dropper",
    "botnet",
    "rootkit",
    "keylogger",
    "virus",
    "adware",
    "stealer",
    "infostealer",
    "implant",
    "payload",
}


def clean_desc(text: str | None) -> str:
    """Clean and format threat actor descriptions for display."""

    if not text:
        return ""
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)  # strip markdown links
    text = re.sub(r"\(Citation:[^)]+\)", "", text)  # remove citation refs
    text = re.sub(r"  +", " ", text).strip()
    return text


def get_groups() -> list[dict]:
    """Fetch threat actor nodes with MITRE IDs and related metadata from Neo4j."""

    return neo4j_query("""
        MATCH (a:ThreatActor)
        WHERE a.mitre_id IS NOT NULL
        OPTIONAL MATCH (a)-[:ORIGINATES_FROM]->(c:Country)
        RETURN a.name as name, a.mitre_id as mitre_id,
               a.aliases as aliases, a.description as description,
               a.url as url, collect(DISTINCT c.name) as origins
        ORDER BY a.name
    """)


def get_relationships() -> tuple[
    dict[str, list[str]],
    dict[str, list[str]],
    dict[str, list[dict]],
]:
    """Retrieve malware, location, and article relationships for threat actors."""

    # Fetch malware via direct links and via actor aliases
    mal_direct = neo4j_query("""
        MATCH (a:ThreatActor)-[:USES]->(m:Malware)
        WHERE a.mitre_id IS NOT NULL
        RETURN a.name as actor, m.name as malware
    """)
    mal_alias = neo4j_query("""
        MATCH (mitre:ThreatActor)
        WHERE mitre.mitre_id IS NOT NULL AND mitre.aliases IS NOT NULL
        UNWIND mitre.aliases AS alias
        MATCH (n:ThreatActor {name:alias})-[:USES]->(m:Malware)
        WHERE n.mitre_id IS NULL
        RETURN mitre.name as actor, m.name as malware
    """)
    loc_direct = neo4j_query("""
        MATCH (a:ThreatActor)-[:TARGETS]->(l:Location)
        WHERE a.mitre_id IS NOT NULL
        RETURN a.name as actor, l.name as location
    """)
    loc_alias = neo4j_query("""
        MATCH (mitre:ThreatActor)
        WHERE mitre.mitre_id IS NOT NULL AND mitre.aliases IS NOT NULL
        UNWIND mitre.aliases AS alias
        MATCH (n:ThreatActor {name:alias})-[:TARGETS]->(l:Location)
        WHERE n.mitre_id IS NULL
        RETURN mitre.name as actor, l.name as location
    """)
    art_direct = neo4j_query("""
        MATCH (art:Article)-[:MENTIONS_ACTOR]->(a:ThreatActor)
        WHERE a.mitre_id IS NOT NULL
        RETURN a.name as actor, art.title as title,
               art.url as url, art.source as source
    """)
    art_alias = neo4j_query("""
        MATCH (mitre:ThreatActor)
        WHERE mitre.mitre_id IS NOT NULL AND mitre.aliases IS NOT NULL
        UNWIND mitre.aliases AS alias
        MATCH (art:Article)-[:MENTIONS_ACTOR]->(n:ThreatActor {name:alias})
        WHERE n.mitre_id IS NULL
        RETURN mitre.name as actor, art.title as title,
               art.url as url, art.source as source
    """)

    mal_map: dict[str, set[str]] = {}
    loc_map: dict[str, set[str]] = {}
    art_map: dict[str, list[dict[str, str]]] = {}
    seen_urls: dict[str, set[str]] = {}  # deduplicate articles per actor by URL

    for r in mal_direct + mal_alias:
        mal_map.setdefault(r["actor"], set()).add(r["malware"])
    for r in loc_direct + loc_alias:
        loc_map.setdefault(r["actor"], set()).add(r["location"])
    for r in art_direct + art_alias:
        actor = r["actor"]
        url = r["url"] or ""
        seen_urls.setdefault(actor, set())
        if url not in seen_urls[actor]:
            seen_urls[actor].add(url)
            art_map.setdefault(actor, []).append(
                {"title": r["title"], "url": url, "source": r["source"]}
            )
    return (
        {k: list(v) for k, v in mal_map.items()},
        {k: list(v) for k, v in loc_map.items()},
        art_map,
    )


def build_accordion_data(
    groups: list[dict],
    mal_map: dict[str, list[str]],
    loc_map: dict[str, list[str]],
    art_map: dict[str, list[dict]],
    linked_actors: set[str],
) -> str:
    """Prepare JSON data for the actor accordion UI in Streamlit."""

    data = []
    for g in groups:
        name = g["name"]
        mid = g["mitre_id"] or ""
        aliases = g["aliases"] or []
        origins = g["origins"] or []
        desc = clean_desc(g["description"] or "")[:320]
        url = g.get("url", "") or ""
        malware = [
            m for m in mal_map.get(name, []) if m.lower() not in MALWARE_GENERIC
        ][:10]
        locs = loc_map.get(name, [])[:10]
        articles = art_map.get(name, [])[:6]
        # linked = actor has live intel articles or appears in Snowflake pipeline
        linked = bool(articles) or name.upper() in linked_actors
        data.append(
            {
                "name": name,
                "mid": mid,
                "aliases": " · ".join(str(a) for a in aliases[:5]),
                "origins": origins,
                "desc": desc,
                "url": url,
                "linked": linked,
                "malware": malware,
                "locs": locs,
                "articles": articles,
            }
        )
    return json.dumps(data)


def show() -> None:
    """Render the Actor Intelligence page with search, filters, and accordion display."""

    st.markdown("<h1>Actor Intelligence</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.6rem;"
        "color:#344858;letter-spacing:0.08em;margin-bottom:20px;'>"
        "MITRE ATT&CK DATABASE WITH LIVE PIPELINE INTELLIGENCE OVERLAY</div>",
        unsafe_allow_html=True,
    )

    with st.spinner("Loading actor data..."):
        groups = get_groups()
        mal_map, loc_map, art_map = get_relationships()

    # Actors present in the live Snowflake pipeline (used to flag INTEL badge)
    linked_df = sf_query("""
        SELECT DISTINCT f.value::STRING as actor
        FROM THREAT_INTEL.PUBLIC.THREAT_ARTICLES t,
        LATERAL FLATTEN(input => t.THREAT_ACTORS) f
    """)
    linked_actors = (
        set(linked_df["ACTOR"].str.upper().tolist()) if not linked_df.empty else set()
    )

    if not groups:
        st.warning(
            "No MITRE data. Run: python -m ingestion.run_mitre && python -m storage.run_neo4j"
        )
        return

    fc1, fc2 = st.columns([4, 1])
    with fc1:
        search = st.text_input(
            "",
            placeholder="Search actor, alias, MITRE ID, origin country...",
            label_visibility="collapsed",
        )
    with fc2:
        only_linked = st.toggle("Intel only", value=False)

    filtered = groups
    if search:
        term = search.lower()
        filtered = [
            g
            for g in groups
            if term in g["name"].lower()
            or (g["mitre_id"] and term in g["mitre_id"].lower())
            or (g["aliases"] and any(term in str(a).lower() for a in g["aliases"]))
            or (g["description"] and term in g["description"].lower())
            or any(term in o.lower() for o in (g["origins"] or []))
        ]
    if only_linked:
        filtered = [
            g
            for g in filtered
            if g["name"].upper() in linked_actors or bool(art_map.get(g["name"]))
        ]

    linked_count = sum(
        1
        for g in groups
        if g["name"].upper() in linked_actors or bool(art_map.get(g["name"]))
    )

    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.62rem;"
        "color:#344858;padding:6px 0 12px;'>"
        f"SHOWING <span style='color:#B06EFF;'>{len(filtered)}</span> OF "
        f"<span style='color:#7A90A4;'>{len(groups)}</span> GROUPS &nbsp;·&nbsp; "
        f"<span style='color:#00E676;'>{linked_count}</span> WITH LIVE INTEL"
        "</div>",
        unsafe_allow_html=True,
    )

    data_json = build_accordion_data(filtered, mal_map, loc_map, art_map, linked_actors)

    html = f"""<!DOCTYPE html>
<html>
<head>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=Inter:wght@400;500&display=swap');
*{{ margin:0; padding:0; box-sizing:border-box; }}
body{{ background:#0D0B14; font-family:'IBM Plex Mono',monospace; padding:0 0 20px; }}
.row{{
    padding:9px 14px; margin-bottom:2px;
    background:#130F1F; border:1px solid #2A1C38;
    border-left:3px solid #2A1C38; border-radius:2px;
    cursor:pointer; transition:border-color 0.12s, background 0.12s;
    user-select:none;
}}
.row:hover{{ border-left-color:#7A90A4; background:#1A1228; }}
.row.linked{{ border-left-color:#1A2A1A; background:rgba(0,230,118,0.02); }}
.row.linked:hover{{ border-left-color:#00E676; }}
.row.open{{ border-left-color:#B06EFF !important; background:rgba(176,110,255,0.04) !important; }}
.row-header{{ display:flex; justify-content:space-between; align-items:center; }}
.row-left{{ display:flex; align-items:center; gap:8px; flex-wrap:wrap; }}
.mid{{ font-size:0.6rem; color:#344858; min-width:46px; }}
.rname{{ font-size:0.78rem; font-weight:500; color:#E2EAF0; }}
.badge{{ font-size:0.55rem; color:#00E676; border:1px solid rgba(0,230,118,0.3); padding:1px 6px; border-radius:1px; }}
.otag{{ font-size:0.55rem; color:#FFB800; border:1px solid rgba(255,184,0,0.3); padding:1px 6px; border-radius:1px; }}
.chev{{ font-size:0.65rem; color:#344858; transition:transform 0.2s; flex-shrink:0; }}
.row.open .chev{{ transform:rotate(180deg); color:#B06EFF; }}
.aliases{{ font-size:0.6rem; color:#344858; margin-top:2px; }}
.detail{{
    display:none; background:#100D1A;
    border:1px solid #2A1C38; border-top:none;
    border-radius:0 0 2px 2px;
    padding:14px 16px; margin-bottom:2px; margin-top:-2px;
}}
.detail.open{{ display:block; }}
.desc{{ font-family:'Inter',sans-serif; font-size:0.8rem; color:#7A90A4; line-height:1.55; margin-bottom:12px; }}
.stats{{ display:flex; gap:20px; margin-bottom:14px; flex-wrap:wrap; }}
.slabel{{ font-size:0.55rem; color:#344858; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:2px; }}
.sval{{ font-size:0.95rem; font-weight:600; color:#E2EAF0; }}
.sval.a{{ color:#B06EFF; }}
.sec{{ font-size:0.58rem; color:#344858; text-transform:uppercase; letter-spacing:0.12em; margin:10px 0 5px; }}
.tag{{ display:inline-block; font-size:0.58rem; padding:2px 7px; border-radius:2px; margin:2px 2px 0 0; border:1px solid; }}
.tag-red   {{ color:#FF4D4D; border-color:rgba(255,77,77,0.3);    background:rgba(255,77,77,0.06); }}
.tag-amber {{ color:#FFB800; border-color:rgba(255,184,0,0.3);    background:rgba(255,184,0,0.06); }}
.tag-purple{{ color:#A855F7; border-color:rgba(168,85,247,0.3);   background:rgba(168,85,247,0.06); }}
.tag-accent{{ color:#B06EFF; border-color:rgba(176,110,255,0.3);  background:rgba(176,110,255,0.06); }}
.tag-dim   {{ color:#7A90A4; border-color:#2A1C38; }}
.art{{ padding:6px 0; border-bottom:1px solid #2A1C38; }}
.art:last-child{{ border-bottom:none; }}
.art-title{{ font-family:'Inter',sans-serif; font-size:0.82rem; font-weight:500; }}
.art-title a{{ color:#E2EAF0; text-decoration:none; }}
.art-title a:hover{{ color:#B06EFF; }}
.art-meta{{ font-size:0.58rem; color:#344858; margin-top:2px; }}
.mlink{{ font-size:0.62rem; color:#B06EFF; text-decoration:none; display:inline-block; margin-top:10px; }}
.mlink:hover{{ text-decoration:underline; }}
.none{{ font-size:0.65rem; color:#344858; }}
</style>
</head>
<body>
<div id="acc"></div>
<script>
const DATA = {data_json};
function build() {{
    const acc = document.getElementById("acc");
    DATA.forEach((g, i) => {{
        const row = document.createElement("div");
        row.className = "row" + (g.linked ? " linked" : "");
        row.id = "row-" + i;
        const originBadges = (g.origins||[]).map(o => "<span class='otag'>" + o + "</span>").join(" ");
        row.innerHTML =
            "<div class='row-header'>" +
            "<div class='row-left'>" +
            "<span class='mid'>" + g.mid + "</span>" +
            "<span class='rname'>" + g.name + "</span>" +
            (g.linked ? "<span class='badge'>INTEL</span>" : "") +
            originBadges +
            "</div><span class='chev'>▼</span></div>" +
            "<div class='aliases'>" + g.aliases + "</div>";
        row.addEventListener("click", () => toggle(i));
        acc.appendChild(row);

        const det = document.createElement("div");
        det.className = "detail";
        det.id = "det-" + i;
        const malHtml = g.malware.length
            ? g.malware.map(m => "<span class='tag tag-purple'>" + m + "</span>").join("")
            : "<span class='none'>none in graph</span>";
        const locHtml = g.locs.length
            ? g.locs.map(l => "<span class='tag tag-red'>" + l + "</span>").join("")
            : "<span class='none'>none in graph</span>";
        const artHtml = g.articles.map(a =>
            "<div class='art'><div class='art-title'><a href='" + (a.url||"#") + "' target='_blank'>" +
            ((a.title||"").length > 85 ? (a.title||"").slice(0,83) + "…" : (a.title||"")) +
            "</a></div><div class='art-meta'>" + (a.source||"") + "</div></div>"
        ).join("");
        det.innerHTML =
            (g.desc ? "<div class='desc'>" + g.desc + (g.desc.length >= 320 ? "…" : "") + "</div>" : "") +
            "<div class='stats'>" +
            "<div><div class='slabel'>MITRE ID</div><div class='sval a'>" + g.mid + "</div></div>" +
            "<div><div class='slabel'>Malware</div><div class='sval'>" + g.malware.length + "</div></div>" +
            "<div><div class='slabel'>Targets</div><div class='sval'>" + g.locs.length + "</div></div>" +
            "<div><div class='slabel'>Articles</div><div class='sval'>" + g.articles.length + "</div></div>" +
            "</div>" +
            "<div class='sec'>Known Malware</div>" + malHtml +
            "<div class='sec'>Targeted Locations</div>" + locHtml +
            (g.articles.length ? "<div class='sec'>Recent Intelligence</div>" + artHtml : "") +
            (g.url ? "<a class='mlink' href='" + g.url + "' target='_blank'>↗ VIEW ON MITRE ATT&CK</a>" : "");
        acc.appendChild(det);
    }});
}}
function toggle(i) {{
    const row = document.getElementById("row-" + i);
    const det = document.getElementById("det-" + i);
    const isOpen = row.classList.contains("open");
    // Collapse any currently open row before opening the new one
    document.querySelectorAll(".row.open").forEach(r => r.classList.remove("open"));
    document.querySelectorAll(".detail.open").forEach(d => d.classList.remove("open"));
    if (!isOpen) {{
        row.classList.add("open");
        det.classList.add("open");
        row.scrollIntoView({{ behavior:"smooth", block:"nearest" }});
    }}
}}
build();
</script>
</body>
</html>"""

    # Cap height to avoid an oversized iframe for small result sets
    height = min(len(filtered) * 52 + 300, 900)
    components.html(html, height=height, scrolling=True)
