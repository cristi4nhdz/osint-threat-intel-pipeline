# dashboard/_sections/actor_graph.py
"""Actor Graph: D3 visualization of Neo4j nodes/edges."""

import json
import streamlit as st
import streamlit.components.v1 as components
from dashboard.db import neo4j_query

ACCENT = "#B06EFF"


def build_graph_html(nodes_json: str, edges_json: str, height: int = 700) -> str:
    """Generates HTML + D3 for interactive actor graph."""

    return f"""<!DOCTYPE html>
<html>
<head>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
html, body {{ width:100%; height:{height}px; background:#0D0B14; overflow:hidden; }}
svg {{ width:100%; height:100%; display:block; }}
text {{ font-size:11px; fill:#7A90A4; font-family:monospace; pointer-events:none; }}
.link-targets         {{ stroke:rgba(255,77,77,0.35); }}
.link-originates_from {{ stroke:rgba(255,184,0,0.4); }}
.link-uses            {{ stroke:rgba(176,110,255,0.4); }}
.link-unknown         {{ stroke:rgba(100,100,100,0.3); stroke-dasharray:4,3; }}
#toolbar {{
    position:absolute; top:10px; right:10px;
    display:flex; gap:6px; z-index:100;
}}
.tb {{ background:rgba(19,15,31,0.95); border:1px solid #2A1C38;
       color:#7A90A4; font-family:monospace; font-size:10px;
       padding:4px 10px; cursor:pointer; border-radius:2px; }}
.tb:hover {{ border-color:#B06EFF; color:#B06EFF; }}
#legend {{
    position:absolute; bottom:10px; left:12px;
    font-family:monospace; font-size:10px; color:#344858;
    display:flex; gap:14px; align-items:center; flex-wrap:wrap;
}}
.dot {{ display:inline-block; width:7px; height:7px; border-radius:50%; margin-right:4px; vertical-align:middle; }}
.ln  {{ display:inline-block; width:14px; height:2px; margin-right:4px; vertical-align:middle; border-radius:1px; }}
#panel {{
    position:absolute; top:10px; left:10px; width:250px;
    background:rgba(13,11,20,0.97); border:1px solid #2A1C38;
    border-left:3px solid #B06EFF; border-radius:3px;
    padding:12px; font-family:monospace; font-size:10px;
    color:#7A90A4; display:none; z-index:200; max-height:80%; overflow-y:auto;
}}
#panel.open {{ display:block; }}
#panel-close {{ position:absolute; top:7px; right:9px; cursor:pointer; color:#344858; font-size:13px; }}
#panel-close:hover {{ color:#FF4D4D; }}
.pname {{ font-size:12px; font-weight:600; color:#E2EAF0; margin-bottom:8px; padding-right:16px; line-height:1.3; }}
.prow  {{ display:flex; justify-content:space-between; padding:3px 0; border-bottom:1px solid #2A1C38; font-size:10px; }}
.prow:last-child {{ border-bottom:none; }}
.plabel {{ color:#344858; text-transform:uppercase; letter-spacing:0.07em; }}
.pval {{ color:#E2EAF0; }}
.pval.accent {{ color:#B06EFF; }}
.pval.amber  {{ color:#FFB800; }}
.pval.grey   {{ color:#7A90A4; }}
.psec {{ margin-top:8px; }}
.psec-label {{ font-size:9px; color:#344858; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:4px; }}
.ptag {{ display:inline-block; font-size:9px; padding:1px 5px; border-radius:2px; border:1px solid; margin:2px 2px 0 0; }}
.ptag-accent {{ color:#B06EFF; border-color:rgba(176,110,255,0.3); background:rgba(176,110,255,0.06); }}
.ptag-red    {{ color:#FF4D4D; border-color:rgba(255,77,77,0.3);  background:rgba(255,77,77,0.06); }}
.ptag-amber  {{ color:#FFB800; border-color:rgba(255,184,0,0.3);  background:rgba(255,184,0,0.06); }}
.ptag-purple {{ color:#A855F7; border-color:rgba(168,85,247,0.3); background:rgba(168,85,247,0.06); }}
.ptag-grey   {{ color:#7A90A4; border-color:rgba(120,120,120,0.3); background:rgba(120,120,120,0.06); }}
.ptag-dim    {{ color:#7A90A4; border-color:#2A1C38; }}
.mlink {{ color:#B06EFF; text-decoration:none; font-size:9px; display:inline-block; margin-top:8px; }}
.mlink:hover {{ text-decoration:underline; }}
</style>
</head>
<body>
<svg id="g"></svg>
<div id="toolbar">
    <button class="tb" onclick="resetZoom()">⟳ RESET</button>
    <button class="tb" onclick="toggleLabels()">◎ LABELS</button>
</div>
<div id="panel">
    <span id="panel-close" onclick="closePanel()">✕</span>
    <div id="panel-body"></div>
</div>
<div id="legend">
    <span><span class="dot" style="background:#B06EFF;box-shadow:0 0 4px #B06EFF;"></span>Actor</span>
    <span><span class="dot" style="background:#FF4D4D;box-shadow:0 0 4px #FF4D4D;"></span>Target</span>
    <span><span class="dot" style="background:#FFB800;box-shadow:0 0 4px #FFB800;"></span>Origin</span>
    <span><span class="dot" style="background:#555;"></span>Unknown</span>
    <span><span class="dot" style="background:#A855F7;box-shadow:0 0 4px #A855F7;"></span>Malware</span>
    <span style="color:#2A1C38;">|</span>
    <span><span class="ln" style="background:rgba(255,77,77,0.6);"></span>Targets</span>
    <span><span class="ln" style="background:rgba(255,184,0,0.6);"></span>Origin</span>
    <span><span class="ln" style="background:rgba(176,110,255,0.6);"></span>Uses</span>
    <span style="color:#2A1C38;">|</span>
    <span style="color:#344858;">scroll=zoom · drag=pan · click=info</span>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
<script>
const NODES = {nodes_json};
const LINKS = {edges_json};
const COLORS = {{ actor:"#B06EFF", target:"#FF4D4D", origin:"#FFB800", malware:"#A855F7", unknown:"#555555" }};
let zoom, svgSel, labelSel, showLabels = true;

function nodeColor(d) {{
    if (d.ntype === "origin" && d.id === "Unknown") return COLORS.unknown;
    return COLORS[d.ntype] || COLORS.target;
}}

function linkClass(d) {{
    const rtype = (d.rtype || "default").toLowerCase();
    if (d.target && (d.target.id === "Unknown" || (d.target && d.target === "Unknown"))) return "link-unknown";
    return "link-" + rtype;
}}

window.addEventListener("load", () => {{
    const W = document.documentElement.clientWidth || window.innerWidth || 1100;
    const H = document.documentElement.clientHeight || window.innerHeight || 700;
    init(W, H);
}});

function init(W, H) {{
    const nodes = NODES.map(d => ({{...d}}));
    const links = LINKS.map(d => ({{...d}}));

    const svg = d3.select("#g").attr("width", W).attr("height", H);
    svg.selectAll("*").remove();

    // SVG glow filters — one per node color type
    const defs = svg.append("defs");
    Object.entries(COLORS).forEach(([k, c]) => {{
        const f = defs.append("filter").attr("id", "glow-" + k);
        f.append("feGaussianBlur").attr("stdDeviation", "2.5").attr("result", "blur");
        const m = f.append("feMerge");
        m.append("feMergeNode").attr("in", "blur");
        m.append("feMergeNode").attr("in", "SourceGraphic");
    }});

    const g = svg.append("g");
    zoom = d3.zoom().scaleExtent([0.02, 8]).on("zoom", e => g.attr("transform", e.transform));
    svg.call(zoom);
    svg.on("click", e => {{ if (e.target === svg.node()) closePanel(); }});

    const sim = d3.forceSimulation(nodes)
        .force("link", d3.forceLink(links).id(d => d.id).distance(55))
        .force("charge", d3.forceManyBody().strength(-70))
        .force("center", d3.forceCenter(W / 2, H / 2))
        .force("x", d3.forceX(W / 2).strength(0.06))
        .force("y", d3.forceY(H / 2).strength(0.06))
        .force("collide", d3.forceCollide(12));

    const link = g.append("g").selectAll("line").data(links).join("line")
        .attr("class", d => {{
            const rtype = (d.rtype || "default").toLowerCase();
            return "link-" + rtype;
        }})
        .attr("stroke-width", 1.2);

    const node = g.append("g").selectAll("g").data(nodes).join("g")
        .call(d3.drag()
            .on("start", (e, d) => {{ if (!e.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; }})
            .on("drag",  (e, d) => {{ d.fx = e.x; d.fy = e.y; }})
            .on("end",   (e, d) => {{ if (!e.active) sim.alphaTarget(0); d.fx = null; d.fy = null; }}))
        .on("click", (e, d) => {{ e.stopPropagation(); showInfo(d); }});

    node.append("circle")
        .attr("r", d => d.ntype === "actor" ? 8 : 6)
        .attr("fill", d => nodeColor(d))
        .attr("fill-opacity", d => d.id === "Unknown" ? 0.5 : 0.9)
        .attr("stroke", d => nodeColor(d))
        .attr("stroke-width", 1.5)
        .attr("stroke-opacity", 0.4)
        .style("filter", d => d.id === "Unknown" ? "none" : "url(#glow-" + (d.ntype || "target") + ")")
        .style("cursor", "pointer");

    labelSel = node.append("text")
        .attr("dx", 10).attr("dy", 4)
        .style("fill", d => d.id === "Unknown" ? "#555" : "#7A90A4")
        .text(d => d.id.length > 20 ? d.id.slice(0, 18) + "…" : d.id);

    sim.on("tick", () => {{
        link.attr("x1", d => d.source.x).attr("y1", d => d.source.y)
            .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
        node.attr("transform", d => `translate(${{d.x}},${{d.y}})`);
    }});

    svgSel = svg;
}}

function showInfo(d) {{
    const panel = document.getElementById("panel");
    const body  = document.getElementById("panel-body");
    panel.classList.add("open");
    panel.style.borderLeftColor = nodeColor(d);
    const info = d.info || {{}};

    if (d.ntype === "actor") {{
        // Strip generic malware class names from the panel display
        const mal = (info.malware || []).filter(m =>
            !["malware","backdoor","trojan","worm","exploit","spyware","ransomware","loader","dropper","botnet"].includes(m.toLowerCase()));
        body.innerHTML =
            "<div class='pname'>" + d.id + "</div>" +
            (info.mitre_id ? "<div class='prow'><span class='plabel'>MITRE</span><span class='pval accent'>" + info.mitre_id + "</span></div>" : "") +
            "<div class='prow'><span class='plabel'>Articles</span><span class='pval'>" + (info.article_count || 0) + "</span></div>" +
            ((info.origins || []).length ? "<div class='psec'><div class='psec-label'>Origin</div>" +
                info.origins.map(o => "<span class='ptag " + (o === "Unknown" ? "ptag-grey" : "ptag-amber") + "'>" + o + "</span>").join("") + "</div>" : "") +
            ((info.targets || []).length ? "<div class='psec'><div class='psec-label'>Targets</div>" +
                info.targets.slice(0, 6).map(t => "<span class='ptag ptag-red'>" + t + "</span>").join("") + "</div>" : "") +
            (mal.length ? "<div class='psec'><div class='psec-label'>Malware</div>" +
                mal.slice(0, 6).map(m => "<span class='ptag ptag-purple'>" + m + "</span>").join("") + "</div>" : "") +
            ((info.aliases || []).length ? "<div class='psec'><div class='psec-label'>Aliases</div>" +
                info.aliases.slice(0, 5).map(a => "<span class='ptag ptag-dim'>" + a + "</span>").join("") + "</div>" : "") +
            (info.url ? "<a class='mlink' href='" + info.url + "' target='_blank'>↗ MITRE ATT&CK</a>" : "");
    }} else if (d.id === "Unknown") {{
        const actors = info.actors || [];
        body.innerHTML =
            "<div class='pname'>Unknown / Unattributed</div>" +
            "<div class='prow'><span class='plabel'>Type</span><span class='pval grey'>Criminal / Unattributed</span></div>" +
            (actors.length ? "<div class='psec'><div class='psec-label'>Groups</div>" +
                actors.slice(0, 15).map(a => "<span class='ptag ptag-grey'>" + a + "</span>").join("") + "</div>" : "");
    }} else {{
        const actors = info.actors || [];
        const typeLabel = d.ntype === "origin" ? "Origin Country" : d.ntype === "malware" ? "Malware" : "Target";
        const tagCls = d.ntype === "origin" ? "ptag-amber" : d.ntype === "malware" ? "ptag-purple" : "ptag-accent";
        body.innerHTML =
            "<div class='pname'>" + d.id + "</div>" +
            "<div class='prow'><span class='plabel'>Type</span><span class='pval'>" + typeLabel + "</span></div>" +
            (actors.length ? "<div class='psec'><div class='psec-label'>" + (d.ntype === "origin" ? "Actors From Here" : "Linked Actors") + "</div>" +
                actors.slice(0, 10).map(a => "<span class='ptag " + tagCls + "'>" + a + "</span>").join("") + "</div>" : "");
    }}
}}

function closePanel() {{ document.getElementById("panel").classList.remove("open"); }}
function resetZoom() {{ svgSel.transition().duration(400).call(zoom.transform, d3.zoomIdentity); }}
function toggleLabels() {{ showLabels = !showLabels; if (labelSel) labelSel.style("display", showLabels ? null : "none"); }}
</script>
</body>
</html>"""


def show() -> None:
    """Queries Neo4j, builds nodes/edges JSON, renders graph in Streamlit."""

    st.markdown("<h1>Actor Graph</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.6rem;"
        "color:#344858;letter-spacing:0.08em;margin-bottom:16px;'>"
        "NEO4J KNOWLEDGE GRAPH — CLICK ANY NODE FOR DETAILS</div>",
        unsafe_allow_html=True,
    )

    c1, c2 = st.columns([3, 1])
    with c1:
        mode = st.selectbox(
            "View",
            [
                "Actor → Origins only",
                "Actor → Origin + Targets",
                "Actor → Targets only",
                "Actor → Malware",
            ],
        )
    with c2:
        limit = st.slider("Limit", 50, 500, 300)

    if mode == "Actor → Origins only":
        raw = neo4j_query(
            f"""
            MATCH (a:ThreatActor)-[:ORIGINATES_FROM]->(c:Country)
            RETURN a.name as actor, c.name as target,
                   'ORIGINATES_FROM' as rtype, 'Country' as ttype
            LIMIT {limit}
        """
        )
    elif mode == "Actor → Origin + Targets":
        raw = neo4j_query(
            f"""
            MATCH (a:ThreatActor)-[r]->(n)
            WHERE type(r) IN ['TARGETS','ORIGINATES_FROM']
            RETURN a.name as actor, n.name as target,
                   type(r) as rtype, labels(n)[0] as ttype
            LIMIT {limit}
        """
        )
    elif mode == "Actor → Targets only":
        raw = neo4j_query(
            f"""
            MATCH (a:ThreatActor)-[:TARGETS]->(l:Location)
            RETURN a.name as actor, l.name as target,
                   'TARGETS' as rtype, 'Location' as ttype
            LIMIT {limit}
        """
        )
    else:
        raw = neo4j_query(
            f"""
            MATCH (a:ThreatActor)-[:USES]->(m:Malware)
            RETURN a.name as actor, m.name as target,
                   'USES' as rtype, 'Malware' as ttype
            LIMIT {limit}
        """
        )

    if not raw:
        st.warning(
            "No graph data. Make sure Docker is running and the pipeline has been executed."
        )
        return

    # Bulk-fetch metadata for all actors in the result set
    actor_names = list({r["actor"] for r in raw if r.get("actor")})
    actor_info = {}
    if actor_names:
        bulk = neo4j_query(
            """
            MATCH (a:ThreatActor) WHERE a.name IN $names
            OPTIONAL MATCH (a)-[:USES]->(m:Malware)
            OPTIONAL MATCH (a)-[:TARGETS]->(l:Location)
            OPTIONAL MATCH (a)-[:ORIGINATES_FROM]->(c:Country)
            OPTIONAL MATCH (art:Article)-[:MENTIONS_ACTOR]->(a)
            RETURN a.name as name, a.mitre_id as mid,
                   a.aliases as aliases, a.url as url,
                   collect(DISTINCT m.name) as malware,
                   collect(DISTINCT l.name) as targets,
                   collect(DISTINCT c.name) as origins,
                   count(DISTINCT art) as cnt
        """,
            names=actor_names,
        )
        for r in bulk:
            actor_info[r["name"]] = {
                "mitre_id": r["mid"],
                "aliases": list(r["aliases"] or []),
                "url": r["url"],
                "malware": list(r["malware"] or []),
                "targets": list(r["targets"] or []),
                "origins": list(r["origins"] or []),
                "article_count": r["cnt"],
            }

    # Resolve ntype for each target node across all relationship types
    target_names = list({r.get("target") for r in raw if r.get("target")})
    target_info = {}
    if target_names:
        for q, ntype in [
            (
                "MATCH (a:ThreatActor)-[:ORIGINATES_FROM]->(n) WHERE n.name IN $names RETURN n.name as name, collect(DISTINCT a.name) as actors",
                "origin",
            ),
            (
                "MATCH (a:ThreatActor)-[:TARGETS]->(n) WHERE n.name IN $names RETURN n.name as name, collect(DISTINCT a.name) as actors",
                "target",
            ),
            (
                "MATCH (a:ThreatActor)-[:USES]->(n) WHERE n.name IN $names RETURN n.name as name, collect(DISTINCT a.name) as actors",
                "malware",
            ),
        ]:
            for r in neo4j_query(q, names=target_names):
                if r["name"] not in target_info:
                    target_info[r["name"]] = {"actors": r["actors"], "ntype": ntype}

    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    for row in raw:
        src = row.get("actor") or "unknown"
        tgt = row.get("target")
        rtype = row.get("rtype", "default")
        ttype = row.get("ttype", "")
        if src not in nodes:
            nodes[src] = {"id": src, "ntype": "actor", "info": actor_info.get(src, {})}
        if tgt:
            if tgt not in nodes:
                ti = target_info.get(tgt, {})
                ntype = ti.get("ntype", "target")
                # rtype takes precedence over ttype for node classification
                if rtype == "ORIGINATES_FROM" or ttype == "Country":
                    ntype = "origin"
                elif rtype == "USES" or ttype == "Malware":
                    ntype = "malware"
                nodes[tgt] = {
                    "id": tgt,
                    "ntype": ntype,
                    "info": {"actors": ti.get("actors", [])},
                }
            edges.append({"source": src, "target": tgt, "rtype": rtype})

    nodes_json = json.dumps(list(nodes.values()))
    edges_json = json.dumps(edges)

    components.html(build_graph_html(nodes_json, edges_json, 720), height=730)

    ac = sum(1 for n in nodes.values() if n["ntype"] == "actor")
    tc = sum(1 for n in nodes.values() if n["ntype"] == "target")
    oc = sum(1 for n in nodes.values() if n["ntype"] == "origin")
    mc = sum(1 for n in nodes.values() if n["ntype"] == "malware")

    st.markdown(
        "<div style='display:flex;gap:20px;padding:6px 0;"
        f"font-family:IBM Plex Mono,monospace;font-size:0.62rem;color:#344858;'>"
        f"ACTORS: <span style='color:{ACCENT};'>{ac}</span> &nbsp;"
        f"TARGETS: <span style='color:#FF4D4D;'>{tc}</span> &nbsp;"
        f"ORIGINS: <span style='color:#FFB800;'>{oc}</span> &nbsp;"
        f"MALWARE: <span style='color:#A855F7;'>{mc}</span> &nbsp;"
        f"EDGES: <span style='color:#7A90A4;'>{len(edges)}</span>"
        "</div>",
        unsafe_allow_html=True,
    )
