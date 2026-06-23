# tools/

## graph_query.py — UA knowledge-graph slicer (worker context)
Given a UA `knowledge-graph.json` + a target (function/class/file name or path),
emit the FOCUSED subgraph (callers, callees, container file, layer, siblings,
related tables) — compact enough for a prompt. Lets a heavy-repo worker get its
target's context without reading the whole tree.

    python3 tools/graph_query.py <knowledge-graph.json> <target>

Graph is produced by Understand-Anything (Lum1104/Understand-Anything, MIT). The
DETERMINISTIC tree-sitter structure (nodes/edges/layers) is the valuable part; the
LLM per-node summaries underdelivered (generic) on mnemos 2026-06-04 — prefer a
STRUCTURE-ONLY refresh (run the skill's .mjs scripts, skip the LLM summary phase)
for $0 + no codex-allowance pressure. See MNEMOS project_ua_knowledge_graph_2026_06_04.
