#!/usr/bin/env python3
"""Worker-context graph slicer: given a target (function/class/file name or path),
emit the FOCUSED subgraph (callers, callees, container, layer, related tables) —
compact enough for a prompt. Reads UA knowledge-graph.json. The integration value:
a heavy-repo worker calls this for its target instead of reading the whole repo."""
import json, sys, re

g = json.load(open(sys.argv[1]))
target = sys.argv[2]
nodes = g["nodes"]; edges = g["edges"]
byid = {n["id"]: n for n in nodes}

def ekey(e, *cands):
    for c in cands:
        if c in e: return e[c]
    return None
SRC = lambda e: ekey(e, "source", "from", "src", "fromId")
DST = lambda e: ekey(e, "target", "to", "dst", "toId")
ETY = lambda e: ekey(e, "type", "kind", "label") or "?"

# match target: by name (case-insens substring) or exact filePath/id
hits = [n for n in nodes if target.lower() == (n.get("name","").lower())
        or target in (n.get("filePath","") or "") or target == n.get("id")]
if not hits:
    hits = [n for n in nodes if target.lower() in (n.get("name","").lower())][:5]
if not hits:
    print("no node matches", target); sys.exit(1)

for n in hits[:3]:
    nid = n["id"]
    print("="*60)
    print(f"NODE  {n.get('type')}  {n.get('name')}   [{n.get('id')}]")
    print(f"  file: {n.get('filePath')}  lang: {n.get('language')}  complexity: {n.get('complexity')}")
    # layer
    for L in g.get("layers", []):
        ids = L.get("nodeIds") or L.get("members") or []
        if nid in ids: print(f"  layer: {L.get('name')}"); break
    callers = [SRC(e) for e in edges if DST(e)==nid]
    callees = [DST(e) for e in edges if SRC(e)==nid]
    def names(ids, lim=12):
        out=[]
        for i in ids[:lim]:
            m=byid.get(i); out.append(m.get("name") if m else i)
        return out
    print(f"  callers/refs-in ({len(callers)}): {names(callers)}")
    print(f"  callees/refs-out ({len(callees)}): {names(callees)}")
    # container file + siblings
    cont = n.get("filePath")
    sibs = [x.get("name") for x in nodes if x.get("filePath")==cont and x.get("type") in ("function","class") and x["id"]!=nid][:15]
    print(f"  siblings in file ({len(sibs)}): {sibs}")
    # related tables
    tbls = [byid[d].get("name") for d in callees if byid.get(d,{}).get("type")=="table"]
    if tbls: print(f"  related tables: {tbls}")
