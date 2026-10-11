"""
LangSmith evidence pull + deterministic audit for the support-bot project.

Pulls three things (not just root runs):
  1. root runs        -> inputs/outputs, latency, tokens, cost, metadata, feedback
  2. tool runs        -> proves whether live APIs (order/logistics lookups) are called
  3. errored runs     -> failures hidden below a root that still says "success"

Writes CSVs to OUT_DIR and prints a plain-text report. No LLM judging anywhere.
Verify argument names against your installed `langsmith` SDK version.

Usage:
    python langsmith_audit.py --days 14 --max-roots 5000
Optional Mongo join (set env vars): MONGO_URI, MONGO_DB, MONGO_COLLECTIONS (comma-sep),
    TICKET_ID_FIELD (default "_id"), OUTCOME_FIELDS (comma-sep, e.g. "status,assignedTo")
"""
import argparse
import json
import os
import warnings
from datetime import datetime, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv
from langsmith import Client

# list_runs() is deprecated (removal after Jan 31, 2027); it still works until then.
warnings.filterwarnings("ignore", category=DeprecationWarning, module="langsmith")

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
PROJECT_NAME = os.getenv("PROJECT_NAME")
OUT_DIR = "langsmith_audit_out"
 
FIELDS = [
    "id", "name", "run_type", "trace_id", "parent_run_id", "inputs", "outputs",
    "error", "status", "start_time", "end_time", "first_token_time",
    "total_tokens", "prompt_tokens", "completion_tokens", "total_cost",
    "extra", "tags", "feedback_stats",
]
# Metadata keys you should be logging on every trace (see recommendations at the end).
META_KEYS = ["ticket_id", "thread_id", "conversation_id", "session_id", "brand", "channel", "route"]
 
 
# --------------------------------------------------------------------------- fetch
def fetch(client, label, max_runs, **kwargs):
    """Stream list_runs and stop at max_runs.
 
    The API rejects any single request with limit > 100, so we never pass `limit`;
    the SDK pages through cursors and we break out once we have enough.
    Falls back to no `select` only if the error is actually about `select`.
    """
    def stream(**kw):
        out = []
        for r in client.list_runs(**kw):
            out.append(r)
            if len(out) >= max_runs:
                break
        return out
 
    try:
        runs = stream(select=FIELDS, **kwargs)
    except Exception as e:  # noqa: BLE001
        if "select" not in str(e).lower():
            raise
        print(f"  [warn] select= rejected for {label}; retrying without it")
        runs = stream(**kwargs)
    print(f"  {label}: {len(runs)} runs")
    return runs
 
 
# ------------------------------------------------------------------------ flatten
def dumps(obj):
    return json.dumps(obj, default=str, ensure_ascii=False) if obj is not None else None
 
 
def utc(d):
    """LangSmith returns a mix of naive and tz-aware datetimes; treat naive as UTC."""
    if d is None:
        return None
    return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)
 
 
def secs(a, b):
    a, b = utc(a), utc(b)
    return (b - a).total_seconds() if a and b else None
 
 
def last_message(outputs, role=None):
    """Pull the last message (optionally of a given role) from {'messages': [...]} outputs."""
    msgs = outputs.get("messages") if isinstance(outputs, dict) else None
    if not isinstance(msgs, list):
        return None
    for m in reversed(msgs):
        if isinstance(m, dict) and (role is None or m.get("role") == role):
            return str(m.get("content", ""))
    return None
 
 
def to_record(r):
    meta = (r.extra or {}).get("metadata", {}) or {}
    outputs = r.outputs if isinstance(r.outputs, dict) else {}
    rec = {
        "run_id": str(r.id),
        "trace_id": str(r.trace_id) if r.trace_id else None,
        "parent_run_id": str(r.parent_run_id) if r.parent_run_id else None,
        "name": r.name,
        "run_type": r.run_type,
        "status": r.status,
        "error": r.error,
        "start_time": utc(r.start_time),
        "latency_s": secs(r.start_time, r.end_time),
        "ttft_s": secs(r.start_time, r.first_token_time),
        "total_tokens": r.total_tokens,
        "prompt_tokens": r.prompt_tokens,
        "completion_tokens": r.completion_tokens,
        "total_cost": float(r.total_cost) if r.total_cost is not None else None,
        "tags": ",".join(r.tags or []),
        "feedback": dumps(r.feedback_stats) if r.feedback_stats else None,
        # router roots return {'output': '<AgentName>'}
        "route": outputs.get("output") if r.name == "router" and isinstance(outputs.get("output"), str) else meta.get("route"),
        "last_user_msg": last_message(outputs, "user"),
        "final_reply": last_message(outputs),
        "inputs": dumps(r.inputs),
        "outputs": dumps(r.outputs),
    }
    for k in META_KEYS:
        if k != "route":
            rec[k] = meta.get(k)
    return rec
 
 
def to_df(runs):
    return pd.DataFrame([to_record(r) for r in runs])
 
 
# ------------------------------------------------------------------- Mongo (optional)
def join_mongo(roots):
    uri = os.getenv("MONGO_URI")
    if not uri:
        return roots, None
    if roots["ticket_id"].notna().sum() == 0:
        print("  [skip] Mongo join: no ticket_id metadata on any root run")
        return roots, None
    from pymongo import MongoClient
 
    db = MongoClient(uri)[os.environ["MONGO_DB"]]
    id_field = os.getenv("TICKET_ID_FIELD", "_id")
    outcome = [f for f in os.getenv("OUTCOME_FIELDS", "").split(",") if f]
    proj = {id_field: 1, **{f: 1 for f in outcome}}
    rows = []
    for coll in os.environ["MONGO_COLLECTIONS"].split(","):
        for d in db[coll.strip()].find({}, proj):
            rows.append({"ticket_id": str(d.get(id_field)), "collection": coll.strip(),
                         **{f: d.get(f) for f in outcome}})
    tickets = pd.DataFrame(rows).drop_duplicates("ticket_id")
    roots = roots.assign(ticket_id=roots["ticket_id"].astype("string")).merge(
        tickets, on="ticket_id", how="left", suffixes=("", "_mongo"))
    matched = roots["collection"].notna().sum()
    print(f"  Mongo join: {matched}/{len(roots)} roots matched a ticket")
    return roots, tickets
 
 
# ----------------------------------------------------------------------- reporting
def pct(n, d):
    return f"{(100 * n / d):.1f}%" if d else "n/a"
 
 
def report(roots, tools, errs):
    line = "=" * 64
    print(f"\n{line}\nLANGSMITH AUDIT  |  {PROJECT_NAME}\n{line}")
 
    print(f"\n[1] Volume: {len(roots)} root runs, {len(tools)} tool runs, {len(errs)} errored runs")
    print(roots["name"].value_counts().to_string())
    if roots["name"].nunique() > 1:
        print("  NOTE: multiple root names -> router/agent are likely separate traces,"
              " so route-to-outcome can't be linked. Nest them or share a thread_id.")
 
    print("\n[2] Join-key coverage (without these, no ground-truth joins)")
    for k in ["ticket_id", "thread_id", "brand", "channel"]:
        print(f"  {k:<10} {pct(roots[k].notna().sum(), len(roots))}")
 
    print("\n[3] Latency / tokens / cost by root name")
    g = roots.groupby("name").agg(
        n=("run_id", "count"),
        lat_p50=("latency_s", "median"),
        lat_p95=("latency_s", lambda s: s.quantile(.95)),
        tok_mean=("total_tokens", "mean"),
        zero_tok=("total_tokens", lambda s: (s.fillna(0) == 0).sum()),
        cost_total=("total_cost", "sum"),
    ).round(3)
    print(g.to_string())
 
    print("\n[4] Tool usage (the live-API proof)")
    if tools.empty:
        print("  NO tool runs found. Either tools aren't traced, or the bot never calls any.")
    else:
        t = tools.groupby("name").agg(
            calls=("run_id", "count"),
            errors=("error", lambda s: s.notna().sum()),
            lat_p50=("latency_s", "median"),
            lat_p95=("latency_s", lambda s: s.quantile(.95)),
        ).round(3)
        t["err_rate"] = (t["errors"] / t["calls"]).round(3)
        print(t.sort_values("calls", ascending=False).to_string())
        traces_with_tool = tools["trace_id"].nunique()
        print(f"  Traces that invoked >=1 tool: {traces_with_tool} "
              f"({pct(traces_with_tool, roots['trace_id'].nunique())} of root traces)")
 
    print("\n[5] Hidden failures (root says success, a child errored)")
    if errs.empty:
        print("  none found")
    else:
        hidden = roots[(roots["status"] == "success") & roots["trace_id"].isin(errs["trace_id"])]
        print(f"  {len(hidden)} root runs marked success with errored child runs")
        print(errs.groupby("name")["error"].agg(
            n="count", sample=lambda s: str(s.iloc[0])[:80]).to_string())
 
    print("\n[6] Router distribution")
    r = roots[roots["name"] == "router"]["route"].value_counts(dropna=False)
    if r.empty:
        print("  no router roots (or no route metadata)")
    else:
        print(r.to_string())
        dead = r[r.index.isin(["FallbackAgent", "HandoffAgent"])].sum()
        print(f"  Fallback+Handoff share: {pct(dead, r.sum())}")
 
    print("\n[7] Canned / zero-token replies (guard paths that skip the LLM)")
    z = roots[(roots["name"] != "router") & (roots["total_tokens"].fillna(0) == 0)]
    print(f"  {len(z)} runs; top replies:")
    print(z["final_reply"].fillna("").str[:80].value_counts().head(5).to_string())
 
    print("\n[8] Slowest 10 roots (drill into these trace_ids in the UI)")
    cols = ["trace_id", "name", "latency_s", "total_tokens", "last_user_msg"]
    print(roots.nlargest(10, "latency_s")[cols].to_string(index=False, max_colwidth=50))
 
    if "collection" in roots and roots["collection"].notna().any():
        outcome_cols = [c for c in roots.columns if c in os.getenv("OUTCOME_FIELDS", "").split(",") and c]
        for c in outcome_cols:
            print(f"\n[9] Outcome '{c}' by route")
            print(pd.crosstab(roots["route"].fillna("n/a"), roots[c].astype(str)).to_string())
 
 
# --------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--max-roots", type=int, default=5000)
    ap.add_argument("--max-tools", type=int, default=20000)
    ap.add_argument("--max-errors", type=int, default=5000)
    args = ap.parse_args()
 
    load_dotenv()
    client = Client()
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    os.makedirs(OUT_DIR, exist_ok=True)
 
    print(f"Fetching from '{PROJECT_NAME}' since {since:%Y-%m-%d %H:%M} UTC")
    roots = to_df(fetch(client, "root runs", args.max_roots, project_id=PROJECT_ID,
                        is_root=True, start_time=since))
    tools = to_df(fetch(client, "tool runs", args.max_tools, project_id=PROJECT_ID,
                        run_type="tool", start_time=since))
    errs = to_df(fetch(client, "errored runs", args.max_errors, project_id=PROJECT_ID,
                       error=True, start_time=since))
 
    if roots.empty:
        print("No root runs in window; widen --days.")
        return
 
    roots, _ = join_mongo(roots)
 
    roots.to_csv(f"{OUT_DIR}/roots.csv", index=False)
    tools.to_csv(f"{OUT_DIR}/tool_runs.csv", index=False)
    errs.to_csv(f"{OUT_DIR}/errored_runs.csv", index=False)
    print(f"Saved CSVs to ./{OUT_DIR}/")
 
    report(roots, tools, errs)
 
 
if __name__ == "__main__":
    main()