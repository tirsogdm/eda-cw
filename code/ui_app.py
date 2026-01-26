import os
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from redis_state import get_redis, k_run, k_run_proteins, k_protein

# Helpers -----------

@st.cache_data(ttl=30)
def list_run_ids() -> [str]:
    r = get_redis()

    run_ids = []
    cursor = 0

    # Scan keys like pp:run:<id> but exclude subkeys
    while True:
        cursor, keys = r.scan(cursor=cursor, match="pp:run:*", count=500)
        for k in keys:
            if ":proteins" in k or ":protein:" in k:
                continue
            parts = k.split(":")
            if len(parts) == 3:
                run_ids.append(parts[2])
        if cursor == 0:
            break
        
    # assert len(run_ids) == len(set(run_ids)), "Duplicate run IDs found in Redis"
    return sorted(set(run_ids), reverse=True)

def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default
    
def trunc(s: str, n=80) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n-3] + "..."

def load_run_meta(run_id: str) -> dict:
    r = get_redis()
    return r.hgetall(k_run(run_id))

def load_protein_ids(run_id: str) -> list[str]:
    r = get_redis()
    ids = list(r.smembers(k_run_proteins(run_id)))
    ids.sort()
    return ids

def load_proteins_batch(run_id: str, protein_ids: list[str]) -> list[dict]:
    r = get_redis()
    pipe = r.pipeline()
    for p_id in protein_ids:
        pipe.hgetall(k_protein(run_id, p_id))
    return pipe.execute()

# UI Components -----------
st.set_page_config(page_title="Protein Pipeline Monitor", layout="wide")
st.title("Protein Pipeline Monitor")

# Sidebar controls
with st.sidebar:
    st.header("Controls")

    refresh_now = st.button("Refresh") # Refresh entire page
    auto_refresh = st.checkbox("Auto-refresh", value=True)
    refresh_seconds = st.slider("Refresh interval (seconds)", 10, 120, 30, step=5)

    st.divider()
    st.subheader("Protein table")
    auto_refresh_table = st.checkbox("Auto-refresh protein table", value=False)
    
    if refresh_now:
        list_run_ids.clear()
    
    run_ids = list_run_ids()
    if not run_ids:
        st.warning("No runs found in Redis.")
        st.stop()
    
    run_id = st.selectbox("Run", run_ids, index=0)

# Run-change reset
if st.session_state.get("last_run_id") != run_id:
    st.session_state["last_run_id"] = run_id
    st.session_state["protein_df"] = None
    st.session_state["protein_ids"] = None
    st.session_state["loaded_window"] = None

# Load run metadata
meta = load_run_meta(run_id)
if not meta:
    st.error(f"Run not found: {run_id}")
    st.stop()

submitted = safe_int(meta.get("submitted"))
succeeded = safe_int(meta.get("succeeded"))
failed = safe_int(meta.get("failed"))
completed = succeeded + failed
progress = (completed / submitted) if submitted else 0.0

# Run Summary
c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1.2])
with c1:
    st.subheader("Run")
    st.write(f"**run_id:** {run_id}")
    st.write(f"**status:** {meta.get('status', '')}")
with c2:
    st.subheader("Counts")
    st.write(f"**submitted:** {submitted}")
    st.write(f"**enqueued:** {meta.get('enqueued', '')}")
with c3:
    st.subheader("Results")
    st.write(f"**succeeded:** {succeeded}")
    st.write(f"**failed:** {failed}")
with c4:
    st.subheader("Timing")
    st.write(f"**created_at:** {meta.get('created_at', '')}")
    st.write(f"**started_at:** {meta.get('started_at', '')}")
    st.write(f"**finished_at:** {meta.get('finished_at', '')}")

st.progress(progress, text=f"Progress: {completed}/{submitted} ({progress*100:.1f}%)")

st.divider()

# Protein table loading strategy
# Keep in session_state to avoid reloading everything too often
if "protein_df" not in st.session_state or refresh_now:
    st.session_state["protein_df"] = None
    st.session_state["loaded_window"] = None

protein_ids = st.session_state.get("protein_ids")
if protein_ids is None:
    protein_ids = load_protein_ids(run_id)
    st.session_state["protein_ids"] = protein_ids

st.subheader("Proteins")

# Filters and pagination
f1, f2, f3, f4 = st.columns([0.22, 0.30, 0.24, 0.24])
with f1:
    status_filter = st.selectbox("Status Filter", ["all", "submitted", "running", "succeeded", "failed"], index=0)
with f2:
    search_filter = st.text_input("Search protein_id", value="")
with f3:
    batch_size = st.slider("Load size (proteins)", 200, 700, 400, step=50)

total = len(protein_ids)
max_pages = max(1, (total + batch_size - 1) // batch_size)

with f4:
    page = st.number_input("Page", min_value=1, max_value=max_pages, value=1, step=1)

start = (page - 1) * batch_size
end = min(start + batch_size, total)
ids_window = protein_ids[start:end]

st.caption(f"Window: {start + 1}-{end} of {total} proteins (page {page}/{max_pages}).")

# Table reload logic
load_btn = st.button("Load/Reload protein table")
window_key = (run_id, start, end)

should_reload_table = (
    load_btn
    or st.session_state["protein_df"] is None
    or st.session_state.get("loaded_window") != window_key
)

if should_reload_table:
    rows = load_proteins_batch(run_id, ids_window)
    # Normalize into DataFrame
    norm = []
    for p_id, h in zip(ids_window, rows):
        h = h or {}
        norm.append({
            "protein_id": p_id,
            "status": h.get("status", ""),
            "ok": h.get("ok", ""),
            "started_at": h.get("started_at", ""),
            "finished_at": h.get("finished_at", ""),
            "best_hit": h.get("best_hit", ""),
            "score_std": h.get("score_std", ""),
            "score_gmean": h.get("score_gmean", ""),
            "error": trunc(h.get("error", "") or h.get("hard_error", "")),
            "task_id": h.get("task_id", ""),
        })
    st.session_state["protein_df"] = pd.DataFrame(norm)
    st.session_state["loaded_window"] = window_key

# DataFrame view (applying filters)
df = st.session_state["protein_df"]
if df is None or df.empty:
    st.info("No protein data loaded yet. Click 'Load/Reload protein table'.")
    st.stop()

# Apply filters
view = df.copy()
if status_filter != "all":
    view = view[view["status"] == status_filter]
if search_filter.strip():
    view = view[view["protein_id"].str.contains(search_filter.strip(), case=False, na=False)]

st.caption(f"Showing {len(view)} of {len(df)} loaded proteins (out of {len(protein_ids)} total).")

# Table + selection
st.dataframe(
    view[["protein_id", "status", "ok", "best_hit", "score_std", "score_gmean", "started_at", "finished_at", "error"]],
    use_container_width=True,
    hide_index=True,
)

st.divider()

# Protein detail selector ()
st.subheader("Protein detail")
detail_p_id = st.selectbox("Select protein_id", view["protein_id"].tolist()[:500] if len(view) else [], index=0 if len(view) else None)

if detail_p_id:
    r = get_redis()
    h = r.hgetall(k_protein(run_id, detail_p_id)) or {}
    st.write(f"### Redis fields")
    st.json(h)

st.divider()

# Downloads
st.subheader("Run outputs")
run_dir = meta.get("run_dir", "")
if not run_dir:
    st.warning("run_dir not set in Redis for this run.")
else:
    out_dir = Path(run_dir).expanduser()
    hits_fp = out_dir / "hits_output.csv"
    profile_fp = out_dir / "profile_output.csv"
    failures_fp = out_dir / "failures_output.csv"

    d1, d2, d3 = st.columns(3)

    def download(col, fp: Path, label: str):
        with col:
            if fp.exists():
                data = fp.read_bytes()
                st.download_button(label, data=data, file_name=fp.name, mime="text/csv")
                st.caption(str(fp))
            else:
                st.button(label, disabled=True)
                st.caption(f"Not present yet.")
    
    download(d1, hits_fp, "Download hits_output.csv")
    download(d2, profile_fp, "Download profile_output.csv")
    download(d3, failures_fp, "Download failures_output.csv")

if auto_refresh and not refresh_now:
    if auto_refresh_table:
        st.session_state["protein_df"] = None

    time.sleep(refresh_seconds)
    st.rerun()