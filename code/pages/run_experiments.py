import os
import time
import threading
import streamlit as st
from pathlib import Path
from typing import Optional
from datetime import datetime

from orchestrator import run_experiments  # import from pipeline_code_dir

st.set_page_config(page_title="Run experiments", layout="wide")
st.title("Run experiments")

RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", "/home/almalinux/protein_pipeline/code/results")).expanduser()

# Persistent state
if "run_thread" not in st.session_state:
    st.session_state.run_thread = None
if "run_logs" not in st.session_state:
    st.session_state.run_logs = []
if "run_status" not in st.session_state:
    st.session_state.run_status = "idle"  # idle | running | done | error
if "run_id" not in st.session_state:
    st.session_state.run_id = 0

def append_log(line: str):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    st.session_state.run_logs.append(f"{ts} {line}")
    st.session_state.run_logs = st.session_state.run_logs[-5000:]

def worker_fn(limit: Optional[int], sample: bool, run_id: int):
    try:
        st.session_state.run_status = "running"
        append_log(f"[UI] Starting run: limit={limit}, sample={sample}")
        run_experiments(limit=limit, sample=sample, log=append_log)
        append_log(f"[UI] Run complete (run_id={run_id})")
        st.session_state.run_status = "done"
    except Exception as e:
        append_log(f"[UI][ERROR] run_id={run_id} {repr(e)}")
        st.session_state.run_status = "error"

with st.form("run_form"):
    with st.container(width=150):
        limit = st.number_input("Limit (0 means no limit)", min_value=0, max_value=10000, value=5, step=1)
    sample = st.checkbox("Random sample", value=True)
    submitted = st.form_submit_button("Launch")

t = st.session_state.run_thread
alive = bool(t and t.is_alive())

if submitted:
    if alive:
        st.warning("A run is already in progress.")
    else:
        st.session_state.run_id += 1
        run_id = st.session_state.run_id
        st.session_state.run_logs = []
        lim = None if limit == 0 else int(limit)

        st.session_state.run_thread = threading.Thread(target=worker_fn, args=(lim, sample, run_id), daemon=True)
        st.session_state.run_thread.start()
        st.success(f"Run started (run_id={run_id})")

# Status display
st.subheader("Status")
t = st.session_state.run_thread
alive = bool(t and t.is_alive())

status_map = {
    "idle": ("Idle", "complete"),
    "running": ("Pipeline running", "running"),
    "done": ("Pipeline finished", "complete"),
    "error": ("Pipeline failed", "error"),
}
label, state = status_map.get(st.session_state.run_status, ("Unknown", "warning"))
with st.status(label, state=state):
    st.write("Run id:", st.session_state.run_id)
    st.write("Thread status:", alive)
    st.write("Run status:", st.session_state.run_status)

# Live logs display
st.subheader("Live logs")
with st.container(height=500):
    st.code("\n".join(st.session_state.run_logs) or "(no logs yet)", language="text")

# Auto-refresh while running
if alive:
    time.sleep(0.5)
    st.rerun()

st.subheader("Outputs")
if RESULTS_DIR.exists():
    files = sorted([p for p in RESULTS_DIR.glob("*") if p.is_file()])
    for p in files:
        st.write(p.name, f"({p.stat().st_size} bytes)")
        st.download_button(f"Download {p.name}", data=p.read_bytes(), file_name=p.name)
else:
    st.info(f"No results dir yet: {RESULTS_DIR}")