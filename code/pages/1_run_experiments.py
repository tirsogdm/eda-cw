import subprocess
from pathlib import Path

import streamlit as st

PIPELINE_CODE_DIR = Path("/home/almalinux/protein_pipeline/code")
PYTHON_BIN = Path("/home/almalinux/protein_pipeline/venv/bin/python")
ORCH = PIPELINE_CODE_DIR / "orchestrator.py"

st.set_page_config(page_title="Run Experiments", layout="wide")
st.title("Run Experiments")

st.caption(f"Python: {PYTHON_BIN}")
st.caption(f"Woring dir: {PIPELINE_CODE_DIR}")
st.caption(f"Orchestrator: {ORCH}")

limit = None
if st.checkbox("Set limit"):
    limit = st.number_input("Limit", min_value=1, max_value=100_000, value=5, step=1)
sample = st.checkbox("Sample (random)", value=True)

run_btn = st.button("Run orchestrator", type="primary")

log_box = st.empty()

if run_btn:
    if not PYTHON_BIN.exists():
        st.error(f"Missing python binary: {PYTHON_BIN}")
        st.stop()
    if not ORCH.exists():
        st.error(f"Missing orchestrator: {ORCH}")
        st.stop()
    
    if limit:
        cmd = [str(PYTHON_BIN), str(ORCH), str(int(limit)), "true" if sample else "false"]
    else:
        cmd = [str(PYTHON_BIN), str(ORCH)]
    st.write("Command:")
    st.code(" ".join(cmd))
    
    proc = subprocess.Popen(
        cmd,
        cwd=str(PIPELINE_CODE_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    output_lines = []
    for line in proc.stdout:
        output_lines.append(line.rstrip("\n"))
        output_lines = output_lines[-500:]
        log_box.code("\n".join(output_lines))
    
    rc = proc.wait()
    if rc == 0:
        st.success("Orchestrator finished successfully.")
    else:
        st.error(f"Orchestrator exited with code {rc}")
    