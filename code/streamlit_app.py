import os
from pathlib import Path
import streamlit as st

RESULTS_DIR = Path(
    os.environ.get(
        "RESULTS_DIR",
        "~/protein_pipeline/code/results"
    )
).expanduser()

st.set_page_config(page_title="Protein Analysis", layout="wide")

st.title("Protein Analysis Dashboard")

col1, col2 = st.columns(2)

with col1:
    st.subheader("System")
    st.write("Controller:", os.uname().nodename)
    st.write("Results directory:", str(RESULTS_DIR))

with col2:
    st.subheader("Quick Checks")
    if st.button("Create results directory"):
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        st.success(f"Ensured {RESULTS_DIR} exists")

st.divider()

st.subheader("Results")
if RESULTS_DIR.exists():
    files = sorted([p for p in RESULTS_DIR.glob("**/*") if p.is_file()])
    st.write(f"Found {len(files)} files")
    for file in files[:50]:
        st.write(f"- {str(file)}")
else:
    st.warning(f"Results directory {RESULTS_DIR} does not exist yet.")