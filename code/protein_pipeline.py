import os
import sys
import json
import tempfile
from subprocess import Popen, PIPE
from pathlib import Path
from Bio import SeqIO

from storage import get_sequence

# ---------------------------------
# ------ Configuration paths ------
# ---------------------------------

# Python: binary
PY_BIN = os.environ.get("PY_BIN", "python3")

# S4Pred: script
S4PRED_SCRIPT = os.environ.get(
    "S4PRED_SCRIPT", 
    os.path.expanduser("~/protein_pipeline/tools/s4pred/run_model.py")
)

# HHSearch: binary and database
HHSEARCH_BIN = os.environ.get(
    "HHSEARCH_BIN", 
    os.path.expanduser("~/protein_pipeline/tools/hh-suite/bin/hhsearch")
)

HHSEARCH_DB = os.environ.get(
    "HHSEARCH_DB", 
    os.path.expanduser("~/protein_pipeline/data/pdb70/pdb70")
)

# ---------------------------------

def run_parser(hhr_file: str, out_file: str):
    """
    Run the results_parser.py over the hhr file to produce the output summary.
    Writes PARSE_OUT in the current directory and prints its contents to stdout.
    """
    cmd = [PY_BIN, './results_parser.py', hhr_file, out_file]
    print(f'STEP 4: RUNNING PARSER: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    if p.returncode != 0:
        print("Parser FAILED:", err.decode("utf-8"), file=sys.stderr)
        sys.exit(1)
    print(out.decode("utf-8"))

def run_hhsearch(a3m_file: str, hhr_file: str):
    """
    Run HHSearch to produce the hhr file.
    """
    cmd = [HHSEARCH_BIN, '-i', a3m_file, '-cpu', '1', '-d', HHSEARCH_DB, '-o', hhr_file]
    print(f'STEP 3: RUNNING HHSEARCH: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    if p.returncode != 0:
        print("HHSearch FAILED:", err.decode("utf-8"), file=sys.stderr)
        sys.exit(1)

def read_horiz(tmp_file: str, horiz_file: str, a3m_file: str):
    """
    Parse horiz file and concatenate the information to a new tmp a3m file.
    """
    pred = ''
    conf = ''
    print("STEP 2: REWRITING INPUT FILE TO A3M")
    with open(horiz_file) as fh_in:
        for line in fh_in:
            if line.startswith('Conf: '):
                conf += line[6:].rstrip()
            if line.startswith('Pred: '):
                pred += line[6:].rstrip()
    with open(tmp_file) as fh_in:
        contents = fh_in.read()
    with open(a3m_file, "w") as fh_out:
        fh_out.write(f">ss_pred\n{pred}\n>ss_conf\n{conf}\n")
        fh_out.write(contents)

def run_s4pred(input_file: str, out_file: str):
    """
    Runs the s4pred secondary structure predictor to produce the horiz file.
    """
    cmd = [PY_BIN, S4PRED_SCRIPT, '-t', 'horiz', '-T', '1', input_file]
    print(f'STEP 1: RUNNING S4PRED: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    if p.returncode != 0:
        print("S4Pred FAILED:", err.decode("utf-8"), file=sys.stderr)
        sys.exit(1)
    with open(out_file, "w") as fh_out:
        fh_out.write(out.decode("utf-8"))

def get_result(parse_file: str) -> dict:
    """
    Read CSV produced by results.parser and return dict mapping column_name -> value.

    Returns:
        dict with keys:
            query_id, best_hit, best_evalue, best_score, score_mean, score_std, score_gmean
    """
    with open(parse_file, "r") as fh_in:
        header = fh_in.readline().strip().split(",")
        row = fh_in.readline().strip().split(",")
    return dict(zip(header, row))

def run_pipeline_for_sequence(protein_id: str, sequence: str) -> dict:
    """
    Run the 4-step pipeline for a single (protein_id, sequence) pair.
    Creates a unique temporary working directory for each run.

    Returns:
        dict with keys:
            query_id, best_hit, best_evalue, best_score, score_mean, score_std, score_gmean
    """
    # --- 1. Create unique working directory ----
    work_dir = Path(tempfile.mkdtemp(prefix=f"pp_{protein_id.replace('|','_')}_"))
    print(f"[DEBUG] Working directory: {work_dir}")

    tmp_fas   = work_dir / "tmp.fas"
    tmp_horiz = work_dir / "tmp.horiz"
    tmp_a3m   = work_dir / "tmp.a3m"
    tmp_hhr   = work_dir / "tmp.hhr"
    parse_out = work_dir / "hhr_parse.out"

    # --- 2. Write tmp FASTA for this sequence ---
    with open(tmp_fas, "w") as fh_out:
        fh_out.write(f">{protein_id}\n{sequence}\n")

    # --- 3. Run all pipeline steps ---
    run_s4pred(str(tmp_fas), str(tmp_horiz))
    read_horiz(str(tmp_fas), str(tmp_horiz), str(tmp_a3m))
    run_hhsearch(str(tmp_a3m), str(tmp_hhr))
    run_parser(str(tmp_hhr),  str(parse_out))

    # --- 4. Read parsed result from hhr_parse.out ---
    result = get_result(str(parse_out))

    # --- 5. Clean up tmp directory
    # shutil.rmtree(work_dir, ignore_errors=True)

    return result

def run_pipeline_for_id(protein_id: str) -> dict:
    """
    Fetch sequence for protein_id using storage.get_sequence() and run full pipeline.

    Returns:
        dict with keys:
            query_id, best_hit, best_evalue, best_score, score_mean, score_std, score_gmean
    """
    seq = get_sequence(protein_id)
    return run_pipeline_for_sequence(protein_id, seq)


if __name__ == "__main__":
    # TODO: Extend to accept either id,list of ids, FASTA file?
    if len(sys.argv) != 2:
        print("Usage: python3 protein_pipeline.py PROTEIN_ID", file=sys.stderr)
        sys.exit(1)
    
    protein_id = sys.argv[1]
    result = run_pipeline_for_id(protein_id)
    print(json.dumps(result, indent=2))