import os
import sys
import json
from subprocess import Popen, PIPE
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

# Default temporary filenames (per process)
TMP_FAS = "tmp.fas"
TMP_HORIZ = "tmp.horiz"
TMP_A3M = "tmp.a3m"
TMP_HHR = "tmp.hhr"
PARSE_OUT = "hhr_parse.out"

# ---------------------------------

def run_parser(hhr_file):
    """
    Run the results_parser.py over the hhr file to produce the output summary.
    Writes PARSE_OUT in the current directory and prints its contents to stdout.
    """
    cmd = [PY_BIN, './results_parser.py', hhr_file]
    print(f'STEP 4: RUNNING PARSER: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    if p.returncode != 0:
        print("Parser FAILED:", err.decode("utf-8"), file=sys.stderr)
        sys.exit(1)
    print(out.decode("utf-8"))

def run_hhsearch(a3m_file, hhr_file):
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

def read_horiz(tmp_file, horiz_file, a3m_file):
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

def run_s4pred(input_file, out_file):
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

def get_result(parse_file):
    """
    Read CSV produced by results.parser and return dict mapping column_name -> value.
    """
    with open(parse_file, "r") as f:
        header = f.readline().strip().split(",")
        row = f.readline().strip().split(",")
    return dict(zip(header, row))

def run_pipeline_for_sequence(protein_id, sequence):
    """
    Run the 4-step pipeline for a single (protein_id, sequence) pair.

    Returns:
    """
    # 1. Write tmp FASTA for this sequence
    with open(TMP_FAS, "w") as fh_out:
        fh_out.write(f">{protein_id}\n")
        fh_out.write(f"{sequence}\n")

    # 2. S4Pred
    run_s4pred(TMP_FAS, TMP_HORIZ)

    # 3. Rewrite to A3M
    read_horiz(TMP_FAS, TMP_HORIZ, TMP_A3M)

    # 4. HHSerach
    run_hhsearch(TMP_A3M, TMP_HHR)
    
    # 5. Parse HHSearch output
    run_parser(TMP_HHR)

    # 6. Read parsed result from hhr_parse.out
    result = get_result(PARSE_OUT)
    return result

def run_pipeline_for_id(protein_id):
    """
    Fetch sequence for protein_id using storage.get_sequence() and run full pipeline.
    """
    seq = get_sequence(protein_id)
    return run_pipeline_for_sequence(protein_id, seq)


if __name__ == "__main__":
    # TODO: Extend to accept either id,list of ids, FASTA file?
    if len(sys.argv) != 2:
        print("Usage: python3 pipeline_script.py PROTEIN_ID", file=sys.stderr)
        sys.exit(1)
    
    protein_id = sys.argv[1]
    result = run_pipeline_for_id(protein_id)
    print(json.dumps(result, indent=2))