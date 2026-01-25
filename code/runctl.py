import json
import os
import sys
import random
import argparse

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence
from celery import chord

from utils import _utc_now_iso
from tasks import analyse_protein, finalise_run
from redis_state import get_redis, new_run_id, init_run, add_protein, k_run

PIPELINE_RUNS_DIR = Path(
    os.environ.get(
        "PIPELINE_RUNS_DIR",
        "~/protein_pipeline/runs"
    )
).expanduser()

EXPERIMENT_IDS_FP = Path(
    os.environ.get(
        "EXPERIMENT_IDS_FP",
        "experiment_ids.txt"
    )
).expanduser()

@dataclass(frozen=True)
class RunSpec:
    run_id: str
    created_at_utc: str
    limit: Optional[int]
    sample: bool
    selected_ids_file: str
    run_dir: str

def select_ids(
    ids_file: Path,
    limit: Optional[int] = None,
    sample: bool = False,
    rng_seed: Optional[int] = None
) -> list[str]:
    """
    Generate full list of protein ids from experiment_ids to run pipeline analysis on.
    Parameters allow setting a limit (cap), and random sampling for testing.
    If rng_seed is provided, sampling is deterministic.
    """
    with ids_file.open("r") as fh_in:
        ids = [line.strip() for line in fh_in if line.strip()]
    
    if limit is None:
        return ids

    limit = int(limit)
    if limit <= 0:
        return ids
    
    if sample:
        if limit > len(ids):
            raise ValueError(f"Sample limit {limit} exceeds total ids {len(ids)}")
        r = random.Random(rng_seed)
        return r.sample(ids, limit)
    
    return ids[:limit]

def prepare_run(
    *,
    limit: Optional[int] = None,
    sample: bool = False,
    runs_dir: Path = PIPELINE_RUNS_DIR,
    ids_file: Path = EXPERIMENT_IDS_FP,
    run_id: Optional[str] = None,
    rng_seed: Optional[int] = None
) -> tuple[str, Path]:
    """
    Prepare a new run:
    - picks run_id,
    - selects protein ids,
    - writes selected ids to runs_dir/<run_id>.ids.txt,
    - writes spec JSON to runs_dir/<run_id>.json
    - initializes Redis run metadata

    Returns: (run_id, spec_path)
    """
    r = get_redis()

    r_id = run_id if run_id is not None else new_run_id()    
    run_dir = runs_dir / r_id
    run_dir.mkdir(parents=True, exist_ok=True)

    selected = select_ids(ids_file, limit=limit, sample=sample, rng_seed=rng_seed)
    submitted = len(selected)

    selected_fp = run_dir / "selected.ids.txt"
    spec_fp = run_dir / "spec.json"

    selected_fp.write_text("\n".join(selected) + ("\n" if selected else ""))

    spec = RunSpec(
        run_id=r_id,
        created_at_utc=_utc_now_iso(),
        limit=limit,
        sample=sample,
        selected_ids_file=str(selected_fp),
        run_dir=str(run_dir)
    )
    spec_fp.write_text(json.dumps(asdict(spec), indent=2, sort_keys=True) + "\n")

    init_run(
        r=r,
        run_id=r_id,
        limit=limit,
        sample=sample,
        submitted=submitted,
        run_dir=str(run_dir),
    )

    print(f"[PREPARE] run_id={r_id} | submitted={submitted} | spec={spec_fp}")
    return r_id, spec_fp

def _load_spec(spec_path: Path) -> RunSpec:
    data = json.loads(spec_path.read_text())
    return RunSpec(**data)

def execute_run(*, run_id: str) -> int:
    """
    Execute an existing prepared run:
    - loads spec
    - creates chord(header=analyse_protein tasks, callback=finalise_run)
    - records task_id per protein in Redis before submitting
    - submits chord

    Returns: number of submitted tasks
    """
    r = get_redis()

    spec_path  = PIPELINE_RUNS_DIR / run_id / "spec.json"
    if not spec_path.exists():
        raise FileNotFoundError(f"Run spec not found: {spec_path}")
    
    spec = _load_spec(spec_path=spec_path)

    if spec.run_id != run_id:
        raise ValueError(f"Run ID mismatch: spec.run_id={spec.run_id} != run_id={run_id}")

    ids_fp = Path(spec.selected_ids_file)
    if not ids_fp.exists():
        raise FileNotFoundError(f"Selected ids file not found: {ids_fp}")
    
    protein_ids = [line.strip() for line in ids_fp.read_text().splitlines() if line.strip()]
    if not protein_ids:
        raise ValueError(f"No protein ids found in: {ids_fp}")
    
    # Build signature for chord header (allocating task ids)
    sigs = [analyse_protein.s(run_id, pid) for pid in protein_ids]
    frozen = [s.freeze() for s in sigs]

    for pid, ar in zip(protein_ids, frozen):
        add_protein(
            r=r,
            run_id=run_id,
            protein_id=pid,
            task_id=ar.id
        )
        print(f"[SUBMIT] run_id={run_id} | protein={pid} | task_id={ar.id}")

    # Mark run as running
    r.hset(k_run(run_id), mapping={"status": "running", "started_at": _utc_now_iso()})

    # Sumbit chord: callback runs on 'finalise' queue
    callback = finalise_run.s(run_id)
    chord_res = chord(sigs)(callback)

    # Store enqueued and chord id for debugging/traceability
    enqueued = len(protein_ids)  
    r.hset(k_run(run_id), mapping={"enqueued": enqueued, "chord_id": chord_res.id})

    print(f"[EXECUTE] run_id={run_id} | enqueued={enqueued} | chord_id={chord_res.id}")
    return enqueued

def submit_run(
    *,
    limit: Optional[int] = None,
    sample: bool = False,
    run_id: Optional[str] = None,
    rng_seed: Optional[int] = None
) -> tuple[str, Path]:
    """
    Prepare and execute a new run:
    - prepare_run()
    - execute_run()

    Returns: (run_id, spec_path)
    """
    r_id, spec_fp = prepare_run(
        limit=limit,
        sample=sample,
        run_id=run_id,
        rng_seed=rng_seed
    )
    execute_run(run_id=r_id)
    return r_id, spec_fp

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Protein Pipeline runner (prepare/execute)")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_prepare = sub.add_parser("prepare", help="Prepare anew run (create spec + Redis init)")
    p_prepare.add_argument("--limit", type=int, default=None, help="Max number of experiments to run")
    p_prepare.add_argument("--sample", action="store_true", help="Random sampling of selected ids")
    p_prepare.add_argument("--run-id", type=str, default=None, help="Force run id")
    p_prepare.add_argument("--seed", type=int, default=None, help="RNG seed for determinsitc sampling")

    p_execute = sub.add_parser("execute", help="Execute a prepared run (enqueue tasks from spec)")
    p_execute.add_argument("--run-id", type=str, required=True, help="Run ID to execute")

    p_submit = sub.add_parser("submit", help="Prepare and execute a run")
    p_submit.add_argument("--limit", type=int, default=None, help="Max number of experiments to run")
    p_submit.add_argument("--sample", action="store_true", help="Random sampling of selected ids")
    p_submit.add_argument("--run-id", type=str, default=None, help="Force run id")
    p_submit.add_argument("--seed", type=int, default=None, help="RNG seed for determinsitc sampling")

    return p

def main(argv: Sequence[str]) -> int:
    args = build_parser().parse_args(argv)

    try:
        if args.cmd == "prepare":
            r_id, spec_fp = prepare_run(
                limit=args.limit,
                sample=args.sample,
                run_id=args.run_id,
                rng_seed=args.seed
            )
            print(f"[OK] run_id={r_id}, spec={spec_fp}")
            return 0
        
        if args.cmd == "execute":
            execute_run(run_id=args.run_id)
            return 0

        if args.cmd == "submit":
            r_id, spec_fp = submit_run(
                limit=args.limit,
                sample=args.sample,
                run_id=args.run_id,
                rng_seed=args.seed
            )
            print(f"[OK] run_id={r_id}, spec={spec_fp}")
            return 0
        
        raise RuntimeError(f"Unknown command: {args.cmd}")
    
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))