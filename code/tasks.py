from celery.signals import task_success, task_failure, task_prerun
from pathlib import Path
import traceback
import math
import json
import socket
import time
import os

from redis_state import get_redis, k_run, k_protein
from protein_pipeline import run_pipeline_for_id
from utils import _utc_now_iso
from celery_app import app

@app.task(name="analyse_protein")
def analyse_protein(run_id: str, protein_id: str) -> dict:
    """
    Celery task: run the 4-step pipeline for a single protein ID.

    Returns:
        dict with keys:
            query_id, best_hit, best_evalue, best_score, score_mean, score_std, score_gmean
    """
    host = socket.gethostname()
    pid  = os.getpid()
    now  = time.strftime("%Y-%m-%d %H:%M:%S")

    # which file/code version is actually running
    import tasks as tasks_mod
    st = f"[analyse_protein] {now} host={host} pid={pid} tasks_file={getattr(tasks_mod,'__file__',None)} run_id={run_id} protein_id={protein_id}"
    print(st, flush=True)

    r = get_redis()
    status = r.hget(k_protein(run_id, protein_id), "status") or ""

    # proves what Redis returned on THIS worker at runtime
    print(f"[analyse_protein] host={host} protein_id={protein_id} redis_status={status!r} type={type(status)}", flush=True)

    if isinstance(status, bytes):
        status = status.decode("utf-8", errors="replace")
    
    if status == "succeeded":
        h = r.hgetall(k_protein(run_id, protein_id))

        def ds(x):
            if isinstance(x, bytes):
                return x.decode("utf-8", errors="replace")
            return x
        
        h = {ds(k): ds(v) for k, v in h.items()}

        # query_id, best_hit, best_evalue, best_score, score_mean, score_std, score_gmean
        return {
            "query_id": protein_id,
            "ok": True,
            "best_hit": h.get("best_hit", ""),
            "best_evalue": h.get("best_evalue", ""),
            "best_score": h.get("best_score", ""),
            "score_mean": h.get("score_mean", ""),
            "score_std": h.get("score_std", ""),
            "score_gmean": h.get("score_gmean", ""),    
        }

    try:
        out = run_pipeline_for_id(protein_id)
        if not isinstance(out, dict):
            out = {}
        out["ok"] = True
        out.setdefault("query_id", protein_id)
        return out
    
    except KeyboardInterrupt:
        raise

    except BaseException as e:
        msg = f"{type(e).__name__}: {str(e)}"
        tb = traceback.format_exc(limit=20)
        # Return a normal results so chord can complete (with callback)
        return {
            "ok": False,
            "query_id": protein_id,
            "error": msg[:2000],
            "traceback": tb[:8000],
        }

@app.task(name="finalise_run")
def finalise_run(results, run_id):
    """
    Chord callback:
        - `results` is a list of dicts returned by `analyse_protein`
        - writes hits_output.csv and profile.csv into the run's directory
        - updates pp:run:{run_id} summary fields
    """
    r = get_redis()

    if not isinstance(results, list):
        results = []

    run_key = k_run(run_id)
    run_dir = r.hget(run_key, "run_dir") or ""
    if not run_dir:
        raise RuntimeError(f"Missing run_dir for run_id={run_id}")
    
    out_dir = Path(run_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    hits_rows = []
    failures = []
    sum_std = 0.0
    sum_gmean = 0.0
    std_count = 0
    gmean_count = 0
    ok_count = 0

    for item in results:
        if not isinstance(item, dict):
            continue
        
        ok = bool(item.get("ok", True))
        protein_id = item.get("query_id") or ""

        if not ok:
            failures.append((protein_id, item.get("error", "unknown error")))
            continue

        ok_count += 1

        best_hit = item.get("best_hit", "")
        if protein_id and best_hit:
            hits_rows.append((protein_id, best_hit))

        try:
            std_val = float(item.get("score_std", "nan"))
        except (TypeError, ValueError):
            std_val = float("nan")

        try:
            gmean_val = float(item.get("score_gmean", "nan"))
        except (TypeError, ValueError):
            gmean_val = float("nan")

        if not math.isnan(std_val):
            sum_std += std_val
            std_count += 1
        
        if not math.isnan(gmean_val):
            sum_gmean += gmean_val
            gmean_count += 1

    mean_std = (sum_std / std_count) if std_count else "nan"
    mean_gmean = (sum_gmean / gmean_count) if gmean_count else "nan"

    # Write hits_output.csv
    hits_fp = out_dir / "hits_output.csv"
    with hits_fp.open("w", encoding="utf-8") as fh_out:
        fh_out.write("fasta_id,best_hit_id\n")
        for fasta_id, best_hit_id in hits_rows:
            fh_out.write(f"{fasta_id},{best_hit_id}\n")

    # Write profile_output.csv
    profile_fp = out_dir / "profile_output.csv"
    with profile_fp.open("w", encoding="utf-8") as fh_out:
        fh_out.write("ave_std,ave_gmean\n")
        fh_out.write(f"{mean_std},{mean_gmean}\n")

    # Failures_output.csv
    failures_fp = out_dir / "failures_output.csv"
    with failures_fp.open("w", encoding="utf-8") as fh_out:
        fh_out.write("fasta_id,error\n")
        for fasta_id, err in failures:
            err = (err or "").replace("\n", " ").replace("\r", " ").replace(",", ";")
            fh_out.write(f"{fasta_id},{err}\n")

    # Update run status + summary in Redis
    r.hset(
        run_key,
        mapping={
            "status": "completed",
            "finished_at": _utc_now_iso(),
            "received_results": str(len(results)),
            "ok_results": str(ok_count),
            "hits_count": str(len(hits_rows)),
            "ave_std": str(mean_std),
            "ave_gmean": str(mean_gmean),
            "std_count": str(std_count),
            "gmean_count": str(gmean_count),
            "hits_csv": str(hits_fp),
            "profile_csv": str(profile_fp),
            "failures_csv": str(failures_fp),
        }
    )

    return {
        "run_id": run_id,
        "received_results": len(results),
        "ok_results": ok_count,
        "hits_csv": str(hits_fp),
        "profile_csv": str(profile_fp),
        "failures_csv": str(failures_fp),
    }

# Signal handlers to update Redis state on task success/failure

@task_success.connect
def on_task_success(sender=None, result=None, **kwargs):
    if sender is None or sender.name != "analyse_protein":
        return

    if not isinstance(result, dict):
        result = {}

    req = getattr(sender, "request", None)
    args = getattr(req, "args", None) or ()
    if len(args) < 2:
        return

    run_id, protein_id = args[0], args[1]
    r = get_redis()

    ok = bool(result.get("ok", True))
    protein_key = k_protein(run_id, protein_id)

    if ok:
        r.hset(
            protein_key,
            mapping={
                "ok": "true",
                "status": "succeeded",
                "finished_at": _utc_now_iso(),
                "best_hit": result.get("best_hit", ""),
                "score_std": str(result.get("score_std", "")),
                "score_gmean": str(result.get("score_gmean", "")),
            }
        )
        r.hincrby(k_run(run_id), "succeeded", 1)
    else:
        r.hset(
            protein_key,
            mapping={
                "ok": "false",
                "status": "failed",
                "finished_at": _utc_now_iso(),
                "error": result.get("error", "unknown error"),
            }
        )
        r.hincrby(k_run(run_id), "failed", 1)


@task_failure.connect
def on_task_failure(sender=None, exception=None, **kwargs):
    if sender is None or sender.name != "analyse_protein":
        return
    
    req = getattr(sender, "request", None)
    args = getattr(req, "args", None) or ()
    if len(args) < 2:
        return

    run_id, protein_id = args[0], args[1]
    r = get_redis()

    protein_key = k_protein(run_id, protein_id)

    prev_status = r.hget(protein_key, "status") or ""
    if prev_status in ("succeeded", "failed"):
        r.hset(
            protein_key,
            mapping={
                "hard_failed_at": _utc_now_iso(),
                "hard_error": str(exception) if exception is not None else "unknown exception",
                "hard_error_type": type(exception).__name__ if exception is not None else "UnknownType",
            }
        )
        return
    
    r.hset(
        protein_key,
        mapping={
            "ok": "false",
            "status": "failed",
            "finished_at": _utc_now_iso(),
            "error": str(exception) if exception is not None else "unknown exception",
            "failure_kind": "hard",
        }
    )
    # Update per-run count
    r.hincrby(k_run(run_id), "failed", 1)

@task_prerun.connect
def on_task_prerun(sender=None, task_id=None, args=None, **kwargs):
    if sender is None or sender.name != "analyse_protein":
        return

    if not args or len(args) != 2:
        return
    
    run_id, protein_id = args
    r = get_redis()
    key = k_protein(run_id, protein_id)
    
    status = r.hget(key, "status") or ""
    if isinstance(status, bytes):
        status = status.decode("utf-8", errors="replace")

    if status == "succeeded":
        return
    
    r.hset(
        key,
        mapping={
            "status": "running",
            "started_at": _utc_now_iso(),
        }
    )