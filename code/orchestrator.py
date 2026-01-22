import os
import sys
import time
import math
import random
from pathlib import Path
from typing import Callable, Optional

from tasks import analyse_protein

RESULTS_DIR = Path(
    os.environ.get(
        "RESULTS_DIR",
        "~/protein_pipeline/code/results"
    )
).expanduser()

def select_ids(limit: Optional[int] = None, sample: Optional[bool] = False) -> list[str]:
    """
    Generate full list of protein ids from experiment_ids to run pipeline analysis on.
    Parameters allow setting a limit (cap), and random sampling for testing.
    """
    with open("experiment_ids.txt", "r") as fh_in:
        ids = [line.strip() for line in fh_in if line.strip()]
    if limit is not None:
        limit = int(limit)
        if sample:
            return random.sample(ids, limit)
        return ids[:limit]
    return ids

def run_experiments(limit: Optional[int] = None, sample: Optional[bool] = False, log: Optional[Callable[[str], None]] = None):
    """
    Submit a batch of tasks on Celery pipeline, and monitor, aggregate, and write out results.
    """
    def _log(msg: str):
        if log:
            log(msg)
        else:
            print(msg)
        
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    selected_ids = select_ids(limit, sample)

    # Submit tasks
    async_results = {}
    for protein_id in selected_ids:
        res_obj = analyse_protein.delay(protein_id)
        async_results[res_obj.id] = (protein_id, res_obj)
        _log(f"[SUBMIT] protein={protein_id} | task_id={res_obj.id}")

    submitted = len(selected_ids)
    failed = 0
    profile_count = 0 # non-NaN std & gmean

    # Monitor tasks
    remaining = dict(async_results)
    hits_rows = []
    sum_std = 0.0
    sum_gmean = 0.0

    while remaining:
        _log(f"[MONITOR] Pending tasks: {len(remaining)}")
        for task_id, (protein_id, res_obj) in list(remaining.items()):
            if res_obj.ready():
                if res_obj.successful():
                    result = res_obj.result # run_pipeline_for_id output dict
                    hits_rows.append((protein_id, result['best_hit']))

                    std_val = float(result['score_std'])
                    gmean_val = float(result['score_gmean'])

                    # Only include in averages if both are real numbers
                    if not (math.isnan(std_val) or math.isnan(gmean_val)):
                        sum_std += std_val
                        sum_gmean += gmean_val
                        profile_count += 1
                    _log(f"[DONE] {protein_id} -> {result['best_hit']} (task_id={task_id})")
                else:
                    failed += 1
                    _log(f"[ERROR] Task {task_id} for {protein_id} failed: {repr(res_obj.result)}")
                del remaining[task_id]

        if remaining:
            time.sleep(2)

    succeeded = len(hits_rows)
    _log(f"[SUMMARY] submitted={submitted} | succeeded={succeeded} | failed={failed} | profile_count={profile_count}")

    if profile_count == 0:
        _log("[WARNING] No successful non-NaN tasks, skipping CSV generation.")
        return
    
    # Compute profile statistics
    mean_std = sum_std / profile_count
    mean_gmean = sum_gmean / profile_count
    _log(f"[INFO] Mean std: {mean_std} | Mean gmean: {mean_gmean}")

    # Write hits_outputs.csv
    hits_fp = RESULTS_DIR / "hits_output.csv"
    with open(hits_fp, "w") as fh_out:
        fh_out.write("fasta_id,best_hit_id\n")
        for fasta_id, best_hit_id in hits_rows:
            fh_out.write(f"{fasta_id},{best_hit_id}\n")

    profile_fp = RESULTS_DIR / "profile_output.csv"
    with open(profile_fp, "w") as fh_out:
        fh_out.write("ave_std,ave_gmean\n")
        fh_out.write(f"{mean_std},{mean_gmean}\n")

    _log(f"[OK] Wrote: {hits_fp}")
    _log(f"[OK] Wrote: {profile_fp}")

def pusage_exit():
    print(
        "Usage: python3 orchestrator.py [LIMIT] [SAMPLE]\n"
        "--------\n"
        "LIMIT: int (optional) - Max number of experiments run\n"
        "SAMPLE: bool (optional) - Randomise order of selected ids",
        file=sys.stderr,
    )
    sys.exit(1)


if __name__ == "__main__":
    argc = len(sys.argv)
    
    if argc == 1:
        run_experiments()
    elif argc == 2:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            pusage_exit()
        run_experiments(limit=limit)
    elif argc == 3:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            pusage_exit()
        sample_str = sys.argv[2].lower()
        sample = sample_str in ("true", "1", "yes", "y")
        run_experiments(limit=limit, sample=sample)
    else:
        pusage_exit()