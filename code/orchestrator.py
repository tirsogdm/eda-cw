import os
import json
import time
import random
from pathlib import Path

from tasks import analyse_protein

RESULTS_DIR = Path(
    os.environ.get(
        "RESULTS_DIR",
        "~/protein_pipeline/code/results"
    )
).expanduser()

def submit_some_test_tasks(num_tasks: int=5):
    """
    Submit a small batch of random tasks to test the Celery pipeline
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    # --- Load exprimental ids ---
    with open("experiment_ids.txt", "r") as fh_in:
        lines = [line.strip() for line in fh_in if line.strip()]

    test_ids = random.sample(lines, num_tasks)

    # --- Submit tasks ---
    async_results = {}
    for protein_id in test_ids:
        res_obj = analyse_protein.delay(protein_id)
        async_results[res_obj.id] = (protein_id, res_obj)
        print(f"[SUBMIT] protein={protein_id} | task_id={res_obj.id}")

    # --- Monitor tasks ---
    remaining = dict(async_results)
    completed = []

    while remaining:
        print(f"[MONITOR] Pending tasks: {len(remaining)}")
        for task_id, (protein_id, res_obj) in list(remaining.items()):
            if res_obj.ready():
                if res_obj.successful():
                    result = res_obj.result # run_pipeline_for_id output dict
                    completed.append((protein_id, result))
                    print(f"[DONE] {protein_id} -> {result['best_hit']} (task_id={task_id})")
                else:
                    print(f"[ERROR] Task {task_id} for {protein_id} failed: {res_obj.result}")
                del remaining[task_id]

        if remaining:
            time.sleep(2)
    
    # --- Write results to JSON ---
    out_path = RESULTS_DIR / "test_results.json"
    with open(out_path, "w") as fh_out:
        json.dump(
            {protein_id: result for (protein_id, result) in completed},
            fh_out,
            indent=2,
        )
    print(f"[OK] Wrote {len(completed)} results to {out_path}")

if __name__ == "__main__":
    submit_some_test_tasks()