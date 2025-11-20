from celery_app import app
from protein_pipeline import run_pipeline_for_id

@app.task(name="analyse_protein")
def analyse_protein(protein_id: str) -> dict:
    """
    Celery task: run the 4-step pipeline for a single protein ID.

    Returns:
        dict with keys:
            query_id, best_hit, best_evalue, best_score, score_mean, score_std, score_gmean
    """
    return run_pipeline_for_id(protein_id)