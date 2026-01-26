import os
import uuid
from typing import Any, Dict, Optional

import redis

from utils import _utc_now_iso

def get_redis() -> redis.Redis:
    """
    Uses env vars
    """
    host = os.getenv("REDIS_HOST", "controller-node")
    port = int(os.getenv("REDIS_PORT", "6379"))
    password = os.getenv("REDIS_PASSWORD", "imaproteinpipelinerunner") or None
    db = int(os.getenv("REDIS_DB", "1"))
    return redis.Redis(host=host, port=port, password=password, db=db, decode_responses=True)

def new_run_id() -> str:
    """
    Globally unique run ID
    """
    return uuid.uuid4().hex

def k_run(run_id: str) -> str:
    return f"pp:run:{run_id}"

def k_run_proteins(run_id: str) -> str:
    return f"pp:run:{run_id}:proteins"

def k_protein(run_id: str, protein_id: str, ) -> str:
    return f"pp:run:{run_id}:protein:{protein_id}"

def init_run(
    r: redis.Redis,
    run_id: str,
    limit: Optional[int],
    sample: bool,
    submitted: int,
    run_dir: str,
) -> None:
    """
    Initialize a new run in Redis - store minimal metadata
    """
    mapping={
        "run_id": run_id,
        "status": "submitted",
        "created_at": _utc_now_iso(),
        "limit": "" if limit is None else str(limit),
        "sample": "true" if sample else "false",
        "submitted": str(submitted),
        "run_dir": run_dir,
        "succeeded": "0",
        "failed": "0",
    }

    r.hset(k_run(run_id), mapping=mapping)

def add_protein(
    r: redis.Redis,
    run_id: str,
    protein_id: str,
    task_id: str
) -> None:
    """
    Add a new protein to the run in Redis - store minimal metadata
    """
    key = k_protein(run_id, protein_id)

    # Memership set
    r.sadd(k_run_proteins(run_id), protein_id)

    # If protein already exists, DO NOT overwrite analysis
    if r.exists(key):
        r.hset(key, mapping={"task_id": task_id})
        return

    # Per-protein hash (only on first submit)
    r.hset(
        key,
        mapping={
            "protein_id": protein_id,
            "task_id": task_id,
            "status": "submitted",
            "created_at": _utc_now_iso(),
            "ok": "",
        }
    )