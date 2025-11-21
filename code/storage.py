import os
import sqlite3
from pathlib import Path
from functools import lru_cache

DEFAULT_DB_PATH = os.environ.get(
    "MOUSE_PROTEOME_DB",
    "/opt/protein_pipeline/data/mouse_proteome.db"
)

@lru_cache(maxsize=1)
def _get_db_connection(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    db_path = Path(db_path)
    if not db_path.is_file():
        raise FileNotFoundError(f"Database file not found at {db_path}")
    return sqlite3.connect(db_path, check_same_thread=False)

def get_sequence(protein_id: str, db_path: str = DEFAULT_DB_PATH) -> str:
    conn = _get_db_connection(db_path)
    cur = conn.cursor()
    cur.execute("SELECT sequence FROM proteins WHERE id = ?", (protein_id,))
    row = cur.fetchone()
    if row is None:
        raise KeyError(f"Protein ID {protein_id} not found in database.")
    return row[0]