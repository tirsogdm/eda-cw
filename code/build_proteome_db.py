import sys
import sqlite3
from pathlib import Path
from Bio import SeqIO

def build_db(fasta_path: str, db_path: str) -> None:
    """
    """
    fasta_path = Path(fasta_path)
    db_path = Path(db_path)

    if not fasta_path.is_file():
        raise FileNotFoundError(f"FASTA file not found: {fasta_path}")
    
    print(f"Building SQLite DB from FASTA: {fasta_path}")
    print(f"Output DB: {db_path}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute(""" 
        CREATE TABLE IF NOT EXISTS proteins (
            id TEXT PRIMARY KEY,
            sequence TEXT NOT NULL
        )
    """)
    conn.commit()

    count = 0
    with fasta_path.open("r") as fs:
        for record in SeqIO.parse(fs, "fasta"):
            pid = record.id
            seq = str(record.seq)
            cur.execute(
                "INSERT OR REPLACE INTO proteins (id, sequence) VALUES (?, ?)",
                (pid, seq)
            )
            count += 1
            if count % 1000 == 0:
                print(f"Inserted {count} sequences...")
                conn.commit()    
    
    conn.commit()
    conn.close()
    print(f"Done. Inserted {count} sequences.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python build_proteome_db.py <input_fasta> <output_db>")
        sys.exit(1)
    
    fasta = sys.argv[1]
    db = sys.argv[2]
    build_db(fasta, db)