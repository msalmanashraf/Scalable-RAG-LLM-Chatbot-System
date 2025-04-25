import duckdb
import sys
import os

def clear_table(db_path: str, table_name: str):
    conn = duckdb.connect(db_path)
    
    if table_name.lower() == "all":
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        for table in tables:
            print(f"Clearing data from table: {table[0]}")
            conn.execute(f"DELETE FROM {table[0]}")
    else:
        print(f"Clearing data from table: {table_name}")
        conn.execute(f"DELETE FROM {table_name}")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <database_path> <table_name|all>")
        sys.exit(1)
    
    parent_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(parent_dir, '..', 'embeddings', 'embeddings.duckdb')
    table_name = sys.argv[1]
    
    clear_table(db_path, table_name)
