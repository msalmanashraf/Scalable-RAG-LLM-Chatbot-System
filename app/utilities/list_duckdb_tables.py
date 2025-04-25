import duckdb
import sys
import os

def list_tables_and_data(db_file):
    try:
        # Connect to the DuckDB database
        conn = duckdb.connect(db_file)

        # Get the list of tables
        tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';"
        tables = conn.execute(tables_query).fetchall()

        if not tables:
            print("No tables found in the database.")
            return

        # Iterate through each table and display its data
        for table in tables:
            table_name = table[0]
            print(f"\nTable: {table_name}")
            print("-" * (len(table_name) + 7))

            # Query data from the table
            data_query = f"SELECT * FROM {table_name};"
            data = conn.execute(data_query).fetchdf()

            # Display data if the table is not empty
            if not data.empty:
                print(data)
            else:
                print("This table is empty.")

        # Close the connection
        conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Usage: python script.py ")
        sys.exit(1)

    parent_dir = os.path.dirname(os.path.abspath(__file__))
    db_file = os.path.join(parent_dir, '..', 'embeddings', 'embeddings.duckdb')
    list_tables_and_data(db_file)
