# create_sql_db.py
"""
Creates a SQLite database from the cleaned Spotify dataset.

- Input:  spotify_clean.csv   (must be in the same folder)
- Output: spotify_tracks.db   (SQLite DB with a table named 'tracks')
"""

import os
import sqlite3
import pandas as pd


def main():
    csv_path = "spotify_clean.csv"
    db_path = "spotify_tracks.db"
    table_name = "tracks"

    # 1. Check if CSV exists
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Could not find '{csv_path}'. Make sure the cleaned CSV "
            f"is in the same folder as this script."
        )

    print(f"📁 Loading data from {csv_path} ...")
    df = pd.read_csv(csv_path)

    # Optional: show basic info
    print(f"✅ Data loaded: {df.shape[0]} rows, {df.shape[1]} columns")

    # 2. Create SQLite connection
    print(f"🛠 Creating SQLite database: {db_path} ...")
    conn = sqlite3.connect(db_path)

    try:
        # 3. Write DataFrame to SQL table
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"✅ Table '{table_name}' created/overwritten successfully in {db_path}")

        # 4. Quick sanity check: count rows
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cur.fetchone()[0]
        print(f"📊 Row count in '{table_name}': {row_count}")

    finally:
        conn.close()
        print("🔒 Connection closed.")


if __name__ == "__main__":
    main()
