"""
migrate_sqlite_to_mysql.py

Migrates Spotify data from SQLite to MySQL.
- Reads from: spotify_tracks.db (SQLite)
- Writes to: MySQL database (configurable via command-line args or defaults)

Usage:
    python migrate_sqlite_to_mysql.py [--host localhost] [--user root] [--password] [--db spotify]

Prerequisites:
    - MySQL server running
    - python -m pip install mysql-connector-python pandas
"""

import sqlite3
import pandas as pd
import argparse
import logging
import sys

try:
    import mysql.connector
    from mysql.connector import Error
except ImportError:
    print("ERROR: mysql-connector-python not installed.")
    print("Install with: pip install mysql-connector-python")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def read_sqlite(db_path: str = "spotify_tracks.db", table_name: str = "tracks") -> pd.DataFrame:
    """Read table from SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_csv(db_path.replace(".db", ".csv"))  # fallback to CSV if needed
        # Actually, read from SQLite:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        logger.info(f"✅ Read {len(df)} rows from SQLite table '{table_name}'")
        return df
    except Exception as e:
        logger.error(f"❌ Failed to read from SQLite: {e}")
        raise


def connect_mysql(host: str, user: str, password: str, database: str = None) -> mysql.connector.MySQLConnection:
    """Connect to MySQL server."""
    try:
        config = {
            'host': host,
            'user': user,
            'password': password,
        }
        if database:
            config['database'] = database

        conn = mysql.connector.connect(**config)
        logger.info(f"✅ Connected to MySQL at {host}")
        return conn
    except Error as e:
        logger.error(f"❌ Failed to connect to MySQL: {e}")
        raise


def create_mysql_database(conn: mysql.connector.MySQLConnection, database: str):
    """Create database if it doesn't exist."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        cursor.execute(f"USE `{database}`")
        conn.commit()
        logger.info(f"✅ Database '{database}' created/selected")
    except Error as e:
        logger.error(f"❌ Failed to create database: {e}")
        raise
    finally:
        cursor.close()


def migrate_to_mysql(df: pd.DataFrame, conn: mysql.connector.MySQLConnection, table_name: str = "tracks"):
    """Write DataFrame to MySQL table."""
    cursor = conn.cursor()
    try:
        # Drop existing table if present
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        conn.commit()

        # Create table with proper schema
        # Map pandas dtypes to MySQL column types
        col_defs = []
        for col, dtype in zip(df.columns, df.dtypes):
            if dtype == 'object':
                max_len = df[col].astype(str).str.len().max()
                col_len = min(max(max_len + 10, 50), 500)  # reasonable varchar length
                col_defs.append(f"`{col}` VARCHAR({col_len})")
            elif dtype == 'int64':
                col_defs.append(f"`{col}` BIGINT")
            elif dtype == 'float64':
                col_defs.append(f"`{col}` DOUBLE")
            elif dtype == 'bool':
                col_defs.append(f"`{col}` BOOLEAN")
            else:
                col_defs.append(f"`{col}` TEXT")

        create_table_sql = f"CREATE TABLE `{table_name}` ({', '.join(col_defs)})"
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info(f"✅ Table '{table_name}' created in MySQL")

        # Insert data in batches
        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            placeholders = ', '.join(['%s'] * len(batch.columns))
            insert_sql = f"INSERT INTO `{table_name}` ({', '.join(f'`{c}`' for c in batch.columns)}) VALUES ({placeholders})"

            for _, row in batch.iterrows():
                cursor.execute(insert_sql, tuple(row))
            conn.commit()
            logger.info(f"  Inserted {min(i+batch_size, len(df))}/{len(df)} rows...")

        logger.info(f"✅ All {len(df)} rows inserted into MySQL table '{table_name}'")

    except Error as e:
        logger.error(f"❌ Failed to migrate data: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description='Migrate Spotify SQLite database to MySQL')
    parser.add_argument('--sqlite-db', default='spotify_tracks.db', help='Path to SQLite database')
    parser.add_argument('--sqlite-table', default='tracks', help='Table name in SQLite')
    parser.add_argument('--host', '-H', default='localhost', help='MySQL host (default: localhost)')
    parser.add_argument('--user', '-u', default='root', help='MySQL user (default: root)')
    parser.add_argument('--password', '-p', default='', help='MySQL password (default: empty)')
    parser.add_argument('--db', '-d', default='spotify', help='MySQL database name (default: spotify)')
    parser.add_argument('--mysql-table', default='tracks', help='Table name in MySQL (default: tracks)')

    args = parser.parse_args(argv)

    try:
        # Read from SQLite
        df = read_sqlite(args.sqlite_db, args.sqlite_table)

        # Connect to MySQL
        conn = connect_mysql(args.host, args.user, args.password)

        # Create database
        create_mysql_database(conn, args.db)

        # Migrate data
        migrate_to_mysql(df, conn, args.mysql_table)

        logger.info("✅ Migration complete!")
        conn.close()

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
