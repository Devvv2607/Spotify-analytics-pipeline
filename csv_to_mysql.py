"""
csv_to_mysql.py

Loads Spotify data directly from CSV to MySQL database.
- Input:  spotify_clean.csv
- Output: MySQL database with 'tracks' table

Usage:
    python csv_to_mysql.py [--csv spotify_clean.csv] [--host localhost] [--user root] [--password] [--db spotify]

Prerequisites:
    - MySQL server running
    - python -m pip install mysql-connector-python pandas
"""

import pandas as pd
import argparse
import logging
import sys
import mysql.connector
from mysql.connector import Error

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def read_csv(csv_path: str) -> pd.DataFrame:
    """Read CSV file into DataFrame."""
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"✅ Read {len(df)} rows from {csv_path}")
        return df
    except Exception as e:
        logger.error(f"❌ Failed to read CSV: {e}")
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


def infer_mysql_type(dtype, max_len=None):
    """Map pandas dtype to MySQL column type."""
    if dtype == 'object':
        col_len = min(max(max_len + 10, 50), 500) if max_len else 255
        return f"VARCHAR({col_len})"
    elif dtype == 'int64':
        return "BIGINT"
    elif dtype == 'float64':
        return "DOUBLE"
    elif dtype == 'bool':
        return "BOOLEAN"
    else:
        return "TEXT"


def load_csv_to_mysql(df: pd.DataFrame, conn: mysql.connector.MySQLConnection, table_name: str = "tracks"):
    """Write DataFrame to MySQL table."""
    cursor = conn.cursor()
    try:
        # Drop existing table if present
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        conn.commit()
        logger.info(f"📋 Dropped existing table '{table_name}' (if any)")

        # Create table with proper schema
        col_defs = []
        for col, dtype in zip(df.columns, df.dtypes):
            if dtype == 'object':
                max_len = df[col].astype(str).str.len().max()
                col_len = min(max(int(max_len) + 100, 255), 65535)  # Allow up to 64KB for text
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
        logger.info(f"✅ Table '{table_name}' created with {len(df.columns)} columns")

        # Insert data in batches
        batch_size = 1000
        total_rows = len(df)
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]
            placeholders = ', '.join(['%s'] * len(batch.columns))
            col_names = ', '.join(f'`{c}`' for c in batch.columns)
            insert_sql = f"INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders})"

            for _, row in batch.iterrows():
                # Handle NaN values
                values = tuple(None if pd.isna(v) else v for v in row)
                cursor.execute(insert_sql, values)
            
            conn.commit()
            current = min(i + batch_size, total_rows)
            logger.info(f"  ⏳ Inserted {current}/{total_rows} rows...")

        logger.info(f"✅ All {total_rows} rows inserted into MySQL table '{table_name}'")

    except Error as e:
        logger.error(f"❌ Failed to load data: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description='Load Spotify CSV directly to MySQL')
    parser.add_argument('--csv', default='spotify_clean.csv', help='Path to CSV file (default: spotify_clean.csv)')
    parser.add_argument('--host', '-H', default='localhost', help='MySQL host (default: localhost)')
    parser.add_argument('--user', '-u', default='root', help='MySQL user (default: root)')
    parser.add_argument('--password', '-p', default='', help='MySQL password (default: empty)')
    parser.add_argument('--db', '-d', default='spotify', help='MySQL database name (default: spotify)')
    parser.add_argument('--table', '-t', default='tracks', help='Table name in MySQL (default: tracks)')

    args = parser.parse_args(argv)

    try:
        # Read CSV
        logger.info(f"📁 Loading CSV from {args.csv}...")
        df = read_csv(args.csv)

        # Connect to MySQL
        conn = connect_mysql(args.host, args.user, args.password)

        # Create database
        create_mysql_database(conn, args.db)

        # Load data
        logger.info(f"🔄 Loading data into MySQL table '{args.table}'...")
        load_csv_to_mysql(df, conn, args.table)

        logger.info("✅ CSV to MySQL migration complete!")
        conn.close()

    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
