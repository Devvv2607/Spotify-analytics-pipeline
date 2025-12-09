# SQLite to MySQL Migration Guide

## Overview
This guide shows how to migrate your Spotify SQLite database (`spotify_tracks.db`) to MySQL.

## Prerequisites
1. **MySQL Server** must be running (locally or remotely)
2. **Python packages**:
   ```powershell
   pip install mysql-connector-python pandas
   ```

## Quick Start

### 1. Install Dependencies
```powershell
pip install mysql-connector-python
```

### 2. Run Migration Script
```powershell
python migrate_sqlite_to_mysql.py --host localhost --user root --password YOUR_PASSWORD --db spotify
```

### Options
- `--host` — MySQL server hostname (default: `localhost`)
- `--user` — MySQL username (default: `root`)
- `--password` — MySQL password (default: empty)
- `--db` — Database name to create (default: `spotify`)
- `--sqlite-db` — Path to SQLite file (default: `spotify_tracks.db`)
- `--sqlite-table` — Table name in SQLite (default: `tracks`)
- `--mysql-table` — Table name in MySQL (default: `tracks`)

### 3. Verify in MySQL
```powershell
mysql -u root -p spotify
```

Then in MySQL console:
```sql
SELECT COUNT(*) FROM tracks;
SELECT * FROM tracks LIMIT 5;
```

## Example: Using XAMPP/Local MySQL
If you have XAMPP running with MySQL on `localhost:3306`:

```powershell
python migrate_sqlite_to_mysql.py `
  --host localhost `
  --user root `
  --password "" `
  --db spotify
```

## Schema Details
The script automatically creates:
- **VARCHAR columns** for text fields
- **BIGINT columns** for integer fields
- **DOUBLE columns** for floating-point audio features
- **BOOLEAN columns** for explicit flag

## Troubleshooting

### MySQL Connection Refused
- Ensure MySQL is running
- Check hostname/port (default port 3306)
- Verify username and password

### Table Already Exists
- The script will drop the old table and recreate it

### Large Dataset
- For 89k+ rows, insertion may take 1–5 minutes depending on connection speed

## Next Steps
- Use SQL queries directly on MySQL for faster analysis
- Connect to MySQL from Python, Node.js, or other applications
- Set up backups and replication
