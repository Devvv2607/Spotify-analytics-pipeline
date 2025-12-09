# Spotify Analytics ETL Pipeline

A comprehensive data pipeline for extracting, transforming, and analyzing Spotify track data with support for SQLite and MySQL databases, interactive dashboards, and advanced analytics.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Guide](#usage-guide)
- [Database Setup](#database-setup)
- [SQL Queries](#sql-queries)
- [Project Files](#project-files)
- [Requirements](#requirements)

---

## 🎯 Overview

This project provides an end-to-end ETL pipeline for Spotify data, including:
- **Data Cleaning & Transformation**: Process raw Spotify datasets
- **Database Support**: SQLite for local development, MySQL for production
- **Interactive Dashboard**: Streamlit-based analytics interface
- **Advanced Analytics**: Python scripts for exploratory data analysis
- **SQL Analytics**: Pre-built queries for insights and reporting

---

## 📁 Project Structure

```
.
├── app.py                          # Streamlit interactive dashboard
├── spotify_analysis.py             # Standalone analysis script
├── csv_to_mysql.py                # Direct CSV to MySQL loader
├── spotify sql.sql                # SQL queries and analytics
├── dataset.csv                    # Raw Spotify dataset
├── spotify_clean.csv              # Cleaned dataset (processed)
├── spotify_tracks.db              # SQLite database
├── requirements.txt               # Python dependencies
├── plots/                         # Generated visualization outputs
├── README.md                      # This file
└── README_MYSQL_MIGRATION.md      # MySQL migration guide
```

---

## ✨ Features

### Data Processing
- ✅ CSV data cleaning and validation
- ✅ Automatic SQLite database creation
- ✅ MySQL migration support
- ✅ Data type conversion and handling

### Analytics & Visualization
- 📊 Interactive Streamlit dashboard
- 📈 Exploratory data analysis (EDA)
- 🎨 Multiple chart types (bar, scatter, distribution)
- 📁 Batch plot generation and export

### Database Support
- 🗄️ MySQL for production
- 🔄 Easy migration between databases
- 📋 Pre-built SQL queries

### Analysis Features
- Top 10 most popular tracks ranking
- Genre-based track distribution
- Average popularity by genre
- Genre-specific track ranking with window functions
- Energy vs. Danceability correlation
- Popularity distribution analysis

---

## 🚀 Installation

### Prerequisites
- Python 3.8+
- pip package manager
- (Optional) MySQL server for database migration

### Step 1: Clone or Download Project
```powershell
cd "C:\Users\Dev\OneDrive\Desktop\SPOTIFY ETL PIPELINE"
```

### Step 2: Create Virtual Environment (Recommended)
```powershell
python -m venv venv
venv\Scripts\Activate.ps1
```

### Step 3: Install Dependencies
```powershell
pip install -r requirements.txt
```

---

## ⚡ Quick Start

### 1. Create SQLite Database
```powershell
python create_sql_db.py
```
This will create `spotify_tracks.db` from `spotify_clean.csv`.

### 2. Launch Interactive Dashboard
```powershell
streamlit run app.py
```
Opens browser at `http://localhost:8501` with interactive analytics.

### 3. Run Exploratory Analysis
```powershell
python spotify_analysis.py --path spotify_clean.csv
```
Generates plots and statistical summaries.

---

## 📖 Usage Guide

### Option A: Streamlit Dashboard (Recommended)
Best for interactive exploration and real-time insights.

```powershell
streamlit run app.py
```

**Features:**
- Browse all tracks with filtering
- Top N tracks by popularity
- Genre distribution analysis
- Audio feature correlations
- Interactive charts and metrics

### Option B: Command-Line Analysis
For batch processing and generating static reports.

```powershell
python spotify_analysis.py --path spotify_clean.csv --outdir ./plots
```

**Arguments:**
- `--path`: Path to CSV file (default: `spotify_clean.csv`)
- `--outdir`: Save plots to directory (optional)
- `--show`: Display plots in browser (default: True)

### Option C: Python API
Use in scripts or Jupyter notebooks:

```python
from spotify_analysis import run_analysis

# Run full analysis pipeline
df_final = run_analysis('spotify_clean.csv')
print(df_final.head())
```

---

## 🗄️ Database Setup



### MySQL (Production)

#### Option 1: Direct CSV to MySQL
```powershell
python csv_to_mysql.py `
  --host localhost `
  --user root `
  --password your_password `
  --db spotify
```


**Verify connection:**
```powershell
mysql -u root -p spotify
mysql> SELECT COUNT(*) FROM tracks;
```

See [README_MYSQL_MIGRATION.md](README_MYSQL_MIGRATION.md) for detailed MySQL setup.

---

## 📊 SQL Queries

The `spotify sql.sql` file contains pre-built analytics queries:

### Top 10 Most Popular Tracks
```sql
SELECT track_name, artists, popularity
FROM tracks
ORDER BY popularity DESC
LIMIT 10;
```

### Track Count by Genre
```sql
SELECT track_genre, COUNT(*) AS total_tracks
FROM tracks
GROUP BY track_genre
ORDER BY total_tracks DESC;
```

### Average Popularity by Genre
```sql
SELECT track_genre, ROUND(AVG(popularity), 2) AS avg_popularity
FROM tracks
GROUP BY track_genre
ORDER BY avg_popularity DESC;
```

### Genre Ranking with Window Functions
```sql
SELECT
    track_genre,
    track_name,
    artists,
    popularity,
    RANK() OVER (PARTITION BY track_genre ORDER BY popularity DESC) AS genre_rank
FROM tracks;
```

Run these queries in your MySQL client or SQLite:

**MySQL:**
```powershell
mysql -u root -p spotify < "spotify sql.sql"
```

**SQLite:**
```powershell
sqlite3 spotify_tracks.db < "spotify sql.sql"
```

---

## 📄 Project Files

| File | Purpose |
|------|---------|
| `app.py` | Streamlit dashboard for interactive analytics |
| `spotify_analysis.py` | Standalone EDA script with plotting functions |
| `csv_to_mysql.py` | Loads CSV data directly into MySQL |
| `spotify sql.sql` | SQL queries for analytics and reporting |
| `dataset.csv` | Raw Spotify dataset |
| `spotify_clean.csv` | Cleaned and processed dataset |
| `spotify_tracks.db` | SQLite database with tracks table |
| `requirements.txt` | Python package dependencies |
| `plots/` | Output directory for generated visualizations |

---

## 📦 Requirements

### Python Packages
```
pandas>=1.2
numpy>=1.18
matplotlib>=3.0
seaborn>=0.11
ipython
jupyter
mysql-connector-python>=8.0
streamlit
altair
```

Install all at once:
```powershell
pip install -r requirements.txt
```

### System Requirements
- **For SQLite**: Works on Windows, macOS, Linux (no additional setup)
- **For MySQL**: Requires running MySQL server
  - Download: https://www.mysql.com/downloads/
  - Or use XAMPP/MariaDB for easier setup

---

## 🔧 Troubleshooting

### Issue: "sqlite3 module not found"
SQLite comes with Python. If issues persist:
```powershell
pip install --upgrade setuptools
```

### Issue: "MySQL connection refused"
1. Verify MySQL is running: `mysql -u root`
2. Check host/port in command args
3. Ensure user/password are correct

### Issue: "CSV not found"
Ensure `spotify_clean.csv` is in the project root directory.

### Issue: Streamlit not launching
```powershell
pip install --upgrade streamlit
streamlit run app.py --logger.level=debug
```

---

## 📝 License

This project is open source. Feel free to use and modify for your purposes.

---

## 👤 Author

Created as an ETL and analytics pipeline project for Spotify data exploration.

---

**Last Updated:** December 2025
