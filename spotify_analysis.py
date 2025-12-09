"""
spotify_analysis.py

Standalone, modular script converted from the Jupyter notebook.
Usage:
    python spotify_analysis.py --path spotify_clean.csv

Functions:
- resolve_data_path
- load_and_inspect
- clean_data
- plotting helpers (popularity distribution, top genres, top artists,
  energy vs danceability scatter, avg popularity by genre)
- run_analysis

The script is intended for interactive use; it shows plots by default but can
save figures to an output directory if `--outdir` is provided.
"""

from pathlib import Path
import argparse
import logging
import re
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Plot style
sns.set(style="whitegrid")
plt.rcParams.update({
    'figure.figsize': (10, 6),
    'figure.dpi': 110,
    'axes.titlesize': 14,
    'axes.labelsize': 12
})


def resolve_data_path(candidate: str = 'spotify_clean.csv') -> Path:
    """Resolve a path for the dataset. Search current directory for spotify CSVs as fallback."""
    p = Path(candidate)
    if p.exists():
        return p
    # fallback search for files containing 'spotify' in name
    for f in Path('.').glob('*spotify*.csv'):
        logger.info('Found dataset fallback: %s', f)
        return f
    raise FileNotFoundError(f"Could not locate dataset. Tried {candidate} and workspace for *spotify*.csv")


def load_and_inspect(path: str = 'spotify_clean.csv') -> pd.DataFrame:
    """Load CSV into DataFrame and print basic inspection outputs."""
    p = resolve_data_path(path)
    df = pd.read_csv(p)
    logger.info('Data loaded from: %s', p)

    # Basic inspection
    print('\nFirst 5 rows:')
    print(df.head().to_string(index=False))

    print('\nShape:', df.shape)
    print('\nColumns:')
    print(list(df.columns))

    print('\nSummary statistics (numeric):')
    print(df.describe().to_string())

    print('\nMissing values by column:')
    print(df.isnull().sum().to_string())

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Perform idempotent cleaning of the Spotify dataset.

    Steps:
    - Drop 'Unnamed: 0' if present
    - Fill missing artists/album_name/track_name with 'Unknown'
    - Coerce numeric-like columns to numeric and fill NaNs with medians
    - Drop duplicates by 'track_id' if available
    - Ensure audio feature columns are numeric
    """
    df = df.copy()

    # 1) Drop 'Unnamed: 0' if present
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])
        logger.info("Dropped 'Unnamed: 0' column")

    # 2) Fill missing for text columns
    for col in ['artists', 'album_name', 'track_name']:
        if col in df.columns:
            df[col] = df[col].fillna('Unknown')

    # 3) Identify numeric candidate columns
    numeric_candidates = [
        'danceability', 'energy', 'loudness', 'speechiness', 'acousticness',
        'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms',
        'key', 'mode', 'time_signature', 'popularity'
    ]

    # Heuristic: any object column that converts well for a sample is numeric-like
    for col in df.columns:
        if col in numeric_candidates:
            continue
        if df[col].dtype == 'object':
            sample = df[col].dropna().astype(str).head(20)
            if len(sample) == 0:
                continue
            coerced = pd.to_numeric(sample, errors='coerce')
            if coerced.notna().sum() >= max(1, len(sample) // 2):
                numeric_candidates.append(col)

    # Deduplicate list
    numeric_candidates = list(dict.fromkeys(numeric_candidates))

    # Coerce to numeric and fill NaNs with median
    for col in numeric_candidates:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            median = df[col].median()
            if pd.notna(median):
                df[col] = df[col].fillna(median)

    # 4) Remove duplicate tracks by track_id if available
    if 'track_id' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['track_id'])
        after = len(df)
        logger.info('Removed %d duplicate rows based on track_id', (before - after))
    else:
        before = len(df)
        df = df.drop_duplicates()
        after = len(df)
        logger.info('Removed %d exact duplicate rows', (before - after))

    # 5) Ensure audio features are numeric
    audio_features = ['danceability', 'energy', 'valence', 'loudness', 'speechiness',
                      'acousticness', 'instrumentalness', 'liveness']
    for col in audio_features:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if df[col].isnull().any():
                df[col] = df[col].fillna(df[col].median())

    return df


# --- Plotting helpers ---

def plot_popularity_distribution(df: pd.DataFrame, column: str = 'popularity', outpath: Path = None):
    if column not in df.columns:
        logger.warning('Column %s not found; skipping popularity distribution', column)
        return
    plt.figure()
    sns.histplot(df[column], bins=30, kde=True, color='tab:blue')
    plt.title('Distribution of Song Popularity')
    plt.xlabel('Popularity')
    plt.ylabel('Count')
    plt.tight_layout()
    if outpath:
        plt.savefig(outpath / 'popularity_distribution.png')
        logger.info('Saved popularity distribution to %s', outpath / 'popularity_distribution.png')
    plt.show()


def plot_top_genres(df: pd.DataFrame, genre_column_candidates=('genres', 'genre', 'primary_genre'), top_n: int = 10, outpath: Path = None):
    col = None
    # include 'track_genre' as a common column name in this dataset
    candidates = list(genre_column_candidates) + ['track_genre']
    for c in candidates:
        if c in df.columns:
            col = c
            break
    if col is None:
        logger.warning('No genre column found (tried %s); skipping top genres plot', genre_column_candidates)
        return

    # Split multi-genre strings using common separators
    genres_series = df[col].dropna().astype(str).str.split(r'[;|,/]')
    exploded = genres_series.explode().str.strip().replace('', np.nan).dropna()
    top = exploded.value_counts().nlargest(top_n)

    plt.figure()
    sns.barplot(x=top.values, y=top.index, palette='viridis')
    plt.title(f'Top {top_n} Genres by Number of Tracks')
    plt.xlabel('Number of Tracks')
    plt.ylabel('Genre')
    plt.tight_layout()
    if outpath:
        plt.savefig(outpath / 'top_genres.png')
        logger.info('Saved top genres to %s', outpath / 'top_genres.png')
    plt.show()


def plot_top_artists(df: pd.DataFrame, artist_col: str = 'artists', top_n: int = 10, outpath: Path = None):
    if artist_col not in df.columns:
        logger.warning('Artist column %s not found; skipping top artists plot', artist_col)
        return
    primary = df[artist_col].fillna('Unknown').astype(str).str.split(',').str[0].str.strip()
    top = primary.value_counts().nlargest(top_n)

    plt.figure()
    sns.barplot(x=top.values, y=top.index, palette='magma')
    plt.title(f'Top {top_n} Artists by Track Count')
    plt.xlabel('Track Count')
    plt.ylabel('Artist')
    plt.tight_layout()
    if outpath:
        plt.savefig(outpath / 'top_artists.png')
        logger.info('Saved top artists to %s', outpath / 'top_artists.png')
    plt.show()


def plot_energy_vs_danceability(df: pd.DataFrame, x: str = 'danceability', y: str = 'energy', outpath: Path = None):
    if x not in df.columns or y not in df.columns:
        logger.warning('Columns %s and/or %s not found; skipping scatter plot', x, y)
        return
    plt.figure()
    sns.scatterplot(data=df, x=x, y=y, alpha=0.5, s=30)
    plt.title('Energy vs Danceability')
    plt.xlabel('Danceability')
    plt.ylabel('Energy')
    plt.tight_layout()
    if outpath:
        plt.savefig(outpath / 'energy_vs_danceability.png')
        logger.info('Saved energy vs danceability to %s', outpath / 'energy_vs_danceability.png')
    plt.show()


def plot_avg_popularity_by_genre(df: pd.DataFrame, genre_column_candidates=('genres', 'genre', 'primary_genre'), top_n: int = 15, outpath: Path = None):
    col = None
    # include 'track_genre' as a candidate column
    candidates = list(genre_column_candidates) + ['track_genre']
    for c in candidates:
        if c in df.columns:
            col = c
            break
    if col is None or 'popularity' not in df.columns:
        logger.warning('Required genre or popularity column not found; skipping avg popularity by genre')
        return

    genres_series = df[[col, 'popularity']].dropna(subset=[col])
    genres_series = genres_series.assign(_genres=genres_series[col].astype(str).str.split(r'[;|,/]')).explode('_genres')
    genres_series['_genres'] = genres_series['_genres'].str.strip().replace('', np.nan).dropna()
    grouped = genres_series.groupby('_genres')['popularity'].mean().nlargest(top_n)

    plt.figure()
    sns.barplot(x=grouped.values, y=grouped.index, palette='coolwarm')
    plt.title(f'Average Popularity by Genre (Top {top_n})')
    plt.xlabel('Average Popularity')
    plt.ylabel('Genre')
    plt.tight_layout()
    if outpath:
        plt.savefig(outpath / 'avg_popularity_by_genre.png')
        logger.info('Saved avg popularity by genre to %s', outpath / 'avg_popularity_by_genre.png')
    plt.show()


def validate_cleaned_data(df: pd.DataFrame) -> dict:
    """Validate data quality after cleaning. Returns a quality report."""
    report = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'missing_values': df.isnull().sum().to_dict(),
        'duplicates': len(df[df.duplicated()]),
        'duplicate_tracks': 0,
        'column_types': df.dtypes.to_dict(),
        'issues': []
    }
    
    # Check for duplicate track_ids
    if 'track_id' in df.columns:
        report['duplicate_tracks'] = df['track_id'].duplicated().sum()
        if report['duplicate_tracks'] > 0:
            report['issues'].append(f"⚠️  Found {report['duplicate_tracks']} duplicate track_ids")
    
    # Check for missing values in critical columns
    critical_cols = ['track_id', 'artists', 'track_name', 'popularity']
    for col in critical_cols:
        if col in df.columns:
            missing = df[col].isnull().sum()
            if missing > 0:
                report['issues'].append(f"⚠️  Column '{col}' has {missing} missing values")
    
    # Check audio feature columns are numeric
    audio_features = ['danceability', 'energy', 'valence', 'loudness', 'speechiness', 
                      'acousticness', 'instrumentalness', 'liveness', 'tempo']
    non_numeric_audio = []
    for col in audio_features:
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            non_numeric_audio.append(col)
    
    if non_numeric_audio:
        report['issues'].append(f"⚠️  Audio features not numeric: {non_numeric_audio}")
    
    # Check for 'Unnamed: 0' column (should be dropped)
    if 'Unnamed: 0' in df.columns:
        report['issues'].append("⚠️  'Unnamed: 0' column still present (should be dropped)")
    
    # All checks passed
    if not report['issues']:
        report['issues'].append("✅ Data quality checks passed")
    
    return report


def print_quality_report(report: dict):
    """Pretty-print data quality report."""
    print("\n" + "="*60)
    print("📊 DATA QUALITY REPORT")
    print("="*60)
    print(f"Total rows: {report['total_rows']}")
    print(f"Total columns: {report['total_columns']}")
    print(f"Duplicate rows: {report['duplicates']}")
    print(f"Duplicate track_ids: {report['duplicate_tracks']}")
    
    missing_count = sum(report['missing_values'].values())
    print(f"\nMissing values (total): {missing_count}")
    if missing_count > 0:
        print("  Breakdown:")
        for col, count in sorted(report['missing_values'].items(), key=lambda x: x[1], reverse=True):
            if count > 0:
                print(f"    {col}: {count}")
    
    print("\n📋 Issues & Warnings:")
    for issue in report['issues']:
        print(f"  {issue}")
    
    print("="*60 + "\n")


def run_analysis(data_path: str = 'spotify_clean.csv', outdir: str = None) -> pd.DataFrame:
    """Load, clean, and run EDA plotting functions. Returns cleaned DataFrame."""
    # Resolve actual input path so we can save cleaned CSV beside it
    input_path = resolve_data_path(data_path)
    df = load_and_inspect(str(input_path))

    logger.info('Cleaning data...')
    df_clean = clean_data(df)
    logger.info('Data cleaned. Shape after cleaning: %s', df_clean.shape)

    # Validate cleaned data quality
    quality_report = validate_cleaned_data(df_clean)
    print_quality_report(quality_report)

    # Save cleaned dataframe as `spotify_clean.csv` next to the input file
    cleaned_path = input_path.parent / 'spotify_clean.csv'
    df_clean.to_csv(cleaned_path, index=False)
    logger.info('Saved cleaned dataset to: %s', cleaned_path)

    # Only save plots when an output directory is provided; otherwise just show them
    outpath = None
    if outdir:
        outpath = Path(outdir)
        outpath.mkdir(parents=True, exist_ok=True)
        logger.info('Output directory for plots: %s', outpath)

    # Plots (these will display; saved only if outpath is not None)
    plot_popularity_distribution(df_clean, outpath=outpath)
    plot_top_genres(df_clean, top_n=10, outpath=outpath)
    plot_top_artists(df_clean, top_n=10, outpath=outpath)
    plot_energy_vs_danceability(df_clean, outpath=outpath)
    plot_avg_popularity_by_genre(df_clean, top_n=15, outpath=outpath)

    logger.info('All plots processed (saved only if --outdir provided).')
    return df_clean


def main(argv=None):
    parser = argparse.ArgumentParser(description='Spotify dataset cleaning and EDA')
    parser.add_argument('--path', '-p', default='spotify_clean.csv', help='Path to spotify CSV')
    parser.add_argument('--outdir', '-o', default=None, help='Directory to save plots (optional)')
    args = parser.parse_args(argv)

    try:
        run_analysis(data_path=args.path, outdir=args.outdir)
    except FileNotFoundError as e:
        logger.error(e)
        sys.exit(2)


if __name__ == '__main__':
    main()
