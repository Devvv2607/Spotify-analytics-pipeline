import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import altair as alt

st.set_page_config(page_title="Spotify Analytics", layout="wide")
st.title("Spotify Tracks Analytics Dashboard")

# Connect to DB safely for Streamlit
# Use cache_resource to store a connection factory; open connections with
# check_same_thread=False so Streamlit threads can use it. We also avoid
# caching a live connection object that might be closed across runs.
@st.cache_resource
def get_connection_factory(db_path: str = "spotify_tracks.db"):
    def _connect():
        return sqlite3.connect(db_path, check_same_thread=False)
    return _connect

conn_factory = get_connection_factory()
def get_connection():
    # Return a fresh connection for each use (safe in Streamlit)
    return conn_factory()


@st.cache_data(show_spinner=False)
def load_full_dataframe(db_path: str = "spotify_tracks.db") -> pd.DataFrame:
    """Load full `tracks` table from SQLite and cache the DataFrame."""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM tracks", conn)
    conn.close()

    # Basic dtype fixes
    if 'explicit' in df.columns:
        try:
            df['explicit'] = df['explicit'].astype(bool)
        except Exception:
            pass

    audio_cols = ['danceability','energy','loudness','speechiness','acousticness',
                  'instrumentalness','liveness','valence','tempo','popularity']
    for c in audio_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    return df


def ensure_sqlite_db(db_path: str = "spotify_tracks.db", csv_path: str = "spotify_clean.csv"):
    """Ensure the SQLite DB exists and contains a `tracks` table. If not, attempt to
    create it from a cleaned CSV file. Returns True if table is available.
    """
    import os
    conn = None
    try:
        if not os.path.exists(db_path):
            # Try to create DB from CSV
            if os.path.exists(csv_path):
                st.info(f"Creating SQLite DB '{db_path}' from '{csv_path}'...")
                df = pd.read_csv(csv_path)
                conn = sqlite3.connect(db_path)
                df.to_sql('tracks', conn, if_exists='replace', index=False)
                conn.close()
                st.success("Created SQLite DB and 'tracks' table from CSV.")
                return True
            else:
                st.error(f"Neither '{db_path}' nor '{csv_path}' found. Please run the cleaning script first.")
                return False
        # If DB exists, check for table
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tracks'")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return True
        # Table missing: try to create from CSV
        if os.path.exists(csv_path):
            st.info(f"Table 'tracks' missing in '{db_path}'. Creating from '{csv_path}'...")
            df = pd.read_csv(csv_path)
            conn = sqlite3.connect(db_path)
            df.to_sql('tracks', conn, if_exists='replace', index=False)
            conn.close()
            st.success("Created 'tracks' table in SQLite DB from CSV.")
            return True
        st.error(f"Table 'tracks' missing in '{db_path}' and '{csv_path}' not found.")
        return False
    except Exception as e:
        if conn:
            conn.close()
        st.error(f"Error while ensuring SQLite DB: {e}")
        return False

# Sidebar Filters and interactive dashboard
st.sidebar.header("Filters")

# Ensure SQLite DB and table exist (create from CSV if necessary)
if not ensure_sqlite_db():
    st.stop()

# Load full dataframe once (cached)
df_all = load_full_dataframe()

# Sidebar controls
st.sidebar.subheader("Filter dataset")
all_genres = sorted(df_all['track_genre'].dropna().astype(str).unique().tolist())
genre_list = ["All"] + all_genres
selected_genre = st.sidebar.selectbox("Select Genre", genre_list)

min_pop, max_pop = int(df_all['popularity'].min()), int(df_all['popularity'].max())
pop_range = st.sidebar.slider("Popularity range", min_pop, max_pop, (20, max_pop))

artist_search = st.sidebar.text_input("Artist contains")
top_n = st.sidebar.slider("Top N for leaderboards", 5, 50, 10)
show_corr = st.sidebar.checkbox("Show audio features correlation", value=True)

# Apply filters locally
df_filtered = df_all.copy()
if selected_genre != "All":
    df_filtered = df_filtered[df_filtered['track_genre'] == selected_genre]
if artist_search:
    df_filtered = df_filtered[df_filtered['artists'].str.contains(artist_search, case=False, na=False)]
df_filtered = df_filtered[(df_filtered['popularity'] >= pop_range[0]) & (df_filtered['popularity'] <= pop_range[1])]

# KPIs
k1, k2, k3 = st.columns(3)
k1.metric("Tracks (filtered)", f"{len(df_filtered):,}")
k2.metric("Unique artists", f"{df_filtered['artists'].nunique():,}")
k3.metric("Avg popularity", f"{df_filtered['popularity'].mean():.1f}")

st.markdown("---")

# Two-column layout: main charts + leaderboards
left, right = st.columns((2, 1))

with left:
    st.subheader("Popularity distribution")
    bins = st.slider("Histogram bins", 10, 100, 30, key='hist_bins')
    hist = alt.Chart(df_filtered).mark_bar().encode(
        alt.X('popularity:Q', bin=alt.Bin(maxbins=bins)),
        y='count()'
    ).properties(height=250)
    st.altair_chart(hist, use_container_width=True)

    st.subheader("Energy vs Danceability")
    scatter = alt.Chart(df_filtered).mark_circle(size=60).encode(
        x='danceability:Q',
        y='energy:Q',
        color=alt.Color('valence:Q', scale=alt.Scale(scheme='viridis')),
        tooltip=['track_name', 'artists', 'popularity', 'track_genre']
    ).interactive()
    brush = alt.selection_interval()
    scatter = scatter.add_selection(brush)
    st.altair_chart(scatter, use_container_width=True)

with right:
    st.subheader(f"Top {top_n} genres")
    top_genres = df_filtered['track_genre'].value_counts().nlargest(top_n).reset_index()
    top_genres.columns = ['track_genre', 'count']
    gbar = alt.Chart(top_genres).mark_bar().encode(x='count:Q', y=alt.Y('track_genre:N', sort='-x'), tooltip=['track_genre','count'])
    st.altair_chart(gbar, use_container_width=True)

    st.subheader(f"Top {top_n} artists")
    primary_artist = df_filtered['artists'].fillna('Unknown').astype(str).str.split(',').str[0].str.strip()
    top_art = primary_artist.value_counts().nlargest(top_n).reset_index()
    top_art.columns = ['artist','count']
    ab = alt.Chart(top_art).mark_bar().encode(x='count:Q', y=alt.Y('artist:N', sort='-x'), tooltip=['artist','count'])
    st.altair_chart(ab, use_container_width=True)

st.markdown("---")

st.subheader("Filtered tracks (sample)")
show_cols = st.multiselect('Columns to show', df_filtered.columns.tolist(), default=['track_name','artists','track_genre','popularity','danceability','energy'])
st.dataframe(df_filtered[show_cols].head(500))

if st.button('Download filtered CSV'):
    csv_bytes = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button('Download CSV', data=csv_bytes, file_name='spotify_filtered.csv', mime='text/csv')

st.subheader('Summary statistics')
st.write(df_filtered.describe())

if show_corr:
    st.subheader('Audio features correlation')
    audio_cols = ['danceability','energy','valence','loudness','speechiness','acousticness','instrumentalness','liveness','tempo']
    present_audio = [c for c in audio_cols if c in df_filtered.columns]
    if present_audio:
        corr = df_filtered[present_audio].corr()
        corr = corr.reset_index().melt(id_vars='index')
        corr.columns = ['x','y','value']
        hm = alt.Chart(corr).mark_rect().encode(
            x=alt.X('x:N', sort=present_audio),
            y=alt.Y('y:N', sort=present_audio),
            color='value:Q',
            tooltip=['x','y','value']
        ).properties(height=400)
        st.altair_chart(hm, use_container_width=True)
    else:
        st.write('No audio feature columns available for correlation.')

st.info('Use the sidebar controls to refine the dataset and brush the scatter to explore points interactively.')
