USE SPOTIFY;
select *
from tracks;
-- Top 10 most popular tracks
SELECT track_name, artists, popularity
FROM tracks
ORDER BY popularity DESC
LIMIT 10;

-- Number of tracks per genre
SELECT track_genre, COUNT(*) AS total_tracks
FROM tracks
GROUP BY track_genre
ORDER BY total_tracks DESC;

-- Average popularity per genre
SELECT track_genre, ROUND(AVG(popularity), 2) AS avg_popularity
FROM tracks
GROUP BY track_genre
ORDER BY avg_popularity DESC;

-- Rank songs within each genre
SELECT
    track_genre,
    track_name,
    artists,
    popularity,
    RANK() OVER (
        PARTITION BY track_genre 
        ORDER BY popularity DESC
    ) AS genre_rank
FROM tracks;

     

