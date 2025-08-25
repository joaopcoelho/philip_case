-- 1) Table
CREATE OR REPLACE TABLE `philip_case.streaming_events` (
  user_id STRING,
  timestamp TIMESTAMP,
  event STRING,
  platform STRING,
  version STRING,
  metadata STRING
);

-- 2) Insert realistic sample data
INSERT INTO `philip_case.streaming_events` (user_id, timestamp, event, platform, version, metadata)
WITH
dates AS (
  SELECT d
  FROM UNNEST(GENERATE_DATE_ARRAY('2025-02-01','2025-02-14')) AS d
),
users AS (
  SELECT * FROM UNNEST([
    STRUCT('u123' AS user_id, 'iOS' AS default_platform),
    STRUCT('u456' AS user_id, 'Android'),
    STRUCT('u789' AS user_id, 'Web'),
    STRUCT('u321' AS user_id, 'iOS'),
    STRUCT('u654' AS user_id, 'Android'),
    STRUCT('u987' AS user_id, 'Web'),
    STRUCT('u222' AS user_id, 'iOS'),
    STRUCT('u333' AS user_id, 'Android'),
    STRUCT('u444' AS user_id, 'Web')
  ])
),
catalog AS (
  SELECT * FROM UNNEST([
    STRUCT('875' AS songId, 'Radiohead'      AS artist, 270 AS duration_sec),
    STRUCT('932' AS songId, 'Billie Eilish'  AS artist, 188 AS duration_sec),
    STRUCT('777' AS songId, 'Coldplay'       AS artist, 236 AS duration_sec),
    STRUCT('888' AS songId, 'Adele'          AS artist, 225 AS duration_sec),
    STRUCT('555' AS songId, 'Bad Bunny'      AS artist, 192 AS duration_sec),
    STRUCT('444' AS songId, 'Taylor Swift'   AS artist, 210 AS duration_sec),
    STRUCT('333' AS songId, 'Drake'          AS artist, 204 AS duration_sec),
    STRUCT('222' AS songId, 'Dua Lipa'       AS artist, 201 AS duration_sec),
    STRUCT('111' AS songId, 'The Weeknd'     AS artist, 198 AS duration_sec),
    STRUCT('999' AS songId, 'Olivia Rodrigo' AS artist, 200 AS duration_sec)
  ])
),
sessions AS (
  SELECT
    CONCAT(u.user_id, '-', FORMAT_DATE('%Y%m%d', d.d), '-', s_idx) AS session_id,
    u.user_id,
    IF(RAND() < 0.8, u.default_platform,
       (SELECT p FROM UNNEST(['iOS','Android','Web']) AS p ORDER BY RAND() LIMIT 1)) AS platform,
    d.d AS session_date,
    TIMESTAMP_ADD(TIMESTAMP(d.d),
      INTERVAL CAST( (7 + CAST(FLOOR(RAND()*16) AS INT64)) * 3600
                   + CAST(FLOOR(RAND()*3600) AS INT64) AS INT64) SECOND) AS session_start,
    CASE
      WHEN (IF(RAND()<0.8, u.default_platform, 'Other')) = 'iOS'
        THEN CASE WHEN RAND()<0.65 THEN '3.12' WHEN RAND()<0.9 THEN '3.11' ELSE '3.10' END
      WHEN (IF(RAND()<0.8, u.default_platform, 'Other')) = 'Android'
        THEN CASE WHEN RAND()<0.6 THEN '3.10' WHEN RAND()<0.9 THEN '3.11' ELSE '3.9' END
      ELSE CASE WHEN RAND()<0.7 THEN '4.1' WHEN RAND()<0.95 THEN '4.0' ELSE '3.9' END
    END AS version
  FROM dates d
  JOIN users u
  ON TRUE
  JOIN UNNEST(GENERATE_ARRAY(1, 2)) AS s_idx
  WHERE RAND() < 0.60
),
plays AS (
  SELECT
    s.*,
    play_index
  FROM sessions s
  JOIN UNNEST(GENERATE_ARRAY(1, 1 + CAST(FLOOR(RAND()*6) AS INT64))) AS play_index
),
plays_with_song AS (
  SELECT
    p.session_id, p.user_id, p.platform, p.version, p.session_date, p.session_start, p.play_index,
    (SELECT AS STRUCT c.* FROM catalog c ORDER BY RAND() LIMIT 1) AS song,
    TIMESTAMP_ADD(
      p.session_start,
      INTERVAL ( (p.play_index - 1) * (2 + CAST(FLOOR(RAND()*4) AS INT64))
               + CAST(FLOOR(RAND()*2) AS INT64) ) MINUTE
    ) AS play_start
  FROM plays p
),
song_events AS (
  -- song_started
  SELECT
    user_id,
    play_start AS timestamp,
    'song_started' AS event,
    platform,
    version,
    CAST(FORMAT('songId=%s,artist=%s', song.songId, song.artist) AS STRING) AS metadata
  FROM plays_with_song

  UNION ALL
  -- optional pause
  SELECT
    user_id,
    TIMESTAMP_ADD(play_start, INTERVAL CAST(20 + CAST(FLOOR(RAND()*80) AS INT64) AS INT64) SECOND) AS timestamp,
    'song_paused' AS event,
    platform,
    version,
    CAST(FORMAT('songId=%s,position=%d', song.songId, 15 + CAST(FLOOR(RAND()*120) AS INT64)) AS STRING) AS metadata
  FROM plays_with_song
  WHERE RAND() < 0.25

  UNION ALL
  -- optional resume
  SELECT
    user_id,
    TIMESTAMP_ADD(play_start, INTERVAL CAST(40 + CAST(FLOOR(RAND()*140) AS INT64) AS INT64) SECOND) AS timestamp,
    'song_resumed' AS event,
    platform,
    version,
    CAST(FORMAT('songId=%s,position=%d', song.songId, 15 + CAST(FLOOR(RAND()*120) AS INT64)) AS STRING) AS metadata
  FROM plays_with_song
  WHERE RAND() < 0.22

  UNION ALL
  -- optional like
  SELECT
    user_id,
    TIMESTAMP_ADD(play_start, INTERVAL CAST(30 + CAST(FLOOR(RAND()*120) AS INT64) AS INT64) SECOND) AS timestamp,
    'song_liked' AS event,
    platform,
    version,
    CAST(FORMAT('songId=%s', song.songId) AS STRING) AS metadata
  FROM plays_with_song
  WHERE RAND() < 0.15

  UNION ALL
  -- skip (~30%)
  SELECT
    user_id,
    TIMESTAMP_ADD(play_start, INTERVAL CAST(60 + CAST(FLOOR(RAND()*120) AS INT64) AS INT64) SECOND) AS timestamp,
    'song_skipped' AS event,
    platform,
    version,
    CAST(FORMAT('songId=%s,position=%d', song.songId, 30 + CAST(FLOOR(RAND()*150) AS INT64)) AS STRING) AS metadata
  FROM plays_with_song
  WHERE RAND() < 0.30

  UNION ALL
  -- ended (~65%)
  SELECT
    user_id,
    TIMESTAMP_ADD(play_start, INTERVAL CAST(song.duration_sec + CAST(FLOOR(RAND()*20) AS INT64) AS INT64) SECOND) AS timestamp,
    'song_ended' AS event,
    platform,
    version,
    CAST(FORMAT('songId=%s,duration=%d', song.songId, song.duration_sec) AS STRING) AS metadata
  FROM plays_with_song
  WHERE RAND() < 0.65
),
session_edges AS (
  -- session_started
  SELECT
    user_id,
    session_start AS timestamp,
    'session_started' AS event,
    platform,
    version,
    CAST(NULL AS STRING) AS metadata
  FROM sessions

  UNION ALL
  -- session_ended (after last event or a few minutes later)
  SELECT
    s.user_id,
    TIMESTAMP_ADD(
      COALESCE(MAX(e.timestamp) OVER (PARTITION BY s.session_id), s.session_start),
      INTERVAL CAST(2 + CAST(FLOOR(RAND()*6) AS INT64) AS INT64) MINUTE
    ) AS timestamp,
    'session_ended' AS event,
    s.platform,
    s.version,
    CAST(NULL AS STRING) AS metadata
  FROM sessions s
  LEFT JOIN song_events e
    ON e.user_id = s.user_id
   AND e.timestamp >= s.session_start
   AND e.timestamp < TIMESTAMP_ADD(s.session_start, INTERVAL 12 HOUR)

  UNION ALL
  -- rare crash
  SELECT
    s.user_id,
    TIMESTAMP_ADD(s.session_start, INTERVAL CAST(1 + CAST(FLOOR(RAND()*20) AS INT64) AS INT64) MINUTE) AS timestamp,
    'app_crash' AS event,
    s.platform,
    s.version,
    CAST('err=SIGSEGV,screen=player' AS STRING) AS metadata
  FROM sessions s
  WHERE RAND() < 0.015
)
SELECT * FROM session_edges
UNION ALL
SELECT * FROM song_events;
