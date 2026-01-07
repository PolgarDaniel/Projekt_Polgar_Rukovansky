CREATE OR REPLACE TABLE DIM_DATE AS
SELECT DISTINCT
    TO_DATE(match_datetime) AS date_id,
    TO_DATE(match_datetime) AS full_date,
    DAY(match_datetime) AS day,
    MONTH(match_datetime) AS month,
    YEAR(match_datetime) AS year,
    DAYNAME(match_datetime) AS weekday
FROM FIXTURES_STAGING_TYPED;


CREATE OR REPLACE TABLE DIM_TEAM AS
SELECT
    UUID_STRING() AS team_id,
    team_uuid,
    team_name,
    team_short,
    CURRENT_DATE AS valid_from,
    '9999-12-31' AS valid_to,
    TRUE AS is_current
FROM (
    SELECT HOME_UUID AS team_uuid,
           HOME AS team_name,
           HOME_SHORT AS team_short
    FROM FIXTURES_STAGING_TYPED
    UNION
    SELECT AWAY_UUID,
           AWAY,
           AWAY_SHORT
    FROM FIXTURES_STAGING_TYPED
);

CREATE OR REPLACE TABLE DIM_COMPETITION AS
SELECT DISTINCT
    COMPETITION_UUID AS competition_id,
    COMPETITION AS competition_name,
    COUNTRY,
    REGION
FROM FIXTURES_STAGING_TYPED;

CREATE OR REPLACE TABLE DIM_LOCATION AS
SELECT DISTINCT
    VENUE_UUID AS venue_id,
    VENUE AS venue_name,
    COUNTRY,
    COUNTRY_CODE,
    REGION,
    CURRENT_DATE AS valid_from,
    '9999-12-31' AS valid_to
FROM FIXTURES_STAGING_TYPED;

CREATE OR REPLACE TABLE DIM_SEASON AS
SELECT DISTINCT
    SEASON_UUID AS season_id,
    SEASON AS season_name
FROM FIXTURES_STAGING_TYPED;


CREATE OR REPLACE TABLE FACT_MATCH_RESULTS AS
SELECT
    UUID_STRING() AS match_fact_id,
    d.date_id,
    ht.team_id AS home_team_id,
    at.team_id AS away_team_id,
    c.competition_id,
    v.venue_id,
    s.season_id,

    m.home_score,
    m.away_score,
    (m.home_score - m.away_score) AS goal_difference,
    
    SUM(m.home_score + m.away_score)
        OVER (
            PARTITION BY s.season_id
            ORDER BY m.match_datetime
        ) AS cumulative_goals_season,

    RANK()
        OVER (
            PARTITION BY m.ROUND
            ORDER BY (m.home_score + m.away_score) DESC
        ) AS match_rank_in_round

FROM FIXTURES_STAGING_TYPED m
JOIN DIM_DATE d ON TO_DATE(m.match_datetime) = d.date_id
JOIN DIM_TEAM ht ON m.HOME_UUID = ht.team_uuid AND ht.is_current = TRUE
JOIN DIM_TEAM at ON m.AWAY_UUID = at.team_uuid AND at.is_current = TRUE
JOIN DIM_COMPETITION c ON m.COMPETITION_UUID = c.competition_id
JOIN DIM_LOCATION v ON m.VENUE_UUID = v.venue_id
JOIN DIM_SEASON s ON m.SEASON_UUID = s.season_id;
