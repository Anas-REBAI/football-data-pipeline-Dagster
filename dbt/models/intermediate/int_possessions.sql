{{ config(materialized='view') }}

WITH possessions AS (
    SELECT
        possession,
        possession_team_id AS team_id,
        possession_team_name AS team_name,
        SUM(duration) AS possession_duration_seconds,
        COUNT(*) AS number_of_events
    FROM {{ ref('stg_events') }}
    GROUP BY possession, possession_team_id, possession_team_name
)

SELECT * FROM possessions
