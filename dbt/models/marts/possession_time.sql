SELECT
  match_id,
  team_id,
  COUNT(*) * 1.0 AS estimated_possession_seconds
FROM {{ ref('stg_events') }}
WHERE event_type = 'Pass'
GROUP BY match_id, team_id
