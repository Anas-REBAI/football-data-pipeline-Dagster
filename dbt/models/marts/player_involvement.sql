SELECT
  match_id,
  player_id,
  COUNT(*) AS total_touches,
  COUNT(*) FILTER (WHERE event_type = 'Pass' AND pass_shot_assist = TRUE) AS key_passes
FROM {{ ref('stg_events') }}
GROUP BY match_id, player_id