SELECT
  match_id,
  team_id,
  COUNT(*) FILTER (WHERE event_type IN ('Tackle', 'Interception')) AS defensive_actions,
  COUNT(*) FILTER (WHERE event_type = 'Tackle') AS tackles,
  COUNT(*) FILTER (WHERE event_type = 'Interception') AS interceptions
FROM {{ ref('stg_events') }}
GROUP BY match_id, team_id