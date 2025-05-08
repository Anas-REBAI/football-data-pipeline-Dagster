SELECT
  match_id,
  team_id,
  COUNT(*) AS total_shots,
  SUM(CASE WHEN shot_outcome = 'Goal' THEN 1 ELSE 0 END) AS goals,
  ROUND(SUM(CASE WHEN shot_outcome = 'Goal' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) AS conversion_rate
FROM {{ ref('stg_shots') }}
GROUP BY match_id, team_id