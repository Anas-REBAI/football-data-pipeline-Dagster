SELECT
  match_id,
  team_id,
  COUNT(*) AS total_passes,
  SUM(CASE WHEN pass_outcome = 'complete' THEN 1 ELSE 0 END) AS completed_passes,
  ROUND(SUM(CASE WHEN pass_outcome = 'complete' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) AS passing_accuracy
FROM {{ ref('stg_passes') }}
GROUP BY match_id, team_id
