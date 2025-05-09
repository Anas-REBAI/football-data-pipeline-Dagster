{{ config(materialized='table') }}

with passes as (
    select team_id, count(*) as total_passes, sum(is_completed) as completed_passes
    from {{ ref('int_passes') }}
    group by team_id
),

shots as (
    select team_id, count(*) as total_shots, sum(is_goal) as total_goals
    from {{ ref('int_shots') }}
    group by team_id
),

defense as (
    select team_id, count(*) as total_defensive_actions
    from {{ ref('int_defensive_actions') }}
    group by team_id
),

possession as (
    select team_id, sum(possession_duration_seconds) as total_possession_seconds
    from {{ ref('int_possessions') }}
    group by team_id
),

final as (
    select
        coalesce(passes.team_id, shots.team_id, defense.team_id, possession.team_id) as team_id,
        passes.completed_passes * 1.0 / nullif(passes.total_passes, 0) as pass_accuracy,
        shots.total_goals * 1.0 / nullif(shots.total_shots, 0) as shot_conversion_rate,
        defense.total_defensive_actions,
        possession.total_possession_seconds
    from passes
    full outer join shots using (team_id)
    full outer join defense using (team_id)
    full outer join possession using (team_id)
)

select * from final