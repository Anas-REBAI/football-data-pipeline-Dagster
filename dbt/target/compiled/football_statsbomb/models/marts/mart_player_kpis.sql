

with touches as (
    select
        team_id,
        type_name,
        count(*) as event_count
    from football_statsbomb_intermediate.int_player_events
    group by team_id, type_name
),

pass_stats as (
    select
        team_id,
        sum(is_completed) as passes_completed,
        count(*) as passes_attempted
    from football_statsbomb_intermediate.int_passes
    group by team_id
),

shot_stats as (
    select
        team_id,
        sum(is_goal) as goals,
        count(*) as shots
    from football_statsbomb_intermediate.int_shots
    group by team_id
),

final as (
    select
        coalesce(touches.team_id, pass_stats.team_id, shot_stats.team_id) as team_id,
        sum(touches.event_count) as total_touches,
        pass_stats.passes_completed * 1.0 / nullif(pass_stats.passes_attempted, 0) as pass_accuracy,
        shot_stats.goals * 1.0 / nullif(shot_stats.shots, 0) as shot_conversion
    from touches
    full outer join pass_stats using (team_id)
    full outer join shot_stats using (team_id)
    group by team_id, pass_stats.passes_completed, pass_stats.passes_attempted, shot_stats.goals, shot_stats.shots
)

select * from final