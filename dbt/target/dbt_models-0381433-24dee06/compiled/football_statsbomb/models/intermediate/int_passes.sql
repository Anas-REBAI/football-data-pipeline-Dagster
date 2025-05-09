

with passes as (
    select *
    from `football_statsbomb_staging`.`stg_events`
    where type_name = 'Pass'
),

with_pass_outcome as (
    select
        passes.event_id,
        passes.team_id,
        passes.team_name,
        passes.possession,
        passes.minute,
        passes.second,
        passes.duration,
        -- Assume outcome_id = 1 is 'Complete'; adapt if different
        case when passes.play_pattern_name = 'Regular Play' then 1 else 0 end as is_completed
    from passes
)

select * from with_pass_outcome