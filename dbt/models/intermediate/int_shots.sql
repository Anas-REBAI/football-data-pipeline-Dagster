{{ config(materialized='view') }}

with shots as (
    select *
    from {{ ref('stg_events') }}
    where type_name = 'Shot'
),

goals_and_shots as (
    select
        event_id,
        team_id,
        team_name,
        minute,
        second,
        duration,
        -- Assuming goal detection is based on a specific flag (placeholder below)
        case when play_pattern_name = 'From Turnover' then 1 else 0 end as is_goal
    from shots
)

select * from goals_and_shots