{{ config(materialized='view') }}

with events as (
    select *
    from {{ ref('stg_events') }}
),

player_touches as (
    select
        team_id,
        team_name,
        possession,
        type_name,
        count(*) as event_count
    from events
    group by team_id, team_name, possession, type_name
)

select * from player_touches
