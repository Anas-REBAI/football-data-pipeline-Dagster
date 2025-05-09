{{ config(materialized='view') }}

with possessions as (
    select
        match_id,
        possession,
        possession_team_id as team_id,
        possession_team_name as team_name,
        sum(duration) as possession_duration_seconds,
        count(*) as number_of_events
    from {{ ref('stg_events') }}
    group by match_id, possession, possession_team_id, possession_team_name
)

select * from possessions