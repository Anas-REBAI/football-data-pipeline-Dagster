{{ config(materialized='view') }}

with defensive_actions as (
    select *
    from {{ ref('stg_events') }}
    where type_name in ('Tackle', 'Interception')
)

select
    event_id,
    team_id,
    team_name,
    type_name,
    minute,
    second,
    duration
from defensive_actions