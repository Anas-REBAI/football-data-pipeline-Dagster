{{ config(materialized='view') }}

with raw_three_sixty as (
    select *
    from {{ source('football_statsbomb', 'three_sixty') }}
),

renamed as (
    select
        event_uuid,
        teammate,
        actor,
        keeper,
        x,
        y,
        visible_area
    from raw_three_sixty
)

select * from renamed
