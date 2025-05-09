{{ config(materialized='view') }}

with raw_competitions as (
    select *
    from {{ source('football_statsbomb', 'competitions') }}
),

renamed as (
    select
        competition_id,
        season_id,
        country_name,
        competition_name,
        competition_gender,
        competition_youth,
        competition_international,
        season_name,
        match_updated,
        match_updated_360,
        match_available_360,
        match_available
    from raw_competitions
)

select * from renamed