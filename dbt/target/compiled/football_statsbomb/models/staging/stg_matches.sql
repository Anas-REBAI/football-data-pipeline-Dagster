

with raw_matches as (
    select *
    from football_statsbomb.matches
),

renamed as (
    select
        match_id,
        match_date,
        kick_off,
        competition_id,
        competition_name,
        competition_country,
        season_id,
        season_name,
        home_team_id,
        home_team_name,
        away_team_id,
        away_team_name,
        home_score,
        away_score,
        match_status,
        match_status_360,
        last_updated,
        last_updated_360,
        match_week,
        competition_stage,
        stadium_id,
        stadium_name,
        referee_id,
        referee_name
    from raw_matches
)

select * from renamed