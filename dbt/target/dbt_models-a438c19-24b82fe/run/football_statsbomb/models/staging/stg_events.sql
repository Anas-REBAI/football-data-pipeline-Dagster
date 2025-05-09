

  create or replace view `football_statsbomb_staging`.`stg_events` 
  
    
  
  
    
    
  as (
    

with raw_events as (
    select *
    from `football_statsbomb`.`events`
),

renamed as (
    select
        id as event_id,
        index as event_index,
        period,
        timestamp,
        minute,
        second,
        type_id,
        type_name,
        possession,
        team_id,
        team_name,
        possession_team_id,
        possession_team_name,
        play_pattern_id,
        play_pattern_name,
        duration,
        formation
    from raw_events
)

select * from renamed
    
  )
      
      
                    -- end_of_sql
                    
                    