

  create or replace view `football_statsbomb_intermediate`.`int_defensive_actions` 
  
    
  
  
    
    
  as (
    

with defensive_actions as (
    select *
    from `football_statsbomb_staging`.`stg_events`
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
    
  )
      
      
                    -- end_of_sql
                    
                    