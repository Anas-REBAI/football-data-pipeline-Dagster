

  create or replace view `football_statsbomb_intermediate`.`int_player_events` 
  
    
  
  
    
    
  as (
    

with events as (
    select *
    from `football_statsbomb_staging`.`stg_events`
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
    
  )
      
      
                    -- end_of_sql
                    
                    