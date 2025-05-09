

  create or replace view `football_statsbomb_staging`.`stg_lineups` 
  
    
  
  
    
    
  as (
    

with raw_lineups as (
    select *
    from `football_statsbomb`.`lineups`
),

renamed as (
    select
        team_id,
        team_name,
        player_id,
        player_name,
        position_id,
        position_name,
        jersey_number,
        country
    from raw_lineups
)

select * from renamed
    
  )
      
      
                    -- end_of_sql
                    
                    