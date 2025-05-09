

  create or replace view `football_statsbomb_staging`.`stg_three_sixty` 
  
    
  
  
    
    
  as (
    

with raw_three_sixty as (
    select *
    from `football_statsbomb`.`three_sixty`
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
    
  )
      
      
                    -- end_of_sql
                    
                    