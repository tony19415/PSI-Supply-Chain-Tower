
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        inventory_health_status as value_field,
        count(*) as n_records

    from "psi_supply_chain"."main"."fct_supply_chain_daily"
    group by inventory_health_status

)

select *
from all_values
where value_field not in (
    'Healthy','Risk','Stockout'
)



  
  
      
    ) dbt_internal_test