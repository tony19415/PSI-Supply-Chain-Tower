
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from "psi_supply_chain"."main_raw"."train"
where date is null



  
  
      
    ) dbt_internal_test