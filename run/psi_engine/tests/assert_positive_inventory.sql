
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select
    sale_date,
    product_id,
    projected_inventory_on_hand
from "psi_supply_chain"."main"."fct_supply_chain_daily"
where projected_inventory_on_hand < 0
  
  
      
    ) dbt_internal_test