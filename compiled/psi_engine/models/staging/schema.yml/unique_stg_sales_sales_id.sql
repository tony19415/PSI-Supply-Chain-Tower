
    
    

select
    sales_id as unique_field,
    count(*) as n_records

from "psi_supply_chain"."main"."stg_sales"
where sales_id is not null
group by sales_id
having count(*) > 1


