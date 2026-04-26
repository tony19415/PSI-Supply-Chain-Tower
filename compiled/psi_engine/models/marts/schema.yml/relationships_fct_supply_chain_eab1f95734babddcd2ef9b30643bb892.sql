
    
    

with child as (
    select product_id as from_field
    from "psi_supply_chain"."main"."fct_supply_chain_daily"
    where product_id is not null
),

parent as (
    select product_id as to_field
    from "psi_supply_chain"."main"."dim_product"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


