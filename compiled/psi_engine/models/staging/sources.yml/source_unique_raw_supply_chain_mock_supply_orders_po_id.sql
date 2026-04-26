
    
    

select
    po_id as unique_field,
    count(*) as n_records

from "psi_supply_chain"."main_raw"."mock_supply_orders"
where po_id is not null
group by po_id
having count(*) > 1


