-- 

with source as (
    select * from "psi_supply_chain"."main_raw"."train"
    -- select * from main_raw.train
)

select
    -- Generate unique ID for every sale row if one doesn't exist
    md5(cast(coalesce(cast(date as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(store as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(item as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as sales_id,

    -- Standardize column name
    cast(date as date) + interval '2970 days' as sale_date,
    cast(store as integer) as store_id,
    cast(item as integer) as product_id,
    cast(sales as integer) as qty_sold

from source