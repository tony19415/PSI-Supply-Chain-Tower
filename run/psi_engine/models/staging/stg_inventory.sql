
  
  create view "psi_supply_chain"."main"."stg_inventory__dbt_tmp" as (
    with source as (
    select * from "psi_supply_chain"."main_raw"."mock_inventory_snapshot"
),

cleaned as (
    select
        md5(cast(coalesce(cast(store_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(item_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(snapshot_date as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as inventory_id,

        cast(current_date as date) as inventory_date,
        cast(store_id as integer) as store_id,
        cast(item_id as integer) as product_id,

        cast(qty_on_hand as integer) as qty_on_hand,

        cast(warehouse_location as varchar) as warehouse_location

    from source
)

select * from cleaned
  );
