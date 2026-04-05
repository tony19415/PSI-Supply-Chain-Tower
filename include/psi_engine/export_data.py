import duckdb
import os

# 1. Define absolute paths for Docker environment
DB_PATH = '/opt/airflow/include/psi_engine/psi_supply_chain.duckdb'
OUTPUT_PATH = '/opt/airflow/include/scripts/psi_dashboard_data.csv'

def export_to_csv():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        return
    
    print(f"Connecting to DuckDB at {DB_PATH}")

    try:
        with duckdb.connect(DB_PATH) as con:
            print("Exporting fct_supply_chain_daily to CSV")

            query = """
                COPY (
                    SELECT
                        sale_date, 
                        product_id, 
                        store_id, region,
                        manager_key, 
                        manager_masked,
                        qty_sold, 
                        qty_ordered,
                        projected_inventory_on_hand, 
                        dw_inserted_at,
                        inventory_health_status
                    FROM fct_supply_chain_daily
                ) TO '{OUTPUT_PATH}' (HEADER, DELIMITER ',')
            """
        
            con.sql(query)
        
        print(f"Success! Data exported to {OUTPUT_PATH}")

    except Exception as e:
        print(f"Export failed: {str(e)}")

if __name__ == "__main__":
    export_to_csv()