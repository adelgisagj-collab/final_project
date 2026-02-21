import os
from google.cloud import bigquery
import pandas as pd

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "G:/Il mio Drive/PERSONALE/Boolean/Final projects/sql-sandbox-471915-9367768b0079.json"

save_dir = "CSV"
os.makedirs(save_dir, exist_ok=True)

client = bigquery.Client()

tables = [
    "distribution_centers",
    "events",
    "inventory_items",
    "order_items",
    "orders",
    "products",
    "users",
]

for table in tables:  
    query = f"""  
        SELECT *  
        FROM `bigquery-public-data.thelook_ecommerce.{table}`  
    """  
    df = client.query(query).to_dataframe()  
  
    print(f"\n{'='*40}")  
    print(f"TABLE: {table}")  
    print('='*40)  
    df.info()  
    display(df.head())  
  
    # --- trasformazioni specifiche per tabella ---  
    if table == "distribution_centers":  
        df = df.rename(columns={  
            'id': 'center_id',  
            'name': 'center_name'  
        })  
  
    elif table == "events":  
        df = df.rename(columns={'id': 'event_id'})  
        df = df.drop(columns=['uri', 'session_id'])  
  
    elif table == "inventory_items":  
        df = df.rename(columns={  
            'id': 'inventory_item_id',  
            'product_distribution_center_id': 'center_id'  
        })  
        df = df.drop(columns=[  
            'product_category', 'product_name', 'product_brand',  
            'product_department', 'product_sku'  
        ])  
        df['cost'] = df['cost'].round(2)  
        df['product_retail_price'] = df['product_retail_price'].round(2)

    elif table == "order_items":  
        df = df.drop(columns=['id'])
        df = df.rename(columns={'status': 'items_status'})  
  
    elif table == "orders":  
        df = df.rename(columns={  
            'status': 'order_status',  
            'created_at': 'order_created_at',  
            'returned_at': 'order_returned_at',  
            'shipped_at': 'order_shipped_at',  
            'delivered_at': 'order_delivered_at'
        })  
  
    elif table == "products":  
        df = df.drop(columns=['sku'])
        df = df.rename(columns={  
            'id': 'product_id',  
            'distribution_center_id': 'center_id'  
        })  
        df['cost'] = df['cost'].round(2)  
        df['retail_price'] = df['retail_price'].round(2)  
  
    elif table == "users":  
        df = df.rename(columns={'id': 'user_id'})  
        df = df.drop(columns=['latitude', 'longitude'])  
  
    # --- pulizia date ---  
    date_cols = df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[us, UTC]', 'datetime64']).columns  
    for col in date_cols:  
        df[col] = df[col].dt.tz_localize(None).dt.floor('s')  
  
    # --- salvataggio CSV nella cartella scelta ---  
    file_path = os.path.join(save_dir, f"{table}_cleaned.csv")  
    df.to_csv(file_path, index=False)  
    print(f"✅ Salvato: {file_path}")
