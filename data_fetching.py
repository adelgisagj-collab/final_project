import os
from google.cloud import bigquery
import pandas as pd

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "G:/Il mio Drive/PERSONALE/Boolean/Final projects/sql-sandbox-471915-9367768b0079.json"

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
    df.to_csv(f"{table}.csv", index=False)
