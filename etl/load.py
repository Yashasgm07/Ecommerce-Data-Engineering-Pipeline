import pandas as pd
import mysql.connector
import logging
import os

password = os.getenv("DB_PASSWORD")



def load_data_to_mysql(df: pd.DataFrame):

    try:
        if df is None or df.empty:
            raise ValueError("DataFrame is empty. Nothing to load.")

        logging.info("Connecting to MySQL...")

        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password=os.getenv("DB_PASSWORD"),
            database="ecommerce_pipeline"
        )

        cursor = connection.cursor()

        insert_query = """
        INSERT INTO sales_data (
            order_id,
            order_date,
            status,
            fulfilment,
            sku,
            category,
            quantity,
            currency,
            amount,
            ship_city,
            ship_state,
            ship_country,
            b2b,
            fulfilled_by,
            business_status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            order_date = VALUES(order_date),
            status = VALUES(status),
            fulfilment = VALUES(fulfilment),
            sku = VALUES(sku),
            category = VALUES(category),
            quantity = VALUES(quantity),
            currency = VALUES(currency),
            amount = VALUES(amount),
            ship_city = VALUES(ship_city),
            ship_state = VALUES(ship_state),
            ship_country = VALUES(ship_country),
            b2b = VALUES(b2b),
            fulfilled_by = VALUES(fulfilled_by),
            business_status = VALUES(business_status);
        """

        # Convert DataFrame rows into clean tuples
        data = [
            tuple(None if pd.isna(value) else value for value in row)
            for row in df.itertuples(index=False, name=None)
        ]

        logging.info(f"Loading {len(data)} records into MySQL...")

        cursor.executemany(insert_query, data)
        connection.commit()

        logging.info("Incremental load completed successfully.")
        print(f"✅ Loaded {len(data)} records successfully!")

        cursor.close()
        connection.close()

    except Exception as e:
        logging.error(f"MySQL loading failed: {e}")
        print("❌ Error loading data:", e)
