import pandas as pd
import logging


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info("Starting transformation...")

        if df is None or df.empty:
            raise ValueError("Input DataFrame is empty or None")

        # -------------------------
        # Remove duplicates
        # -------------------------
        df = df.drop_duplicates()

        # -------------------------
        # Standardize column names
        # -------------------------
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_", regex=False)
        )

        # -------------------------
        # Select required columns
        # -------------------------
        required_columns = [
            "order_id", "date", "status", "fulfilment",
            "sku", "category", "qty", "currency",
            "amount", "ship-city", "ship-state",
            "ship-country", "b2b", "fulfilled-by"
        ]

        df = df[required_columns]

        # -------------------------
        # Rename columns
        # -------------------------
        df = df.rename(columns={
            "date": "order_date",
            "qty": "quantity",
            "ship-city": "ship_city",
            "ship-state": "ship_state",
            "ship-country": "ship_country",
            "fulfilled-by": "fulfilled_by"
        })

        # -------------------------
        # Data Type Conversions
        # -------------------------

        # Date
        df["order_date"] = pd.to_datetime(
            df["order_date"],
            format="%m-%d-%y",
            errors="coerce"
        )

        # Quantity
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["quantity"] = df["quantity"].fillna(0).round(0).astype("int64")

        # Amount
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        df["amount"] = df["amount"].fillna(0.0).astype("float64")

        # B2B → 0/1
        df["b2b"] = (
            df["b2b"]
            .astype(str)
            .str.lower()
            .map({"true": 1, "false": 0})
            .fillna(0)
            .astype("int64")
        )

        # -------------------------
        # Data Quality Checks
        # -------------------------

        initial_count = len(df)

        # Remove negative revenue
        df = df[df["amount"] >= 0]

        # Remove null dates
        df = df[df["order_date"].notnull()]

        final_count = len(df)
        removed_rows = initial_count - final_count

        logging.info(f"Rows removed during quality checks: {removed_rows}")
        print(f"Rows removed during quality checks: {removed_rows}")

        # -------------------------
        # BUSINESS STATUS MAPPING
        # -------------------------

        df["business_status"] = "Other"

        # Delivered (highest priority)
        df.loc[
        df["status"].str.contains("Delivered", case=False, na=False),
        "business_status"
        ] = "Delivered"

        # Cancelled
        df.loc[
            df["status"].str.contains("Cancelled", case=False, na=False),
            "business_status"
        ] = "Cancelled"

        # Returned (after Delivered & Cancelled)
        df.loc[
            df["status"].str.contains("Returned|Returning", case=False, na=False),
            "business_status"
        ] = "Returned"

        # Pending
        df.loc[
            df["status"].str.contains("Pending", case=False, na=False),
            "business_status"
        ] = "Pending"

        # In Transit (remaining shipped-related ones)
        df.loc[
            df["status"].str.contains("Shipped|Out for Delivery|Shipping|Picked", case=False, na=False)
            & (df["business_status"] == "Other"),
            "business_status"
        ] = "In Transit"

        # -------------------------
        # Replace NaN with None (MySQL compatibility)
        # -------------------------
        df = df.where(pd.notnull(df), None)

        logging.info("Transformation complete.")
        print("Transformation complete. Cleaned shape:", df.shape)

        return df

    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        print("Transformation failed:", e)
        return None
