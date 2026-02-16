import pandas as pd
import logging


def extract_data(file_path: str) -> pd.DataFrame:
    try:
        logging.info("Starting data extraction...")
        df = pd.read_csv(file_path, low_memory=False)
        logging.info(f"Extraction successful. Rows: {df.shape[0]}")
        return df
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        return None
