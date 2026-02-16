import logging
from extract import extract_data
from transform import transform_data
from load import load_data_to_mysql


logging.basicConfig(
    filename="../logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


if __name__ == "__main__":
    raw_path = "../data/raw_sales.csv"

    df = extract_data(raw_path)

    if df is not None:
        cleaned_df = transform_data(df)

        if cleaned_df is not None:
            cleaned_df.to_csv("../data/cleaned_sales.csv", index=False)
            load_data_to_mysql(cleaned_df)
            print("Pipeline executed successfully!")
            logging.info("Pipeline executed successfully!")
