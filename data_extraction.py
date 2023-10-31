import pandas as pd
import etl_method as etl
import os
import logging

# Configure the logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the path to your files
yaml_file_path = 'config.yaml'

def main():
    # Read the YAML file and parse it into a Python dictionary
    config = etl.read_yaml_config(yaml_file_path)

    if config is not None:
        parquet_file_path = config["datapath"]
        stocks = ["GOOGL", "AAPL", "AMZN", "META"]
        start_date = "2023-01-01"
        end_date = "2023-05-01"
        token = config.get("token")

        # Check if the Parquet file already exists
        if os.path.exists(parquet_file_path):
            # Read the existing parquet file to get the latest timestamp
            existing_df = pd.read_parquet(parquet_file_path)
            latest_timestamp = existing_df['t'].max()
        else:
            existing_df = None
            latest_timestamp = 0  # Default value if the file doesn't exist

        # Create an empty list to store DataFrames
        stock_data = []

        # Only overwrite file if fetched data is newer than the latest timestamp in the existing file
        for symbol in stocks:
            url = f'https://api.marketdata.app/v1/stocks/candles/D/{symbol}?from={start_date}&to={end_date}&token={token}'

            if latest_timestamp == 0 or existing_df is None or existing_df.empty:
                df = etl.fetch_stock_data(url, symbol)
            else:
                df = etl.fetch_stock_data(url, symbol)
                if existing_df['t'].max() == df['t'].max():
                    df = None
                    logger.info(f"No new data available for {symbol}. Skipping.")

            if df is not None:
                df['Stock'] = symbol
                stock_data.append(df)

        if stock_data:
            # Concatenate the DataFrames into a single DataFrame
            df_final = pd.concat(stock_data, axis=0, ignore_index=True)

            if existing_df is not None and not existing_df.empty:
                # Append new data to the existing DataFrame
                df_final = pd.concat([existing_df, df_final], ignore_index=True)

            # Save the final DataFrame to a Parquet file
            df_final.to_parquet(parquet_file_path, compression='snappy', engine='auto', index=False)
            logger.info("Data saved successfully.")
        else:
            logger.warning("No valid data to save.")
    else:
        logger.error("Error: Configuration not loaded.")

if __name__ == "__main__":
    main()
