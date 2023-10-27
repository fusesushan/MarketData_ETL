import requests
import pandas as pd
import yaml
import os

# Define the path to your YAML file
yaml_file_path = 'config.yaml'

# Read the YAML file and parse it into a Python dictionary
try:
    with open(yaml_file_path, 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
except FileNotFoundError:
    print("Error: Config file not found.")
    config = None

if config is not None:
    stocks = ["GOOGL", "AAPL", "AMZN", "META"]
    start_date = "2023-01-01"
    last_date = "2023-07-01"
    token = config.get("token")

    # Check if the Parquet file already exists
    parquet_file_path = 'extracted_data/extracted_data.parquet'
    if os.path.exists(parquet_file_path):
        # Read the existing parquet file to get the latest timestamp
        existing_df = pd.read_parquet(parquet_file_path)
        latest_timestamp = existing_df['t'].max()
    else:
        existing_df = None
        latest_timestamp = 0  # Default value if the file doesn't exist

    # Create an empty list to store DataFrames
    stock_data = []

    def fetch_stock_data(symbol, start_date, token):
        # url = f'https://api.marketdata.app/v1/stocks/candles/D/{symbol}?from={start_date}&to={last_date}&token={token}'
        url = f'https://api.marketdata.app/v1/stocks/candles/D/{symbol}?from={start_date}&token={token}'
        try:
            response = requests.get(url)

            if response.status_code in [200, 203]:
                data = response.json()
                df = pd.DataFrame(data)
                df = df.drop('s', axis=1)
                return df
            else:
                print(f"Failed to retrieve data for {symbol}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request error for {symbol}: {e}")
            return None
        except Exception as e:
            print(f"An error occurred for {symbol}: {e}")
            return None

    # Fetch data for each stock and store it in the list
    for symbol in stocks:
        if latest_timestamp == 0 or existing_df is None or existing_df.empty:
            df = fetch_stock_data(symbol, start_date, token)
        else:
            # Only fetch data if it's newer than the latest timestamp in the existing file
            df = fetch_stock_data(symbol, start_date, token)
            if existing_df['t'].max() == df['t'].max():
                df = None
                print(f"No new data available for {symbol}. Skipping.")

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
        print("Data saved successfully.")
    else:
        print("No valid data to save.")
else:
    print("Error: Configuration not loaded.")
