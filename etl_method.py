import findspark
findspark.init()
import yaml
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pathlib import Path
import os
from datetime import datetime
from pyspark.sql.functions import col, desc, from_unixtime,to_date
import logging

# Configure the logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def make_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def setup_logging_handler(file_dir):
    file_path = os.path.join(file_dir, f'{__name__}.log {datetime.now()}')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(file_path)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    return logger

def setup_logs():
    base_dir = Path(__file__).parents[0]
    log_directory = os.path.join(base_dir, 'logs')
    make_dir(log_directory)
    logger = setup_logging_handler(log_directory)
    return logger

def fetch_stock_data(url, symbol):
    try:
        response = requests.get(url)

        if response.status_code in [200, 203]:
            data = response.json()
            df = pd.DataFrame(data)
            df = df.drop('s', axis=1)
            return df
        else:
            logger.warning(f"Failed to retrieve data for {symbol}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {symbol}: {e}")
        return None
    except Exception as e:
        logger.error(f"An error occurred for {symbol}: {e}")
        return None

def start_spark_session(spark_jar_driverPath):
    try:
        # Create a Spark session
        return (
            SparkSession.builder.appName("transform")
            .config(
                "spark.driver.extraClassPath",
                spark_jar_driverPath,
            )
            .getOrCreate()
        )
    except Exception as e:
        logger.error(f"Error starting Spark session: {e}")
        return None

def read_yaml_config(yaml_file_path):
    try:
        with open(yaml_file_path, "r") as yaml_file:
            config = yaml.safe_load(yaml_file)
        return config
    except FileNotFoundError:
        logger.error("Error: Config file not found.")
        return None
    except Exception as e:
        logger.error(f"Error reading config file: {e}")
        return None

def validate_data_types(df):
    try:
        expected_schema = {
            "t": "bigint",
            "o": "double",
            "h": "double",
            "l": "double",
            "c": "double",
            "v": "bigint",
            "Stock": "string"
        }

        for col_name, col_type in df.dtypes:
            if col_name in expected_schema:
                expected_type = expected_schema[col_name]
                if col_type != expected_type:
                    logger.error(f"Data type validation failed for column: {col_name}. Expected data type {expected_type} but got {col_type}.")
                    return False
            else:
                logger.error(f"Data type validation failed for column: {col_name}. Column not expected in schema.")
                return False

        logger.info("Data type validation passed.")
        return True
    except Exception as e:
        logger.error(f"Data type validation error: {e}")
        return False




def transform_and_load_data(data_path, spark, config):
    try:
        df = spark.read.parquet(data_path)
        df.printSchema()

        #validate the data types of the extracted data
        if df:
            if validate_data_types(df):
                df.show()
            else:
                logger.error("Data type validation failed. Aborting ETL process.")
        else:
            logger.error("Data transformation failed. Aborting ETL process.")

        # Convert timestamp to date format
        df = df.withColumn("t", to_date(from_unixtime(col("t"), "yyyy-MM-dd")))

        # Rename columns
        rename = {
            "t": "Date",
            "o": "Open",
            "c": "Close",
            "l": "Low",
            "h": "High",
            "v": "Volume",
        }

        df = df.select([col(c).alias(rename.get(c, c)) for c in df.columns])

        # Order by Date in descending order
        df = df.orderBy(desc("Date"))
        df.printSchema()
        return df
    except Exception as e:
        logger.error(f"Error loading and transforming data: {e}")
        return None

def save_to_postgres(df, config):
    try:
        jdbc_url = f"jdbc:postgresql://{config['postgres']['host']}:{config['postgres']['port']}/{config['postgres']['dbname']}"
        jdbc_properties = {
            "user": config["postgres"]["user"],
            "password": config["postgres"]["password"],
            "driver": config["postgres"]["driver"],
        }

        # Save DataFrames to PostgreSQL table
        df.write.jdbc(
            url=jdbc_url,
            table="market_data",
            mode="overwrite",
            properties=jdbc_properties,
        )
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error(f"Error saving data to PostgreSQL: {e}")
