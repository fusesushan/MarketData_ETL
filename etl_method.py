import findspark
findspark.init()
import yaml
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, from_unixtime
import logging

# Configure the logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def transform_and_load_data(data_path, spark, config):
    try:
        df = spark.read.parquet(data_path)
        df.printSchema()

        # Convert timestamp to date format
        df = df.withColumn("t", from_unixtime(col("t"), "yyyy-MM-dd"))

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
