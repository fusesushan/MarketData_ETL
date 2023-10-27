import findspark

findspark.init()
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, from_unixtime


def read_yaml_config(yaml_file_path):
    try:
        with open(yaml_file_path, "r") as yaml_file:
            config = yaml.safe_load(yaml_file)
        return config
    except FileNotFoundError:
        print("Error: Config file not found.")
        spark.stop()
        return None

    except Exception as e:
        print(f"Error reading config file: {e}")
        spark.stop()
        return None


def load_and_transform_data(data_path, spark, config):
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

        df = df.withColumnsRenamed(rename)

        # Order by Date in descending order
        df = df.select(
            col("Date"),
            col("Stock"),
            col("Open"),
            col("High"),
            col("Low"),
            col("Close"),
            col("Volume"),
        ).orderBy(desc(col("Date")))

        return df
    except Exception as e:
        print(f"Error loading and transforming data: {e}")
        spark.stop()
        return None


def save_to_postgres(df, config):
    try:
        jdbc_url = config["postgres"]["url"]
        jdbc_properties = {
            "user": config["postgres"]["user"],
            "password": config["postgres"]["password"],
            "driver": "org.postgresql.Driver",
        }

        # Save DataFrames to PostgreSQL table
        df.write.jdbc(
            url=jdbc_url,
            table="market_data",
            mode="overwrite",
            properties=jdbc_properties,
        )
        spark.stop()
    except Exception as e:
        print(f"Error saving data to PostgreSQL: {e}")
        spark.stop()


if __name__ == "__main__":
    # findspark.init()

    # Define the paths and filenames
    data_path = "/home/ubuntu/Desktop/proj_mid/extracted_data/extracted_data.parquet"
    yaml_file_path = "/home/ubuntu/Desktop/proj_mid/config.yaml"

    # Create a Spark session
    spark = (
        SparkSession.builder.appName("transform")
        .config(
            "spark.driver.extraClassPath",
            "/usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.6.0.jar",
        )
        .getOrCreate()
    )

    config = read_yaml_config(yaml_file_path)

    if config:
        df = load_and_transform_data(data_path, spark, config)
        if df:
            df.show()
            save_to_postgres(df, config)
