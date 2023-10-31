import etl_method as etl
import logging


# Configure the logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

yaml_file_path = "/home/user/Documents/mid-project ETL/MarketData_ETL/config.yaml"

if __name__ == "__main__":
    config = etl.read_yaml_config(yaml_file_path)

    if config:
        spark_jar_driverPath = config["spark_jar_driverPath"]
        # Create a Spark session
        spark = etl.start_spark_session(spark_jar_driverPath)

        data_path = config.get("datapath")

        if spark:
            df = etl.transform_and_load_data(data_path, spark, config)
            if df:
                df.show()
                etl.save_to_postgres(df, config)
                spark.stop()
                logger.info("Spark session stopped.")
        else:
            logger.error("Failed to create a Spark session.")
    else:
        logger.error("Error: Configuration not loaded.")