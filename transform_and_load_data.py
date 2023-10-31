import etl_method as etl
import logging
from pathlib import Path


# Configure the logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

base_dir = Path(__file__).parents[0]
yaml_file_path = os.path.join(base_dir, 'config.yaml')

if __name__ == "__main__":
    logger = etl.setup_logs()
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
