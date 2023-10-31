import findspark
findspark.init()
import etl_method as etl
from pyspark.sql import SparkSession

# Define the paths and filenames

yaml_file_path = "/home/user/Documents/mid-project ETL/MarketData_ETL/config.yaml"

if __name__ == "__main__":
    config = etl.read_yaml_config(yaml_file_path)

    spark_jar_driverPath = config["spark_jar_driverPath"]
    # Create a Spark session
    spark = etl.start_spark_session(spark_jar_driverPath)

    
    data_path=config.get("datapath")


    if config:
        df = etl.transform_and_load_data(data_path, spark, config)
        if df:
            df.show()
            etl.save_to_postgres(df, config)
            spark.stop()
