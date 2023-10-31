from pyspark.sql import DataFrame
import logging

# Configure the logging for data validation
data_validation_logger = logging.getLogger(__name__)

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
                    data_validation_logger.error(f"Data type validation failed for column: {col_name}. Expected data type {expected_type} but got {col_type}.")
                    return False
            else:
                data_validation_logger.error(f"Data type validation failed for column: {col_name}. Column not expected in schema.")
                return False

        data_validation_logger.info("Data type validation passed.")
        return True
    except Exception as e:
        data_validation_logger.error(f"Data type validation error: {e}")
        return False
