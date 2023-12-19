import os
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, Dict

from logger import setup_logger

logger = setup_logger()
def load_csv_file(spark:SparkSession,file_path: str) -> DataFrame:
    """
    Load a CSV file into a Spark DataFrame.

    Parameters:
    - file_path (str): The path to the CSV file.

    Returns:
    - df (pyspark.sql.DataFrame): The Spark DataFrame containing the CSV data.

    Raises:
    - FileNotFoundError: If the specified file does not exist.

    """
    # Check if the file exists
    if not os.path.exists(file_path):
        # If the file does not exist, raise an exception and log the error
        error_message = f"File not found: {file_path}"
        logger.error(error_message)
        raise FileNotFoundError(error_message)

    try:
        # Read the CSV file into a Spark DataFrame
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        return df
    except Exception as e:
        # If an exception occurs during the reading process, log the error and raise the exception
        error_message = f"Error loading CSV file: {e}"
        logger.error(error_message)
        raise e

def filter_countries(df:DataFrame, countries_to_filter:list) -> DataFrame:
    """
    Filter a Spark DataFrame based on a list of countries.

    Parameters:
    - df (pyspark.sql.DataFrame): The input Spark DataFrame.
    - countries_to_filter (list): A list of country names to filter the DataFrame.

    Returns:
    - df_filtered (pyspark.sql.DataFrame): The filtered Spark DataFrame based on the specified countries.

    Raises:
    - ValueError: If any country in countries_to_filter does not exist in the DataFrame.
    """

    # Validate that all countries_to_filter exist in the DataFrame
    existing_countries = set(df.select("country").distinct().rdd.flatMap(lambda x: x).collect())
    non_existing_countries = set(countries_to_filter) - existing_countries

    if non_existing_countries:
        # If any specified country does not exist in the DataFrame, raise an exception and log the error
        error_message = f"The following countries do not exist in the DataFrame: {', '.join(non_existing_countries)}"
        logger.error(error_message)
        raise ValueError(error_message)

    try:
        # Filter the DataFrame based on the specified countries
        df_filtered = df.filter(col("country").isin(countries_to_filter))
        return df_filtered
    except Exception as e:
        # If an exception occurs during the filtering process, log the error and raise the exception
        error_message = f"Error filtering DataFrame: {e}"
        logger.error(error_message)
        raise e

def remove_columns(df:DataFrame, columns_to_remove:list) -> DataFrame:
    """
    Remove specified columns from a Spark DataFrame.

    Parameters:
    - df (pyspark.sql.DataFrame): The input Spark DataFrame.
    - columns_to_remove (list): A list of column names to be removed from the DataFrame.

    Returns:
    - df_filtered (pyspark.sql.DataFrame): The DataFrame with specified columns removed.

    Raises:
    - ValueError: If any column in columns_to_remove does not exist in the DataFrame.

    """
    try:
        # Remove the specified columns from the DataFrame
        df_filtered = df.select(*[col(c) for c in df.columns if c not in columns_to_remove])
        logger.info(f"Columns removed: {columns_to_remove}")
        return df_filtered
    except Exception as e:
        # If an exception occurs during the column removal process, log the error and raise the exception
        error_message = f"Error removing columns from DataFrame: {e}"
        logger.error(error_message)
        raise e

def rename_columns(df:DataFrame, column_mapping:Dict) -> DataFrame:
    """
    Rename columns of a PySpark DataFrame based on a provided mapping.

    Parameters:
    - dataframe: PySpark DataFrame
    - column_mapping: dictionary containing current_column: new_column pairs

    Returns:
    - A new PySpark DataFrame with renamed columns.
    """
    new_df = df
    # Iterate through the column_mapping and rename columns
    for old_col, new_col in column_mapping.items():
        new_df = new_df.withColumnRenamed(old_col, new_col)
    return new_df