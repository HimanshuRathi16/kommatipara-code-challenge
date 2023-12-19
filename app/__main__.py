import argparse

import pyspark
from pyspark.sql import SparkSession

import generic_functions
from logger import setup_logger

# Initialize the Spark session
sc = pyspark.SparkConf()
spark = SparkSession.builder.getOrCreate()

# Set up logging
logger = setup_logger()

def main():
    # Log that the application has started
    logger.info("KommatiPara Application started")
    parser = argparse.ArgumentParser(description='KommatiPara Application')

    # Parse command line arguments
    parser.add_argument('client_csv', help='path for the client.csv')
    parser.add_argument('financial_csv', help='path for the finance.csv')
    parser.add_argument('countries', type=str, help='Enter countries to filter (comma-separated)')
    args = parser.parse_args()

    # Set the output directory
    output_dir = "./client_data/"

    # Load client and financial data from CSV files
    df_client_data = generic_functions.load_csv_file(spark, args.client_csv)
    df_financial_data = generic_functions.load_csv_file(spark, args.financial_csv)

    # Parse and filter countries from the command line arguments
    countries_to_filter = [country.strip() for country in args.countries.split(',')]
    df_client_data_filtered = generic_functions.filter_countries(df_client_data, countries_to_filter)

    # Remove personal identifiable information from the client dataset
    df_client_data_clean = generic_functions.remove_columns(df_client_data_filtered, ["first_name", "last_name"])

    # Remove sensitive information from the financial dataset
    df_financial_data_clean = generic_functions.remove_columns(df_financial_data, ["cc_n"])

    logger.info("Joining the dataframe using id field")

    # Join client and financial dataframes on the 'id' field
    df_final = (df_client_data_clean.join(df_financial_data_clean, 'id', 'left'))

    # Rename columns based on the defined mapping
    column_mapping = {"id": "client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"}
    logger.info("Renaming the columns for the easier readability to the business users")
    df_final = generic_functions.rename_columns(df_final, column_mapping)

    # Write the final dataframe to a CSV file in overwrite mode
    df_final.write.mode("overwrite").csv(output_dir)
    logger.info("Processed client data is now available to use in client_data directory.")

if __name__ == "__main__":
    main()
