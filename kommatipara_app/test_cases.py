import pytest
from pyspark.sql import SparkSession
import generic_functions


@pytest.fixture(scope="module")
def spark_session():
    # Create a spark session for testing
    spark = (SparkSession.builder
             .master("local")
             .appName("TestSession")
             .getOrCreate()
             )
    yield spark
    spark.stop()


def test_valid_file_path(spark_session):
    valid_datapath = 'dataset_one.csv'
    try:
        generic_functions.load_csv_file(
            spark_session,
            file_path=valid_datapath
        )
    except Exception as e:
        pytest.fail(f"Unexpected exception: {e}")


def test_invalid_file_path(spark_session):
    with pytest.raises(ValueError):
        invalid_datapath = 'data/input/financial_dataset.csv'
        generic_functions.load_csv_file(
            spark_session,
            file_path=invalid_datapath
        )


def test_valid_columns(spark_session):
    dataset = generic_functions.load_csv_file(
        spark_session, file_path='input_data/dataset_one.csv')
    actual_columns = dataset.columns
    expected_columns = ['id', 'first_name', 'last_name', 'email', 'country']
    assert actual_columns == expected_columns


def test_invalid_columns(spark_session):
    dataset = generic_functions.load_csv_file(
        spark_session, file_path='input_data/dataset_two.csv')
    actual_columns = dataset.columns
    expected_columns = ['id', 'first_name', 'last_name', 'email', 'citycode']
    assert actual_columns == expected_columns


def test_filter_invalid_countries(spark_session):
    dataset = generic_functions.load_csv_file(
        spark_session, file_path='input_data/dataset_one.csv')
    countries_to_filter_invalid = ["US", "Sweden"]
    with pytest.raises(ValueError, match="The following countries do not exist in the DataFrame: NonExistingCountry"):
        generic_functions.filter_countries(
            dataset, countries_to_filter_invalid)


def test_remove_valid_columns(spark_session):
    actual_columns_to_remove = ["first_name", "last_name"]
    dataset = generic_functions.load_csv_file(
        spark_session, file_path='input_data/dataset_one.csv')
    dataset_remove_columns = generic_functions.remove_columns(
        dataset, actual_columns_to_remove)
    all_columns = dataset.columns
    expected_renaming_columns = dataset_remove_columns.columns
    remaining_columns = [
        col for col in all_columns if col not in actual_columns_to_remove]
    assert expected_renaming_columns == remaining_columns


def test_remove_invalid_columns(spark_session):
    actual_columns_to_remove = ["citycode"]
    dataset = generic_functions.load_csv_file(
        spark_session, file_path='input_data/dataset_one.csv')
    with pytest.raises(ValueError, match="The following countries do not exist in the DataFrame: NonExistingCountry"):
        generic_functions.remove_columns(dataset, actual_columns_to_remove)
