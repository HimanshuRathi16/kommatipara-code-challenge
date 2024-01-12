# Overview

- This PySpark project is built to bring together and organize information from two different sets of data.
- These datasets contain details about clients and their financial-related information for a company called KommatiPara,
  which focuses on trading bitcoins.
- The main aim of this project is to create a new set of data that includes client info from the United Kingdom and the Netherlands,
  along with their important financial information.
- This new dataset will be useful for a marketing effort.

Since all the data in the datasets is fake and this is just an exercise, one can forego the issue of having the data stored along with the code in a code repository.

## Requirements

- Python 3.8
- PySpark

## Usage

- To execute the application, begin by installing the required dependencies outlined in the requirements.txt file:

    ```pip install -r .\requirements.txt```

- Once the necessary packages are installed,

    ```python kommatipara_app.pyz <path_to_client_data_csv> <path_to_financial_data_csv> <countries_to_filter_separate_by_commas>```

Ensure to replace <path_to_client_data_csv>, <path_to_financial_data_csv>, and <countries_to_filter> with the actual paths and country filter criteria.

For example, you can run the following command:

`python kommatipara_app.pyz ./input_data/dataset_one.csv ./input_data/dataset_two.csv 'Netherlands,United Kingdom'`

- You can also run __main__.py file by :

    ```python .\kommatipara_app\codebase\__main__.py <path_to_client_data_csv> <path_to_financial_data_csv> <countries_to_filter_separate_by_commas>```

Ensure to replace <path_to_client_data_csv>, <path_to_financial_data_csv>, and <countries_to_filter> with the actual paths and country filter criteria

For example, you can run the following command:

`python .\kommatipara_app\__main__.py ./input_data/dataset_one.csv ./input_data/dataset_two.csv 'Netherlands,United Kingdom'`

*Note: If you are running the code on a local machine, please ensure that you have set the JAVA_HOME and HADOOP_HOME environment variables.*

## Project Structure

The project is organized as follows:

**kommatipara_app/codebase/\_\_main\_\_.py**: This Python script serves as the main entry point for executing the data collation process.

**kommatipara_app/codebase/generic_functions.py**: Hosts generic functions responsible for data filtering and renaming.

**kommatipara_app/codebase/logger.py**: Contains configuration and implementation of logger and logger rotation policies.

**kommatipara_app/test_cases.py**: Encompasses all test cases designed to validate the functionality of the application.

**input_data/**: The directory designated for storing input files crucial to the assignment.

**client_data/**: The directory where the output dataset will be generated.

**requirements.txt**: A file that enumerates the dependencies essential for the project.

**exercise.md**: A document outlining the details and specifications of the assignment.

**README.md**: This documentation file provides comprehensive information about the project structure and functionality.

**.github/workflows/build.yml**: This workflow automates the process of building and packaging a Python project when changes are pushed to the main branch or manually triggered through the GitHub Actions.

## Data Processing

- Extracting client and financial information from designated files is the initial step in the process.
- Eliminating PII from client records is conducted, with the exception of email addresses.
- A filtration process is implemented to include only clients from specific countries, namely the United Kingdom or the Netherlands.
- Enhancing clarity, certain column names are revised as follows:
  - The column labeled "id" is updated to "client_identifier".
  - The column labeled "btc_a" is modified to "bitcoin_address".
  - The column labeled "cc_t" is transformed to "credit_card_type".
- The refined and renamed dataset is then stored in the client_data directory in root folder.

## Logging

- Logs are written to file.log in root directory with rotating policy.
- The old log files will be retained up to the 5 backupCount.

## Additional features and improvements

- Source Distribution: The project is designed to create a source distribution file, facilitating seamless distribution and deployment. The build process generates a file named 'kommatipara_app.pyz,' allowing direct execution with arguments.

- Automated Build pipeline: Automated build pipeline is created to automate the building and packaging of a Python project. It activates either when changes are pushed to the main branch or when manually triggered through the GitHub Actions UI.

- Dependency Management: Utilize the provided requirements.txt file for straightforward installation of project dependencies.

- Enhanced Documentation: Code documentation follows the reStructuredText (reST) format, improving code readability and understanding.

- Log Rotation Policy: The log file undergoes rotation upon reaching a size of 10 MB, maintaining concise and organized logs.
