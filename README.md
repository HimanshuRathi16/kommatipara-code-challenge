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

    -- pip install -r .\requirements.txt

- Once the necessary packages are installed, 

    -- python app.pyz <path_to_client_data_csv> <path_to_financial_data_csv> <countries_to_filter_separate_by_commas>
    Ensure to replace <path_to_client_data_csv>, <path_to_financial_data_csv>, and <countries_to_filter> with the actual paths and country filter criteria

    -- eg : python app.pyz ./input_data/dataset_one.csv ./input_data/dataset_two.csv 'Netherlands,United Kingdom'

- You can also run __main__.py file by :

    -- python .\app\__main__.py <path_to_client_data_csv> <path_to_financial_data_csv> <countries_to_filter_separate_by_commas>
    Ensure to replace <path_to_client_data_csv>, <path_to_financial_data_csv>, and <countries_to_filter> with the actual paths and country filter criteria
    
    eg : python .\app\__main__.py .\input_data\dataset_one.csv .\input_data\dataset_two.csv 'Netherlands,United Kingdom'


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



