# Stock Data ETL and Forecasting Pipeline

This Airflow DAG is designed to automate the extraction, loading, and forecasting of stock data for specified stock symbols. The pipeline extracts stock data from the Alpha Vantage API, loads it into a Snowflake database, and generates forecasts for the next 7 days using ARIMA models. The forecasted data is then loaded into Snowflake for future analysis and reporting.

## Features
- Extraction of daily stock data using the Alpha Vantage API.
- Loading stock data for the last 90 days into a Snowflake database.
- Forecasting stock prices (open, high, low, close, and volume) for the next 7 days using ARIMA.
- Storing forecasted data into Snowflake for further use.

## Prerequisites
- **Airflow**: Installed and set up for running DAGs.
- **Alpha Vantage API Key**: Required for fetching stock data. You can obtain it from the [Alpha Vantage website](https://www.alphavantage.co/support/#api-key).
- **Snowflake Database**: A Snowflake instance with appropriate permissions to create tables and insert data.

### Required Python Libraries
- `airflow`
- `statsmodels`
- `pandas`
- `snowflake-connector-python`
- `requests`
- `numpy`

Ensure these libraries are installed in your environment before running the DAG.

## Configuration

1. **Snowflake Credentials**: Store the `user_id`, `password`, and `account` in environment variables or use Airflow's `Variable` or `Connection` features.
   
2. **API Key**: Similarly, store the Alpha Vantage API key in a secure location (like Airflow Variables) rather than hardcoding it in the DAG.

3. **Stock Symbols**: You can parameterize the list of stock symbols and pass them dynamically based on the configuration or input files.

## Pipeline Components

### 1. Snowflake Connection (`return_snowflake_conn`)
Establishes a connection to the Snowflake database using the provided credentials. The connection is used for loading data into tables within the Snowflake instance.

### 2. Extraction of Stock Data (`extract_stock_data`)
Fetches the last 90 days of stock data for a given symbol from the Alpha Vantage API. The API response is parsed into a pandas DataFrame for further processing.

### 3. Table Creation
- **`create_90_days_stock_table`**: Creates a table in Snowflake for storing the last 90 days of stock data.
- **`create_forecast_table`**: Creates a table in Snowflake for storing the 7-day forecast data.

### 4. Loading Data into Snowflake (`load_90_days_data_to_snowflake`)
Inserts the extracted stock data into the `raw_data.stock_prices` table in Snowflake. If the data for the given date and symbol already exists, it skips the insert.

### 5. 7-Day Forecast (`predict_next_7_days`)
Generates forecasts for stock prices (open, high, low, close, volume) using ARIMA models for the next 7 days based on the historical stock data.

### 6. Loading Forecasts to Snowflake (`load_forecast_to_snowflake`)
Inserts the forecasted stock data into the `raw_data.stock_forecasts` table in Snowflake. It ensures no duplicate entries are inserted for the same date and stock symbol.

### DAG Configuration
The DAG is scheduled to run daily (`@daily`). For each stock symbol in the `stock_symbols` list, it follows the ETL and forecasting process outlined above.

## Usage
1. Clone this repository and add the DAG to your Airflow instance.
2. Set up the necessary Airflow connections/variables for Snowflake and Alpha Vantage API.
3. Start the DAG from the Airflow UI or trigger it manually.

## Security Notes
- **Credentials**: Ensure your credentials are stored securely, using Airflow's variable or secret management systems.
- **Error Handling**: Error handling is implemented with rollback functionality when loading data into Snowflake to avoid partial data inserts.
