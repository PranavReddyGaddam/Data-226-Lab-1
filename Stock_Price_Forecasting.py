from airflow import DAG
from airflow.decorators import task
from statsmodels.tsa.arima.model import ARIMA
from datetime import timedelta, datetime
import snowflake.connector
import requests
import pandas as pd
import numpy as np


# Snowflake Connection
def return_snowflake_conn():
    hook = SnowflakeHook(conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()
    
"""Extraction of Stock Data from Alpha Vantage API."""

@task
def extract_stock_data(stock_symbol):
    API_KEY = Variable.get('VANTAGE_API_KEY')
    
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()["Time Series (Daily)"]
    
    df = pd.DataFrame.from_dict(data, orient='index')
    df.index = pd.to_datetime(df.index)
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    df['symbol'] = stock_symbol
    
    df = df.loc[df.index >= (datetime.now() - timedelta(days=90))]
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)
    
    return df

"""Loading Data into the tables"""

@task
def load_90_days_data_to_snowflake(df):
    cur = return_snowflake_conn()

    try:
        for _, row in df.iterrows():
            check_query = f"""
            SELECT COUNT(1)
            FROM raw_data.stock_prices
            WHERE date = '{row['date'].strftime('%Y-%m-%d')}'
              AND symbol = '{row['symbol']}'
            """
            cur.execute(check_query)
            exists = cur.fetchone()[0]

            if exists == 0:
                insert_query = f"""
                INSERT INTO raw_data.stock_prices (date, open, high, low, close, volume, symbol)
                VALUES 
                ('{row['date'].strftime('%Y-%m-%d')}', {row['open']}, {row['high']},{row['low']}, {row['close']}, {row['volume']}, '{row['symbol']}')
                """
                cur.execute(insert_query)

        cur.execute("COMMIT;")  
    except Exception as e:
        cur.execute("ROLLBACK;") 
        print(f"Error occurred: {e}")
        raise e
    finally:
        cur.close()  

@task
def load_forecast_to_snowflake(forecast_df):
    cur = return_snowflake_conn()

    try:
        for _, row in forecast_df.iterrows():
            check_query = f"""
            SELECT COUNT(1)
            FROM raw_data.stock_forecasts
            WHERE date = '{row['date'].strftime('%Y-%m-%d')}'
              AND symbol = '{row['symbol']}'
            """
            cur.execute(check_query)
            exists = cur.fetchone()[0]

            if exists == 0:
                insert_query = f"""
                INSERT INTO raw_data.stock_forecasts (date, open, high, low, close, volume, symbol)
                VALUES ('{row['date'].strftime('%Y-%m-%d')}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['volume']}, '{row['symbol']}')
                """
                cur.execute(insert_query)

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error occurred: {e}")
        raise e
    finally:
        cur.close()

"""Next 7 Days prediction"""

def getNext7WorkingDays(today):
    next_7_days = []

    day_count = 0
    for _ in range(7):
        while True:
            next_day = today + timedelta(days=day_count)
            day_count += 1
            if next_day.weekday() < 5: 
                next_7_days.append(next_day.strftime('%Y-%m-%d'))
                break  

    return next_7_days

@task
def predict_next_7_days(df):
    df['date'] = pd.to_datetime(df['date'])
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)

    df = df.sort_values(by='date')

    open_prices = df['open'].values
    high_prices = df['high'].values
    low_prices = df['low'].values
    close_prices = df['close'].values
    volume_values = df['volume'].values

    model_open = ARIMA(open_prices, order=(5, 1, 0))
    model_fit_open = model_open.fit()
    forecast_open = model_fit_open.forecast(steps=7)

    model_high = ARIMA(high_prices, order=(5, 1, 0))
    model_fit_high = model_high.fit()
    forecast_high = model_fit_high.forecast(steps=7)

    model_low = ARIMA(low_prices, order=(5, 1, 0))
    model_fit_low = model_low.fit()
    forecast_low = model_fit_low.forecast(steps=7)

    model_close = ARIMA(close_prices, order=(5, 1, 0))
    model_fit_close = model_close.fit()
    forecast_close = model_fit_close.forecast(steps=7)

    model_volume = ARIMA(volume_values, order=(5, 1, 0))
    model_fit_volume = model_volume.fit()
    forecast_volume = model_fit_volume.forecast(steps=7)

    last_date = df['date'].max()
    future_dates = getNext7WorkingDays(last_date)

    forecast_df = pd.DataFrame({
        'date': future_dates,
        'open': forecast_open,
        'high': forecast_high,
        'low': forecast_low,
        'close': forecast_close,
        'volume': np.ceil(forecast_volume),
        'symbol': df['symbol'].iloc[0]
    })

    forecast_df[['open', 'high', 'low', 'close', 'volume']] = forecast_df[['open', 'high', 'low', 'close', 'volume']].round(2)

    return forecast_df



# DAG definition
with DAG(
    dag_id='stocks_forecast_next_few_days',
    start_date=datetime(2024, 10, 12),
    catchup=False,
    schedule_interval='@daily',
    tags=['ETL']
) as dag:
    stock_symbols = ["CVX", "XOM"]

    for stock_symbol in stock_symbols:
        stock_data = extract_stock_data(stock_symbol)
        load_90_days_data = load_90_days_data_to_snowflake(stock_data)
        forecast_data = predict_next_7_days(stock_data)
        load_forecast_to_snowflake(forecast_data)
    

