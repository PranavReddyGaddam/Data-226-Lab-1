create or replace database stock_db;
use database stock_db;

create or replace schema raw_data;
use schema raw_data;


CREATE OR REPLACE TABLE raw_data.stock_prices (
      date DATE,
      open FLOAT,
      high FLOAT,
      low FLOAT,
      close FLOAT,
      volume INTEGER,
      symbol STRING,
      PRIMARY KEY (date,symbol)
    );

CREATE OR REPLACE TABLE raw_data.stock_forecasts (
    date DATE,                      
    open FLOAT,                     
    high FLOAT,                     
    low FLOAT,                      
    close FLOAT,  
    volume FLOAT, 
    symbol STRING
);

CREATE OR REPLACE TABLE raw_data.stock_union_data AS
        SELECT *
        FROM raw_data.stock_prices
        UNION
        SELECT *
        FROM raw_data.stock_forecasts;

select * from stock_prices order by date desc, symbol asc;
select * from stock_forecasts order by date desc, symbol asc;
select * from stock_union_data order by date desc, symbol asc;
