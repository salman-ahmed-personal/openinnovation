# Data_pipeline

Sales and Customer data pipeline with aggregations and visualizations

## Installation

Use this command to install all the required packages

```bash
pip install -r requirements. txt
```

## Usage
Run this command to trigger the pipeline

```bash
python data_pipeline.py
```

## Functionality Documentation

```bash
fetch_users_data(USERS_URL)
```
Function to fetch users data using jsonplaceholder API.

```bash
fetch_sales_data(SALES_DATA_PATH)
```
Function to fetch sales data from csv file.

```bash
pd.merge(user_df, sales_df, left_on='id', right_on='customer_id', how='inner')
```
Join sales and users data for aggregations.

```bash
fetch_weather(row)
```
Function to fetch weather data from openweathermap API.

```bash
merged_df['temp']=merged_df['weather_json'].apply(lambda x: x['main']['temp'])
merged_df['weather']=merged_df['weather_json'].apply(lambda x: x['weather'][0]['main'])
```
Extracting required values from json returned from weather API.


```bash
merged_df['total_amount']=merged_df['quantity']*merged_df['price']
merged_df['order_date'].dt.year.astype('str') +'-'+ merged_df['order_date'].dt.quarter.astype('str')
merged_df['order_date'].dt.year.astype('str') +'-'+ merged_df['order_date'].dt.month.astype('str')
```
Transformations for total sales and month and quarter.

```bash
run_aggs_and_vizs(SALES_DATA_PATH)
```
Function to run all aggregations and visualizations.
1. total_sales_per_user
2. avg_orders_per_prod
3. top_selling_prods
4. quart_sales
5. month_sales
6. avg_sales_per_weather

## Schema Defs
#### Schema for transformed data:
```bash
CREATE TABLE transformed_sales (
	"index" BIGINT, 
	name TEXT, 
	username TEXT, 
	email TEXT, 
	lat TEXT, 
	lng TEXT, 
	order_id BIGINT, 
	customer_id BIGINT, 
	product_id BIGINT, 
	quantity BIGINT, 
	price FLOAT, 
	order_date DATETIME, 
	order_date_unix BIGINT, 
	"temp" FLOAT, 
	weather TEXT, 
	total_amount FLOAT, 
	order_quarter TEXT, 
	order_month TEXT
)
```

#### Schema for Aggregation.1: Total Sales Per User
```bash
CREATE TABLE total_sales_per_user(
	name TEXT, 
	total_amount FLOAT
)
```

#### Schema for Aggregation.2: Average No.Orders Per Product
```bash
CREATE TABLE avg_orders_per_prod (
	product_id BIGINT, 
	quantity BIGINT
)
```

#### Schema for Aggregation.3: Top Selling Products
```bash
CREATE TABLE top_selling_prods (
	product_id BIGINT, 
	quantity BIGINT
)
```

#### Schema for Aggregation.4: Quarterly Sales
```bash
CREATE TABLE quart_sales (
	order_quarter TEXT, 
	quantity BIGINT
)
```

#### Schema for Aggregation.5: Monthly Sales
```bash
CREATE TABLE monthly_sales (
	order_month TEXT, 
	quantity BIGINT
)
```

#### Schema for Aggregation.6: Average Sales Per Weather
```bash
CREATE TABLE avg_sales_per_weather (
	weather TEXT, 
	quantity BIGINT
)
```
