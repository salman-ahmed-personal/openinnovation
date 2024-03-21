#!/usr/bin/env python
# coding: utf-8

'''Sales and Customer data pipeline with aggregations and visualizations'''

#All the necessary imports
import pandas as pd
import requests
import datetime
import matplotlib.pyplot as plt
plt.figure(figsize=(10, 10), dpi=100)
from sqlalchemy import create_engine


def fetch_users_data(USERS_URL):
    '''Function to fetch users data from API'''
    response = requests.get(USERS_URL)
    response_json = response.json()
    user_df=pd.json_normalize(response_json)
    user_df=user_df[['id','name','username','email','address.geo.lat','address.geo.lng']]
    user_df.rename(columns={'address.geo.lat':'lat','address.geo.lng':'lng'}, inplace = True)
    return user_df


def fetch_sales_data(SALES_DATA_PATH):
    '''Function to fetch sales data from file'''
    sales_df=pd.read_csv(SALES_DATA_PATH)
    return sales_df


def fetch_weather(row):
    '''Function to fetch weather data from API'''
    complete_url = WEATHER_BASE_URL + "appid=" + WEATHER_API_KEY + "&lat=" + row['lat'] + "&lon=" + row.lng + "&dt=" + str(row.order_date_unix)
    response = requests.get(complete_url)
    return response.json()



def run_aggs_and_vizs(df, db_connection):
    '''Rull all aggregations one by one with visualization'''
    total_sales_per_user(df, db_connection)
    avg_orders_per_prod(df, db_connection)
    top_selling_prods(df, db_connection)
    quart_sales(df, db_connection)
    month_sales(df, db_connection)
    avg_sales_per_weather(df, db_connection)


def total_sales_per_user(df, db_connection):
    '''Aggregation_1: Calc total sales for every user'''
    agg_1=df.groupby(by=["name"])['total_amount'].sum()
    agg_1.to_sql(name='total_sales_per_user', if_exists='replace', con=db_connection)
    agg_1.plot(x="name", y=["total_amount"], kind="bar", title='Total sales per customer') 
    plt.savefig("visualizations/total_sales_per_cust.jpg")


def avg_orders_per_prod(df, db_connection):
    '''Aggregation_2: Calc average no. of orders per product'''
    agg_2=df.groupby(by=["product_id"])['quantity'].mean()
    agg_2.to_sql(name='avg_orders_per_prod', if_exists='replace', con=db_connection)
    agg_2.plot(x="product_id", y=["quantity"], kind="bar", title='Average order quantity per product') 
    plt.savefig("visualizations/avg_orders_per_prod.jpg")

def top_selling_prods(df, db_connection):
    '''Aggregation_3: Calc top selling products'''
    agg_3=df.groupby(by=["product_id"])['quantity'].sum().sort_values(ascending=False)[:10]
    agg_3.to_sql(name='top_selling_prods', if_exists='replace', con=db_connection)
    agg_3.plot(x="product_id", y=["quantity"], kind="bar", title='Top selling products') 
    plt.savefig("visualizations/top_selling_prods.jpg")


def quart_sales(df, db_connection):
    '''Aggregation_4: Calc quarterly sales'''
    agg_4=df.sort_values(by='order_quarter').groupby(by=["order_quarter"])['quantity'].sum()
    agg_4.to_sql(name='quart_sales', if_exists='replace', con=db_connection)
    agg_4.plot(x="order_quarter", y=["quantity"], kind="line", title='Quarterly sales')
    plt.savefig("visualizations/quart_sales.jpg")

def month_sales(df, db_connection):
    '''Aggregation_5: Calc monthly sales'''
    agg_5=df.sort_values(by='order_month').groupby(by=["order_month"])['quantity'].sum()
    agg_5.to_sql(name='monthly_sales', if_exists='replace', con=db_connection)
    agg_5.plot(x="order_month", y=["quantity"], kind="line", title='Monthly sales')
    plt.savefig("visualizations/monthly_sales.jpg")

def avg_sales_per_weather(df, db_connection):
    '''Aggregation_6: Calc average sales per weather condition'''
    agg_6=df.groupby(by=["weather"])['quantity'].mean()
    agg_6.to_sql(name='avg_sales_per_weather', if_exists='replace', con=db_connection)
    agg_6.plot(x="weather", y=["quantity"], kind="bar", title='Average sales per weather condition') 
    plt.savefig("visualizations/avg_sales_per_weather.jpg")


if __name__ == "__main__":
    #Vars declaration
    #**Note API Tokens should be hidden
    RETAIL_COMPANY_DB = create_engine("sqlite:///db/retail_company.db")  # pass your db url
    USERS_URL = "https://jsonplaceholder.typicode.com/users"
    WEATHER_API_KEY = "90040f64fbc913fdfa0513197b181600"
    WEATHER_BASE_URL = "http://api.openweathermap.org/data/2.5/weather?"
    SALES_DATA_PATH = "source_data/random_data.csv"

    #Source_1 : Customer data
    user_df = fetch_users_data(USERS_URL)
    print("Successfully fetched users data...")

    #Source_2 : Sales data
    sales_df = fetch_sales_data(SALES_DATA_PATH)
    print("Successfully fetched sales data...")

    #Join Sales-Users dataframes
    merged_df=pd.merge(user_df, sales_df, left_on='id', right_on='customer_id', how='inner')
    print("Merged user-sales data...")

    #Source_3 : Weather data
    merged_df=merged_df[:5]
    merged_df['order_date']=pd.to_datetime(merged_df['order_date'])
    merged_df['order_date_unix']=merged_df['order_date'].apply(lambda x : int((x-datetime.datetime(1970,1,1)).total_seconds()))
    merged_df['weather_json']=merged_df.apply(fetch_weather, axis=1)
    print("Successfully fetched weather data...")

    #Extracting weather info
    merged_df['temp']=merged_df['weather_json'].apply(lambda x: x['main']['temp'])
    merged_df['weather']=merged_df['weather_json'].apply(lambda x: x['weather'][0]['main'])
    del merged_df['weather_json']
    del merged_df['id']

    #Calculating total price per product w.r.t quantity
    merged_df['total_amount']=merged_df['quantity']*merged_df['price']
    
    #Extracting quarter and month from date
    merged_df['order_quarter'] = merged_df['order_date'].dt.year.astype('str') +'-'+ merged_df['order_date'].dt.quarter.astype('str')
    merged_df['order_month'] = merged_df['order_date'].dt.year.astype('str') +'-'+ merged_df['order_date'].dt.month.astype('str')


    #Dumping transformed data in db
    merged_df.to_sql(name='transformed_sales', if_exists='replace', con=RETAIL_COMPANY_DB)
    print("Successfully dumped transformed data in db...")

    print("Running aggregations and creating visualizations...")
    
    run_aggs_and_vizs(merged_df, RETAIL_COMPANY_DB)

    print("Successfully Complete.")
    print("Data has been dumped to respective tables in db.")
    print("Visualizations are in visualizations folder.")
