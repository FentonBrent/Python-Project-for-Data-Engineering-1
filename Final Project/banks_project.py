# Code for ETL operations on Country-GDP data

# Importing the required libraries

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'

table_name = 'Largest_banks'
table_attribs = ["Rank", "Name", "MC_USD_Billion"]

csv_path = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'
database_path = './Banks.db'
log_path = './code_log.txt'


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
        code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_path, "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
        information from the website and save it to a data frame. The
        function returns the data frame for further processing. '''

    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
                table_attribs[0]: col[0].contents[0].strip(),
                table_attribs[1]: col[1].text.strip(),
                table_attribs[2]: col[2].contents[0].strip()
            }

            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    df[table_attribs[2]] = df[table_attribs[2]].astype(float).round(2)
    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    # Load exchange rates from CSV file, and place them in a dictionary
    exchange_rate = pd.read_csv(csv_path, index_col=0).to_dict()['Rate']

    # Calculate scaled market cap for each currency
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path, index=False)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output.to_string(index=False))
    print('\n')


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initializing ETL process.')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_path)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, output_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(database_path)
log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete')

sql_connection.close()
log_progress('Server Connection closed')