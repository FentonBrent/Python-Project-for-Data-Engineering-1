import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Rank", "Bank name", "Market cap (US$ billion)"]
table_name = 'By market capitalization'


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
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


log_progress('Preliminaries complete. Initializing ETL process.')
df = extract(url, table_attribs)
log_progress('Data extraction complete.')

print(df.to_string(index=False))
