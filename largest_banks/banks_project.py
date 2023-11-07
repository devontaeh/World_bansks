import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs =["Bank_Name", "MC_USD_Billion"]
exchange_rate = "E:\Projects\largest_banks\exchange_rate.csv"
csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'

def log_progress(message):
    '''Logs message and appends to txt file'''

    format = '%Y-%h-%d-%H:%M:%S'
    time = datetime.now()
    timestamp = time.strftime(format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

    
def extract(url, table_attribs):
    ''' Extracts required website info and returns dataFrame '''

    response = requests.get(url).text

    #parse HTML
    soup = BeautifulSoup(response, 'html.parser')

    # extract all tbody attribs of HTML and extract all rows
    rows = soup.find_all('table')[0].find_all('tr')
    data_list = []

    for row in rows:
        col = row.find_all('td')
        if col:
            # if col[1].find('a') is not None :   
            data_dict = {"Bank_Name": col[1].find_all('a')[1].contents[0], "MC_USD_Billion": float(col[2].contents[0].replace("\n", ""))}
            data_list.append(data_dict)
            
        
    
    df = pd.DataFrame(data_list, columns=table_attribs)
  

    return df


def transform(df, csv_path):
    #Transforms data - added three new data columns of MC in different currencies
    
    exchange_rate = pd.read_csv(csv_path).set_index('Currency').to_dict()['Rate']
    
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df



def load_to_csv(df, output_path):
    '''
    loads to csv
    '''
    
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    '''
    Loads to SQL database
    '''
    df.to_sql(table_name,sql_connection,if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    '''
    runs query -> print results
    
    '''
    print(query_statement)
    print(pd.read_sql(query_statement, sql_connection))
        


log_progress('Preliminaries complete. Initiating ETL process')

tables = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')
transform = transform(tables,exchange_rate)
log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(transform,csv_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated')
load_to_db(transform,sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')
query_statement = [f'SELECT * FROM {table_name}', f'SELECT AVG(MC_GBP_Billion) FROM {table_name}', f'SELECT {table_attribs[0]} from {table_name} LIMIT 5']
[run_query(statement, sql_connection) for statement in query_statement]
log_progress('Process Complete')
sql_connection.close()
log_progress('Server Connection closed')