import pandas as pd 
from bs4 import BeautifulSoup
import sqlite3
import requests
from datetime import datetime 



url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
output_file = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'


def log_progress(message):
    now = datetime.now()
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    with open(log_file, "a") as f:
        f.write(now.strftime(timestamp_format) + ", " + message + "\n")

def extract():
    log_progress("Starting extract")
    dict_list = []
    html = requests.get(url).text
    data = BeautifulSoup(html, 'html.parser')
    #print(data)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        if (len(cols) > 0):
            bank = cols[1].get_text().strip()
            market_cap = float(cols[2].get_text().strip())
            print('bank: ' + bank + ' and ' + str(market_cap))
            dict = {"Name": bank, "MC_USD_Billion": market_cap}
            dict_list.append(dict)
    log_progress("Completing extract")
    return pd.DataFrame(dict_list)
    
def transform(df):
    log_progress("Starting transform")
    rates = pd.read_csv("exchange_rate.csv", index_col="Currency")
    df["MC_GBP_Billion"] = round(df["MC_USD_Billion"] * float(rates.loc["GBP"]["Rate"]), 2)
    df["MC_EUR_Billion"] = round(df["MC_USD_Billion"] * float(rates.loc["EUR"]["Rate"]), 2)
    df["MC_INR_Billion"] = round(df["MC_USD_Billion"] * float(rates.loc["INR"]["Rate"]), 2)
    print(df)
    log_progress("Completing transform")
    return df

def load_to_csv(df):
    df.to_csv(output_file)

def load_to_db(df):
    log_progress("Starting to load to db")
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    log_progress("Finishing load to db")

def run_queries():
    log_progress("Starting run_queries")
    conn = sqlite3.connect(db_name)
    df = pd.read_sql("SELECT NAME FROM Largest_banks limit 5", conn)
    print(df)
    conn.close()
    log_progress("Finishing run_queries")
    

df = extract()
df = transform(df)
load_to_db(df)
run_queries()

