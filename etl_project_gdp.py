import pandas as pd 
import requests 
from bs4 import BeautifulSoup
import sqlite3



url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
output_file = '/home/project/Countries_by_GDP.csv'
table_name = 'Countries_by_GDP'
db_name = 'World_Economies.db'

# Retrieve page
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')
#print(data)

# Parse into df
tables = data.find_all('tbody')
rows = tables[2].find_all('tr')
#print(rows)
#print(len(rows))

dict_list = []
for row in rows:
    cols = row.find_all('td')
    if len(cols) > 0:
        #print(cols)
        country = cols[0].get_text().strip()
        gdp_millions = cols[2].get_text()
        if '—' in gdp_millions:
            gdp_billions = 0.00
            #print('here')
        else:
            gdp_millions = gdp_millions.replace(',', '')
            gdp_billions = round(float(gdp_millions)/1000.0, 2)
        #print(country + " and " + str(gdp_billions))
        dict = {"Country": country, "GDP_USD_billion": gdp_billions}
        dict_list.append(dict)   
#print(dict_list) 
# Write to csv
df = pd.DataFrame(dict_list)
#print(df)
df.to_csv(output_file)

# Write to DB
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()
