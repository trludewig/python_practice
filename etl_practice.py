import pandas as pd 
import glob 
import xml.etree.ElementTree as ET 
from datetime import datetime 

target_file = "extracted_data.csv"
log_file = "log_file.txt"

def extract_json(filename):
    df = pd.read_json(filename, lines=True)
    return df 

def extract_csv(filename):
    df = pd.read_csv(filename)
    return df 

def extract_xml(filename):
    df = pd.DataFrame(columns=["car_model","year_of_manufacture","price","fuel"])
    tree = ET.parse(filename)
    root = tree.getroot()
    for row in root:
        car_model = row.find("car_model").text
        year_of_manufacture = row.find("year_of_manufacture").text
        price = float(row.find("price").text)
        fuel = row.find("fuel").text
        df = pd.concat([df, pd.DataFrame([{"car_model": car_model, 
            "year_of_manufacture": year_of_manufacture, 
            "price": price,
            "fuel": fuel}])], ignore_index=True)
    return df

def extract():
    extracted_data = pd.DataFrame(columns=["car_model","year_of_manufacture","price","fuel"])
    
    csv_files = glob.glob("*.csv")
    for f in csv_files:
        extracted_data = pd.concat([extracted_data, extract_csv(f)], ignore_index=True)

    json_files = glob.glob("*.json")
    for f in json_files:
        extracted_data = pd.concat([extracted_data, extract_json(f)], ignore_index=True)

    xml_files = glob.glob("*.xml")
    for f in xml_files:
        extracted_data = pd.concat([extracted_data, extract_xml(f)], ignore_index=True)

    return extracted_data

def transform(df):
    df["price"] = round(df["price"], 2)
    return df

def load(df, target_file):
    df.to_csv(target_file)

def log_message(message, log_file):
    now = datetime.now()
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    with open(log_file, "a") as f:
        f.write(now.strftime(timestamp_format) + ", " + message + "\n")

log_message("Starting ETL", log_file)

log_message("Starting extract", log_file)
df = extract()
log_message("Ending extract", log_file)

log_message("Starting transform", log_file)
df = transform(df)
log_message("Ending transform", log_file)

log_message("Starting load", log_file)
load(df, target_file)
log_message("Ending load", log_file)

log_message("Ending ETL", log_file)
print(df)