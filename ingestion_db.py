import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename="logs/.log",
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filemode='a'
)

engine = create_engine('sqlite:///inventory.db')

def ingestion_db(df, table_name, engine) :
    df.to_sql(table_name, engine, if_exists='replace', index=False)

def load_raw_data():
    for file in os.listdir("data"):
        if '.csv' in file:
            df = pd.read_csv('data/' + file)
            logging.info(f'Ingesting file: {file} in db')
            ingestion_db(df, file[:-4], engine)

    end = time.time()
    total_time = (end - start)/60
    logging.info("--------------------------------------------------")
    logging.info('Ingestion process completed successfully.')
    logging.info(f"\nTotal time taken for ingestion: {total_time} minutes")
    logging.info("--------------------------------------------------")

if __name__ == '__main__':
    load_raw_data()