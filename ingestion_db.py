import pandas as pd
import os
import time
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Create DB engine (SQLite in this case)
engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    """Ingest the dataframe into a database table"""
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Successfully ingested table: {table_name}")
    except Exception as e:
        logging.error(f"Error ingesting {table_name}: {e}")

def load_raw_data():
    """Load CSVs as dataframes and ingest them into the database"""
    start = time.time()
    folder = r"C:\Users\anike\Videos\data"

    for file in os.listdir(folder):
        if file.endswith(".csv"):
            filepath = os.path.join(folder, file)
            try:
                df = pd.read_csv(filepath)
                logging.info(f"Reading file: {file}")
                ingest_db(df, file[:-4], engine)
            except Exception as e:
                logging.error(f"Error processing {file}: {e}")

    end = time.time()
    total_time = (end - start) / 60
    logging.info("-------- Ingestion Complete --------")
    logging.info(f"Total Time Taken: {total_time:.2f} minutes")

if __name__ == "__main__":
    load_raw_data()
