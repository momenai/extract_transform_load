import sys
import pandas as pd
from datetime import datetime
import sqlite3


class ETL:
    def __init__(self, db):
        self.db = db

    def extract(self, query):
        return pd.read_sql(query, self.db)

    def transform(self):
        # Aggregate the `online_retail_history` and `stock_description` datasets
        online_retail_history_agg = self.extract('''SELECT online_retail_history.*, stock_description.*
           FROM online_retail_history
           LEFT JOIN stock_description ON online_retail_history.StockCode = stock_description.StockCode''')
        # Identify and fix corrupt or unusable data
        df = online_retail_history_agg['Description'].count()
        df = online_retail_history_agg['Description'].value_counts()

        # Remove rows where "Description" is just a question mark (?).
        df = online_retail_history_agg[(online_retail_history_agg["Description"] != "?")]

        # Identify and remove duplicates
        duplicated_data = df[df.duplicated(keep=False)]
        # print('Number of rows with duplicated data:', duplicated_data.shape[0])
        online_retail_history_agg_final = df[~df.duplicated()]
        # Correct date formats
        online_retail_history_agg_final['InvoiceDate'] = pd.to_datetime(online_retail_history_agg_final['InvoiceDate'],
                                                                        format='%Y-%m-%d')
        return online_retail_history_agg_final

    def loud(self, df, file_name):
        df.to_pickle(file_name)

    def read_all_tables_in_db(self, query):
        cursor = conn.cursor()
        tables = cursor.execute(query).fetchall()
        table_names = ", ".join([table[0] for table in tables])
        return table_names


def log(msg):
    with open("data/log.txt", "a") as f:
        dt = datetime.today().strftime("%Y-%m-%d %H:%M-%S")
        f.write(str(dt) + "   " + str(msg) + "\n")


try:
    conn = sqlite3.connect('data/prod_sample.db')
    print("Connected to SQLite")

    etl = ETL(conn)

    log("Connected to SQLite, to data/user_data.db")
    query = """SELECT name FROM sqlite_master WHERE type='table';"""
    tables = etl.read_all_tables_in_db(query)

    # print("We have fitch those tables with name: ", table_names)
    # log("We have fitch those tables: " + table_names)
    #
    log("ETL Job Started")

    print("ETL Job Started\n", "Extract phase Started")
    log("Extract phase Started")
    # Get data from db for online_retail_history table
    online_retail_history = etl.extract("SELECT * FROM online_retail_history")
    # print("online_retail_history's shape is ", online_retail_history.shape)

    stock_description = etl.extract("SELECT * FROM stock_description")
    # print("stock_description's shape is ", stock_description.shape)
    log("Extract phase Ended Successfully")
    print("Extract phase Ended Successfully\n\n")

    log("Transform phase Started")
    df_final_format = etl.transform()
    print("Transform phase Started")
    print("Transform phase Ended Successfully\n\n")
    log("Transform phase Ended")

    log("Load phase Started")
    output_file_name = "online_history_cleaned.pickle"
    etl.loud(df_final_format,output_file_name )
    print("loud phase Started")
    log("Load phase Ended Successfully\n\n")
    print("Load phase Ended\n\n")

    # Confirm that the data was written to the pickle file
    # print(pd.read_pickle(output_file_name).info())
    log("ETL ended successfully")
    print("ETL ended successfully")

except sqlite3.Error as error:
    print("Failed to execute the above query", error)

finally:
    if conn:
        conn.close()
        print("the sqlite connection is closed")
        log("The sqlite connection is closed")
