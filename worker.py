# 2.Worker
# It will take the irn from the irn_table with the limit of 10k, and split it into the 5 parallel api call

import os
from tendo import singleton
from dotenv import load_dotenv
load_dotenv()
import psycopg2
import requests
import time
from psycopg2 import sql
import concurrent.futures
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

postgres_host = os.getenv("PG_HOST")
postgres_db = os.getenv("PG_DATABASE")
postgres_user = os.getenv("PG_USER")
postgres_password = os.getenv("PG_PASSWORD")
postgres_port = os.getenv("PG_PORT")

conn = psycopg2.connect(
    host=postgres_host,
    database=postgres_db,
    port=postgres_port,
    user=postgres_user,
    password=postgres_password
)

cursor = conn.cursor()
logging.info("Postgres DB connected successfully.")

datamap = dict()
def getPendingIrn():
    query = "select irn from invoice_gen where s3_link is NULL order by created_at asc limit 1000"
    cursor.execute(query)
    jobs_data = cursor.fetchall()
    columns = [
        'irn'
    ]
    data = [dict(zip(columns, row)) for row in jobs_data]
    return data

def genrateInvoiceAPICall(irn):
    url = "https://assets-api.finkraftai.com/generate_single/"
    found=False
    for i in range(1, 4, 1):
        response = requests.post(url + irn)
        if response.status_code == 200:
            data = response.json()
            if 'status' in data and data['status'] == True:
                datamap[irn] = {'irn': irn, 's3_link': data['data']['url']}
                found=True
                break
        time.sleep(0.250)
    if found==False:
        datamap[irn] = {'irn': irn, 's3_link': "INVALID"}

def generateInvoices(data):
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Submit tasks to the executor for each item in data
            futures = [executor.submit(genrateInvoiceAPICall, item['irn']) for item in data]
            # Optionally wait for all futures to complete
            concurrent.futures.wait(futures)
    except Exception as e:
        logging.info("Exception happen in the generateInvoices: " + str(e))

def insertLink():
    def chunk_data(data, chunk_size=500):
        items = list(data.items())
        for i in range(0, len(items), chunk_size):
            yield dict(items[i:i + chunk_size])

    try:
        query = sql.SQL("""
                UPDATE invoice_gen AS target
                SET s3_link = data.s3_link
                FROM (VALUES {values}) AS data(irn, s3_link)
                WHERE target.irn = data.irn;
            """)
        for chunk in chunk_data(datamap):
            # Create a list of tuples from the chunk dictionary
            values = [(v['irn'], v['s3_link']) for v in chunk.values()]

            # Format the query with the values
            formatted_query = query.format(values=sql.SQL(',').join(
                sql.Literal(value) for value in values
            ))
            # Execute the update query
            cursor.execute(formatted_query)
            # Commit the transaction for the current batch
            conn.commit()
            logging.info(f"Batch of {len(chunk)} records updated successfully.")
    except Exception as e:
        logging.info("Exception occurred in the insertLink: " + str(e))

if __name__ == '__main__':
    try:
        logging.info("===================================")
        me = singleton.SingleInstance()
        irn_data = getPendingIrn()
        logging.info("No. of invoice to generate: "+str(len(irn_data)))
        generateInvoices(irn_data)
        insertLink()
        logging.info("===================================")
    except Exception as e:
        logging.info("Exception occurred in the main: "+str(e))
    finally:
        logging.info("Closing the DB connection")
        cursor.close()
        conn.close()


