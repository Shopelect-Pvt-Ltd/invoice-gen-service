import time
from tendo import singleton
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
import logging
import requests

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

def updateJobStatus(status,reqid=None):
    if reqid is not None:
        query = "update invoice_gen_request set status='" + str(status) + "' where id='" + str(reqid) + "'"
        cursor.execute(query)
        conn.commit()
    else:
        query = (
            "update invoice_gen_request set status='"+str(status)+"' where id in (select igr.id from invoice_gen_request igr,"
            "invoice_gen ig where igr.status='RUNNING' and igr.id=ig.reqid and ig.s3_link is not NULL);")
        cursor.execute(query)
        conn.commit()
def getProcessingJob():
    query = ("select id,user_id,type,source_file_url from invoice_gen_request where status='PROCESSING' order by "
             "created_at asc limit 1")
    cursor.execute(query)
    jobs_data = cursor.fetchall()
    columns = [
        'id',
        'user_id',
        'type',
        'source_file_url'
    ]
    data = [dict(zip(columns, row)) for row in jobs_data]
    return data

def downloadFile(url):
    filename = url.split('/')[-1]
    if not filename.endswith('.csv'):
        return None
    response = requests.get(url)
    for i in range(1, 3, 1):
        if response.status_code == 200:
            local_file_name = 'updater_temp/' + filename
            with open(local_file_name, 'wb') as file:
                file.write(response.content)
            logging.info("Writing to the file is completed")
            return local_file_name
        time.sleep(2)
    return None

def irnStatusChecker(filepath,reqid):
    try:
        cursor.execute("""
                CREATE TEMP TABLE invoice_gen_checker (
                    irn varchar
                );
            """)
        with open(filepath, 'r') as f:
            next(f)  # Skip the header row
            cursor.copy_expert("COPY invoice_gen_checker (irn) FROM STDIN WITH CSV", f)

        cursor.execute("""
               SELECT ig.irn
               FROM invoice_gen ig
               JOIN invoice_gen_checker igc ON ig.irn = igc.irn
               WHERE ig.s3_link IS NULL;
            """)
        fetched_row = cursor.fetchall()
        if len(fetched_row)==0:
            updateJobStatus("INVOICE GENERATED",reqid)

        conn.commit()
    except Exception as e:
        logging.info("Exception occurred in the irnStatusChecker: " + str(e))


if __name__ == '__main__':
    try:
        logging.info("===================================")
        me = singleton.SingleInstance()
        updateJobStatus('INVOICE GENERATED')
        logging.info("Updated the running job status")
        jobs=getProcessingJob()
        if jobs is not None and len(jobs) != 0:
            for job in jobs:
                logging.info("Job Details: " + str(job))
                source_file_url = job['source_file_url']
                reqid = job['id']
                local_file_path = downloadFile(source_file_url)
                if local_file_path is not None:
                    irnStatusChecker(local_file_path,reqid)
                    os.remove(local_file_path)
                else:
                    updateJobStatus("FAILED",reqid)
                    logging.info("Error while inserting the irn file to DB")
        else:
            logging.info("No job")
        logging.info("Updated the status successfully.")
        logging.info("===================================")
    except Exception as e:
        logging.info("Exception occurred in main: "+str(e))
    finally:
        logging.info("Closing the DB connection")
        cursor.close()
        conn.close()
