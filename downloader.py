# 3. File Downloader
# It will check in the job table whether the job is completed and of that email send flag is false. Pick the file from s3 make the zip file and send it to the sender mail

import os
from tendo import singleton
from dotenv import load_dotenv
load_dotenv()
import psycopg2
import boto3
import shutil
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
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

aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")
s3_bucket_files_prefix = os.getenv("S3_BUCKET_FILES_PREFIX")

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")

conn = psycopg2.connect(
    host=postgres_host,
    database=postgres_db,
    port=postgres_port,
    user=postgres_user,
    password=postgres_password
)

cursor = conn.cursor()
logging.info("Postgres DB connected successfully.")

s3_client = boto3.client('s3',
                         aws_access_key_id=aws_access_key,
                         aws_secret_access_key=aws_secret_access_key,
                         region_name=aws_region)


def download_file_from_s3(s3_url, local_directory):
    # Parse the S3 URL
    parsed_url = urlparse(s3_url)
    bucket_name = parsed_url.netloc.split('.')[0]
    object_key = parsed_url.path.lstrip('/')
    file_name = os.path.basename(object_key)
    folder_name = s3_url.split('/')[-2]

    local_directory = local_directory + folder_name + "/"

    if not os.path.exists(local_directory):
        os.makedirs(local_directory)
        logging.info(f"Folder '{local_directory}' created.")
    else:
        logging.info(f"Folder '{local_directory}' already exists.")

    local_file_path = os.path.join(local_directory, file_name)

    try:
        # Download the file from S3 and save it locally
        s3_client.download_file(bucket_name, object_key, local_file_path)
        logging.info(f"File downloaded to {local_file_path}")
    except boto3.exceptions.S3UploadFailedError as e:
        logging.info(f"Failed to download file: {e}")
    except boto3.exceptions.Boto3Error as e:
        logging.info(f"An error occurred: {e}")
    except Exception as e:
        logging.info(f"Unexpected error: {e}")


def getDownloadRequest():
    query = """SELECT u.name, igr.id, igr.type, igr.source_file_url, igr.notify_to, igr.zip_item
                FROM invoice_gen_request igr
                JOIN users u ON u.id = igr.user_id::uuid
                WHERE igr.status = 'INVOICE GENERATED' 
                  AND igr.notify_to IS NOT NULL 
                  AND igr.mail_sent = false
                ORDER BY igr.created_at ASC
                LIMIT 1;
            """
    cursor.execute(query)
    jobs_data = cursor.fetchall()
    columns = [
        'name',
        'id',
        'type',
        'source_file_url',
        'notify_to',
        'zip_item'
    ]
    data = [dict(zip(columns, row)) for row in jobs_data]
    return data

def getFileLink(filepath):
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
               SELECT ig.s3_link
               FROM invoice_gen ig
               JOIN invoice_gen_checker igc ON ig.irn = igc.irn
               WHERE ig.s3_link IS NOT NULL;
            """)
        fetched_rows = cursor.fetchall()
        conn.commit()
        invoice_links = [row[0] for row in fetched_rows]
        return invoice_links

    except Exception as e:
        logging.info("Exception occurred in the getFileLink: " + str(e))


def download_files_in_parallel(urls, local_file_path):
    # logging.info("urls: "+str(urls))
    # with ThreadPoolExecutor(max_workers=10) as executor:
    #     futures = []
    #     for url in urls:
    #         logging.info("urls: "+str(url))
    #         if url == "INVALID":
    #             continue
    #         futures.append(executor.submit(download_file_from_s3, url, local_file_path))
    #     for future in as_completed(futures):
    #         try:
    #             future.result()  # This will raise an exception if the download failed
    #         except Exception as e:
    #             logging.info(f"An error occurred: {e}")
    for url in urls:
        if url == "INVALID":
            continue
        download_file_from_s3(url,local_file_path)

def deleteFolder(folder_path):
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        logging.info(f"Folder '{folder_path}' deleted successfully.")
    else:
        logging.info(f"Folder '{folder_path}' does not exist.")

def deleteZip(filepath):
    filepath = filepath[:-1]
    if os.path.isfile(filepath + '.zip'):
        os.remove(filepath + '.zip')
    else:
        logging.info("File doesn't exists.")


def zipHandler(local_file_path):
    output_zip_file = local_file_path[:-1]
    shutil.make_archive(output_zip_file, 'zip', local_file_path)
    deleteFolder(local_file_path)


def send_email(sender_email, recipient_email, reqid=None):

    if reqid is None:
        subject = "Invoice is generated successfully "
        content = 'Hi,<br><br>Your requested invoice is generated successfully. ' + '<br><br><br>Regards<br>Finkraft.ai'
    else:
        file_link = "https://files.finkraft.ai/"+str(reqid)
        subject = "Invoice is generated successfully"
        content = f'Hi,<br><br>Your requested invoice is generated successfully. <br><a href="{file_link}" target="_blank">Click here to download</a>  <br><br><br>Regards<br>Finkraft.ai'

    url = "https://api.sendgrid.com/v3/mail/send"

    headers = {
        "Authorization": f"Bearer {SENDGRID_API_KEY}",
        "Content-Type": "application/json"
    }
    recipient_email_list = [{"email": email} for email in recipient_email]

    # Prepare data for the email
    data = {
        "personalizations": [
            {
                "to": recipient_email_list,
                "subject": subject
            }
        ],
        "from": {"email": sender_email},
        "content": [
            {
                "type": "text/html",
                "value": content
            }
        ]
    }


    try:
        # Retry sending email up to 3 times with a delay of 5 seconds between retries
        for _ in range(3):
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 202:
                logging.info("Email sent successfully!")
                break
            time.sleep(5)
    except Exception as e:
        logging.info("Error sending email: ", e)


def updateEmailStatus(id):
    query = "update invoice_gen_request set mail_sent=true where id='" + str(id) + "'"
    cursor.execute(query)
    conn.commit()


def updateJobStatus(status, reqid, file_link=None):
    if file_link is not None:
        query = "update invoice_gen_request set status='" + str(status) + "',file_link='" + str(
            file_link) + "' where id='" + str(reqid) + "'"
        cursor.execute(query)
        conn.commit()
    else:
        query = "update invoice_gen_request set status='" + str(status) + "' where id='" + str(reqid) + "'"
        cursor.execute(query)
        conn.commit()

def upload_file_to_s3(file_name, bucket_name, object_name=None):
    """Upload a file to an S3 bucket and return the S3 URL"""
    if object_name is None:
        object_name = file_name
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{object_name}"
        logging.info(f"File uploaded successfully. URL: {s3_url}")
        return True, s3_url
    except Exception as e:
        logging.error("Exception occurred in the upload_file_to_s3: " + str(e))
        return False, None

def downloadFile(url):
    filename = url.split('/')[-1]
    if not filename.endswith('.csv'):
        return None
    response = requests.get(url)
    for i in range(1, 3, 1):
        if response.status_code == 200:
            local_file_name = 'downloader_temp/' + filename
            with open(local_file_name, 'wb') as file:
                file.write(response.content)
            logging.info("Writing to the file is completed")
            return local_file_name
        time.sleep(2)
    return None


if __name__ == '__main__':

    try:
        logging.info("===================================")
        me = singleton.SingleInstance()
        jobs = getDownloadRequest()
        if jobs is not None and len(jobs) != 0:
            for job in jobs:
                logging.info("Job Details: " + str(job))
                reqid = job['id']
                zip_item = job['zip_item']
                source_file_url = job['source_file_url']
                notify_to = job['notify_to']
                recipient_email = []
                recipient_email.append(notify_to)
                # recipient_email.append("komalkant@kgrp.in")
                local_file_path = 'downloader_temp/' + str(reqid) + "/"
                if not os.path.exists(local_file_path):
                    os.makedirs(local_file_path)
                    logging.info(f"Folder '{local_file_path}' created.")
                else:
                    logging.info(f"Folder '{local_file_path}' already exists.")

                if zip_item == False:
                    if notify_to is not None:
                        send_email('info@finkraft.ai', recipient_email)
                        updateEmailStatus(reqid)
                        updateJobStatus("COMPLETED", reqid)
                else:
                    if notify_to is not None:
                        local_file_path_csv = downloadFile(source_file_url)
                        if local_file_path_csv is not None:
                            irn_invoice_link=getFileLink(local_file_path_csv)
                            download_files_in_parallel(irn_invoice_link, local_file_path)
                            zipHandler(local_file_path)
                            file_name = local_file_path[:-1] + ".zip"
                            bucket_name = s3_bucket_name
                            object_name = s3_bucket_files_prefix + str(reqid) + ".zip"
                            # Upload the file to S3
                            status,s3_url=upload_file_to_s3(file_name, bucket_name, object_name)
                            if status:
                                logging.info(f"File uploaded to S3 bucket: {bucket_name}")
                                send_email('info@finkraft.ai', recipient_email, reqid)
                                updateEmailStatus(reqid)
                                updateJobStatus("COMPLETED", reqid, s3_url)
                            else:
                                updateJobStatus("FAILED", reqid)
                                logging.info("Failed to upload file to S3")
                            deleteZip(local_file_path)
                        else:
                            updateJobStatus( "FAILED",reqid)
                            logging.info("Error while sending mail")
        else:
            logging.info("No job")
        logging.info("===================================")
    except Exception as e:
        logging.info("Exception occurred in the main: " + str(e))
    finally:
        logging.info("Closing the DB connection")
        cursor.close()
        conn.close()
