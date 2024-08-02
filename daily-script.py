from pymongo import MongoClient
import requests
import time
import concurrent.futures
from tendo import singleton
from dotenv import load_dotenv
load_dotenv()
import os
# Use the provided MongoDB connection string
MONGO_URL = os.getenv("MONGO_URL")
QUERY_LIMIT = 800
API_LIMIT = MAX_WORKERS = 5

DEFAULT_PORT = 8000
client = MongoClient(MONGO_URL)
def getIrn():
    try:
        irn = []
        pipeline = [
            {'$match': {'s3_url': {'$exists': False}}},
            {'$sort': {'IrnDt': -1}},
            {'$limit': QUERY_LIMIT}
        ]
        db = client['gstservice']
        collection = db['irn']
        results = collection.aggregate(pipeline)
        for result in results:
            irn.append(result['Irn'])
        return irn
    finally:
        # Close the connection
        client.close()
def genrateInvoiceAPICall(index, irn):
    PORT = DEFAULT_PORT + (index % API_LIMIT)
    url = "http://localhost:" + str(PORT) + "/generate_single/" + irn
    print("Index: " + str(index) + " URL: " + str(url))
    for i in range(1, 2, 1):
        response = requests.post(url)
        if response.status_code == 200:
            data = response.json()
            if 'status' in data and data['status'] == True:
                break
        time.sleep(0.100)

def generateInvoices(data):
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit tasks to the executor for each item in data
            futures = [executor.submit(genrateInvoiceAPICall, index, item) for index, item in enumerate(data)]
            # Optionally wait for all futures to complete
            concurrent.futures.wait(futures)
    except Exception as e:
        print("Exception happen in the generateInvoices: " + str(e))

if __name__ == '__main__':
    try:
        me = singleton.SingleInstance()
        irn = getIrn()
        print("No. of irns: " + str(len(irn)))
        generateInvoices(irn)
        print("Invoice generated successfully ")
    except Exception as e:
        print("Exception happened in the main: "+str(e))
