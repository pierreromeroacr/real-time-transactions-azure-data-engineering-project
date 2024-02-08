#import libraries
import requests
import uuid
import pytz
from datetime import datetime
import random
import json
import time
from azure.eventhub import EventHubProducerClient, EventData

# Set up the connection to Azure Event Hubs
CONNECTION_STR = "Endpoint=sb://evh-realtimetransactions.servicebus.windows.net/;SharedAccessKeyName=MyPolicy;SharedAccessKey=2spIaEJMpV/T24nnKgxicGXQSbYQcpTlz+AEhE20ni8=;EntityPath=evh-entity-realtime"
EVENTHUB_NAME = "evh-entity-realtime"

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENTHUB_NAME
)

# Get username
url_names = "https://randomuser.me/api/?inc=name&results=5000"
response = requests.get(url_names)
array_names = []

if response.status_code == 200:
    data_names = response.json()
    for result in data_names['results'] :
        names = result['name']['first'] +' '+ result['name']['last']  
        array_names.append(names)
else:
    print('Error getting names:', response.status_code)

# Function to simulate transactions
tz_lima = pytz.timezone('America/Lima')

def simulate_transaction(producer):
    for _ in range(100000):

        event_data_batch = producer.create_batch()

        id_registro = str(uuid.uuid4())
        date = datetime.now(tz_lima)
        client = random.choice(array_names)
        transaction_type = random.choice(["retiro", "deposito"])
        amount = round(random.uniform(10, 10000), 2)

        transaction = {
            "id" : id_registro,
            "date" : date.strftime("%Y-%m-%d %H:%M:%S"),
            "client" : client,
            "transaction_type" : transaction_type,
            "amount" : amount
        }
        print(transaction)

        to_json = json.dumps(transaction)
        event_data_batch.add(EventData(to_json))
        producer.send_batch(event_data_batch)
        event_data_batch = ""
        time.sleep(2)

simulate_transaction(producer)

