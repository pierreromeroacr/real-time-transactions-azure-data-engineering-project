# An example to show receiving events from an Event Hub.
import os
from azure.eventhub import EventHubConsumerClient

CONNECTION_STR = "Endpoint=sb://evh-realtimetransactions.servicebus.windows.net/;SharedAccessKeyName=MyPolicy;SharedAccessKey=2spIaEJMpV/T24nnKgxicGXQSbYQcpTlz+AEhE20ni8=;EntityPath=evh-entity-realtime"
EVENTHUB_NAME = "evh-entity-realtime"


def on_event(partition_context, event):
    # Put your code here.
    print("Received event from partition: {}.".format(partition_context.partition_id))
    print(event)

if __name__ == '__main__':
    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group='$Default',
        eventhub_name=EVENTHUB_NAME,
    )

    try:
        with consumer_client:
            consumer_client.receive(
                on_event=on_event,
                starting_position="-1",  # "-1" is from the beginning of the partition.
            )
    except KeyboardInterrupt:
        print('Stopped receiving.')