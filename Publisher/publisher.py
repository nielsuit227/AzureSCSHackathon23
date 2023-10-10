import asyncio
import io
import os
import time

import avro.schema
from avro.io import BinaryEncoder, DatumWriter
from azure.iot.device.aio import IoTHubDeviceClient
from dotenv import load_dotenv

load_dotenv()


messages_per_second = 100
total_duration_in_seconds = 1

# Define your Avro schema
avro_schema = avro.schema.parse(
    """
{
    "type": "record",
    "name": "Message",
    "fields": [
        {"name": "ts", "type": "double"}
    ]
}
"""
)


async def main():
    # Create instance of the device client using the authentication provider
    device_client = IoTHubDeviceClient.create_from_connection_string(
        os.getenv("CONNECTION_STRING")
    )

    # Connect to the IoT Hub
    await device_client.connect()
    start_time = time.time()

    # Loop through total messages
    for i in range(messages_per_second * total_duration_in_seconds):
        current_time = time.time()
        elapsed_time = current_time - start_time

        # Serialize the data to Avro format
        writer = DatumWriter(avro_schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write({"ts": time.time()}, encoder)
        avro_msg = bytes_writer.getvalue()

        # Send message
        print(f"Sending message {i}")
        await device_client.send_message(avro_msg)

        # Sleep
        sleep_time = max((i + 1) / messages_per_second - elapsed_time, 0)
        await asyncio.sleep(sleep_time)

    # Finally, disconnect
    await device_client.disconnect()


asyncio.run(main())
