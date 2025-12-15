import asyncio
import json
import logging
import os
import websockets
from dotenv import load_dotenv
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
load_dotenv()
API_KEY = os.getenv("AISSTREAM_API_KEY")
KAFKA_BROKER = os.getenv("REDPANDA_HOST", "localhost:9092")
AIS_WEBSOCKET_URI = "wss://stream.aisstream.io/v0/stream"
KAFKA_TOPIC = "ais-raw"
BOUNDING_BOX = [[48.88, -1.92], [53.45, 5.82]]

async def stream_ais_data(producer):
    subscribe_message = {
        "APIKey": API_KEY,
        "BoundingBoxes": [BOUNDING_BOX],
        "FilterMessageTypes": ["PositionReport", "StandardClassBPositionReport"]
    }
    async with websockets.connect(AIS_WEBSOCKET_URI) as websocket:
        await websocket.send(json.dumps(subscribe_message))
        logging.info(f"Forwarding to '{KAFKA_TOPIC}'")
        async for message_json in websocket:
            message = json.loads(message_json)
            mmsi = message.get('MetaData', {}).get('MMSI')
            key = str(mmsi).encode('utf-8') if mmsi else None
            await asyncio.to_thread(producer.send, KAFKA_TOPIC, key=key, value=message)

async def main():
    if not API_KEY:
        logging.error("AISSTREAM_API_KEY not found.")
        return
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    while True:
        try:
            await stream_ais_data(producer)
        except Exception as e:
            logging.warning(f"Connection error: {e}.")
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        logging.info("Starting...")
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down.")