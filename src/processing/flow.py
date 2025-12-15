import os
import h3
import logging
import psycopg2
from datetime import timedelta, datetime
from quixstreams import Application

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
BROKER = os.getenv("REDPANDA_HOST", "localhost:9_092")
DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_NAME = os.getenv("DB_NAME", "portpulse")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "password123")
TOPIC = "ais-raw"
H3_RESOLUTION = 7

class PostgresSink:
    def __init__(self):
        self.conn = None

    def _connect(self):
        try:
            return psycopg2.connect(
                host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
            )
        except psycopg2.OperationalError as e:
            logging.error(f"Connection failed: {e}")
            raise

    def write(self, value, key, timestamp, headers):
        if not self.conn or self.conn.closed:
            self.conn = self._connect()
        cursor = self.conn.cursor()
        try:
            payload = value["value"]
            ts = datetime.fromtimestamp(value["end"] / 1000.0)
            query = """
                INSERT INTO ship_counts (time, hex_id, count, lat, lon, avg_speed) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (time, hex_id) 
                DO UPDATE SET 
                    count = EXCLUDED.count,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    avg_speed = EXCLUDED.avg_speed;
            """
            cursor.execute(query, (
                ts, key, payload["count"], payload["lat"], payload["lon"], payload["avg_speed"]
            ))
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error: {e}.")
            self.conn.rollback()
            raise
        finally:
            cursor.close()

def enrich_with_h3(message):
    try:
        content = message["Message"][message["MessageType"]]
        lat, lon = content["Latitude"], content["Longitude"]
        hex_id = h3.latlng_to_cell(lat, lon, H3_RESOLUTION)
        center_lat, center_lon = h3.cell_to_latlng(hex_id)
        return {
            "hex_id": hex_id,
            "lat": center_lat,
            "lon": center_lon,
            "speed": float(content.get("Sog", 0.0) or 0.0)
        }
    except (KeyError, TypeError):
        return None
    
def initializer(current):
    return {
        "count": 1,
        "total_speed": current["speed"]
    }

def reducer(acc, current):
    acc["count"] += 1
    acc["total_speed"] += current["speed"]
    return acc

def format_final_result(value, key, timestamp, headers):
    window_result = value["value"]
    count = window_result["count"]
    total_speed = window_result["total_speed"]
    return {
        "value": {
            "count": count,
            "avg_speed": total_speed / count if count > 0 else 0.0,
            "lat": h3.cell_to_latlng(key)[0],
            "lon": h3.cell_to_latlng(key)[1]
        },
        "start": value["start"],
        "end": value["end"]
    }

def main():
    app = Application(
        broker_address=BROKER,
        consumer_group="portpulse_processor", 
        auto_offset_reset="latest",
    )

    input_topic = app.topic(TOPIC, value_deserializer="json")
    sdf = app.dataframe(input_topic)
    sdf = sdf.apply(enrich_with_h3).filter(lambda row: row is not None)
    sdf = (
        sdf.group_by("hex_id")
        .tumbling_window(timedelta(seconds=10))
        .reduce(reducer=reducer, initializer=initializer)
        .current()
        .apply(format_final_result, metadata=True)
    )
    pg_sink = PostgresSink()
    sdf = sdf.update(pg_sink.write, metadata=True)
    logging.info("Starting...")
    app.run()

if __name__ == "__main__":
    main()