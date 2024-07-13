import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import execute_values

# MQTT Settings
broker = "broker.hivemq.com"
topic = "sensor/data"

# Database connection
conn = psycopg2.connect(f"dbname=datadb user=user password=password host=localhost port=5432")
cursor = conn.cursor()

# Create hypertable if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS sensor_data (
        time TIMESTAMPTZ NOT NULL,
        data DOUBLE PRECISION
    );
    SELECT create_hypertable('sensor_data', 'time', if_not_exists => TRUE);
""")
conn.commit()

def on_message(client, userdata, message):
    data = float(message.payload.decode("utf-8"))
    cursor.execute("INSERT INTO sensor_data (time, data) VALUES (NOW(), %s)", (data,))
    conn.commit()

client = mqtt.Client()
client.connect(broker)
client.subscribe(topic)
client.on_message = on_message

client.loop_forever()
