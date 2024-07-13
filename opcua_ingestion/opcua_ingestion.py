from opcua import Client
import time
import psycopg2
from psycopg2.extras import execute_values

# OPC UA settings
opcua_url = "opc.tcp://192.168.0.1"
host = "4840"
node_id = "ns=2;i=2"  # Example node ID

# Database connection
conn = psycopg2.connect(f"dbname=datadb user=user password=password host=localhost port=5432")
cursor = conn.cursor()

# Create hypertable if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS opcua_data (
        time TIMESTAMPTZ NOT NULL,
        data DOUBLE PRECISION
    );
    SELECT create_hypertable('opcua_data', 'time', if_not_exists => TRUE);
""")
conn.commit()

# OPC UA client
client = Client(opcua_url)

def read_opcua_data():
    client.connect()
    while True:
        try:
            node = client.get_node(node_id)
            data = node.get_value()
            cursor.execute("INSERT INTO opcua_data (time, data) VALUES (NOW(), %s)", (data,))
            conn.commit()
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(5)
    client.disconnect()

if __name__ == "__main__":
    read_opcua_data()
