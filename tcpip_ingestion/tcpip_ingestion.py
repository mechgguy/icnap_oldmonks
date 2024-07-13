import socket
import psycopg2
from psycopg2.extras import execute_values

# TCP/IP settings
tcpip_host = "192.168.1.100"
tcpip_port = 65432

# Database connection
conn = psycopg2.connect(f"dbname=datadb user=user password=password host=localhost port=65432")
cursor = conn.cursor()

# Create hypertable if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS tcpip_data (
        time TIMESTAMPTZ NOT NULL,
        data DOUBLE PRECISION
    );
    SELECT create_hypertable('tcpip_data', 'time', if_not_exists => TRUE);
""")
conn.commit()

# TCP/IP client
def read_tcpip_data():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((tcpip_host, tcpip_port))
        while True:
            try:
                data = s.recv(1024)
                if not data:
                    break
                data = float(data.decode("utf-8"))
                cursor.execute("INSERT INTO tcpip_data (time, data) VALUES (NOW(), %s)", (data,))
                conn.commit()
            except Exception as e:
                print(f"Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    read_tcpip_data()
