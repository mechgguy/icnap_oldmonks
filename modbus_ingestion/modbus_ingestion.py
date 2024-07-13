import time
from pymodbus.client.sync import ModbusTcpClient
import psycopg2
from psycopg2.extras import execute_values

# Modbus settings
modbus_host = "localhost"
modbus_port = 5432
# change for modbus 
register_address = 100  # Example register address

# Database connection
conn = psycopg2.connect(f"dbname=datadb user=user password=password host={modbus_host} port={modbus_port}")
cursor = conn.cursor()

# Create hypertable if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS modbus_data (
        time TIMESTAMPTZ NOT NULL,
        data DOUBLE PRECISION
    );
    SELECT create_hypertable('modbus_data', 'time', if_not_exists => TRUE);
""")
conn.commit()

# Modbus client
client = ModbusTcpClient(modbus_host, port=modbus_port)

def read_modbus_data():
    while True:
        try:
            # Read holding register
            result = client.read_holding_registers(register_address, 1)
            if result.isError():
                print("Error reading Modbus data")
            else:
                data = result.registers[0]
                cursor.execute("INSERT INTO modbus_data (time, data) VALUES (NOW(), %s)", (data,))
                conn.commit()
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(5)

if __name__ == "__main__":
    read_modbus_data()
