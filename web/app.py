from flask import Flask, jsonify, request
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# Database connection
conn = psycopg2.connect("dbname=datadb user=user password=password")
cursor = conn.cursor(cursor_factory=RealDictCursor)

@app.route('/')
def index():
    return "Welcome to the Central Data Integration Platform"

@app.route('/data/mqtt', methods=['GET'])
def get_mqtt_data():
    cursor.execute("SELECT * FROM mqtt_data ORDER BY time DESC LIMIT 100")
    results = cursor.fetchall()
    return jsonify(results)

@app.route('/data/modbus', methods=['GET'])
def get_modbus_data():
    cursor.execute("SELECT * FROM modbus_data ORDER BY time DESC LIMIT 100")
    results = cursor.fetchall()
    return jsonify(results)

@app.route('/data/opcua', methods=['GET'])
def get_opcua_data():
    cursor.execute("SELECT * FROM opcua_data ORDER BY time DESC LIMIT 100")
    results = cursor.fetchall()
    return jsonify(results)

@app.route('/data/tcpip', methods=['GET'])
def get_tcpip_data():
    cursor.execute("SELECT * FROM tcpip_data ORDER BY time DESC LIMIT 100")
    results = cursor.fetchall()
    return jsonify(results)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
