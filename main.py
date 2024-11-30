import base64
import traceback
import paho.mqtt.client as mqtt
import json
import pyodbc

HOST = "eu1.cloud.thethings.network"
PORT = 1883
USERNAME = "project-software-engineering@ttn"
PASSWORD = "NNSXS.DTT4HTNBXEQDZ4QYU6SG73Q2OXCERCZ6574RVXI.CQE6IG6FYNJOO2MOFMXZVWZE4GXTCC2YXNQNFDLQL4APZMWU6ZGA"
TOPIC = "#"
OUTPUT_FILE = "parsed_mqtt_messages.json"

results = []


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to the broker successfully!")
        client.subscribe(TOPIC)
    else:
        print(f"Failed to connect, return code {rc}")



def on_message(client, userdata, message):
    global results
    try:
        payload = message.payload.decode()
        data = json.loads(payload)
        result = parse_message(data)

        results.append(result)
        print(f"Parsed message: {result}")


        gateway = result["gateway"]
        sensor = result["sensor"]
        message = result["message"]

        # Connection details
        server = 'WIN-Q6CTI51L8BB'  # server_name
        database = 'WeatherDB'  # datbase_name
        driver = '{ODBC Driver 17 for SQL Server}'  # driver
        # Connect to the database
        connection = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

        )
        cursor = connection.cursor()

        create_table_query = '''
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Gateway' AND schema_id = SCHEMA_ID('w'))
        BEGIN
            CREATE TABLE [WeatherDB].[w].[Gateway] (
                gateway_id NVARCHAR(50) NOT NULL PRIMARY KEY,
                time_ NVARCHAR(50) NOT NULL,
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                altitude INT NOT NULL,
                rssi INT NOT NULL,
                snr FLOAT NULL,
            );

        END

        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Sensor' AND schema_id = SCHEMA_ID('w'))
        BEGIN
            CREATE TABLE [WeatherDB].[w].[Sensor] (
                sensor_id NVARCHAR(50) PRIMARY KEY,
                sensor_type NVARCHAR(50) NOT NULL,
                place NVARCHAR(50) NOT NULL,
                bat_stat INT NULL,
                bat_volt FLOAT NULL
            );
        END

        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Message' AND schema_id = SCHEMA_ID('w'))
        BEGIN
            CREATE TABLE [WeatherDB].[w].[Message] (
                message_id INT IDENTITY(1,1) PRIMARY KEY,
                sensor_id NVARCHAR(50) NOT NULL,
                gateway_id NVARCHAR(50) NOT NULL,
                time_ NVARCHAR(50) NOT NULL,
                ext_temp FLOAT NOT NULL,
                int_temp FLOAT NULL,
                pressure FLOAT NULL,
                light INT NULL,
                humidity FLOAT NOT NULL,
                FOREIGN KEY (sensor_id) REFERENCES [WeatherDB].[w].[Sensor](sensor_id) ON DELETE NO ACTION,
                FOREIGN KEY (gateway_id) REFERENCES [WeatherDB].[w].[Gateway](gateway_id) ON DELETE NO ACTION
            );
        END
        '''

        insert_g_query = '''
        IF NOT EXISTS (SELECT 1 FROM [WeatherDB].[w].[Gateway] WHERE gateway_id = ?)
            BEGIN
                INSERT INTO [WeatherDB].[w].[Gateway] (gateway_id, time_, latitude, longitude, altitude, rssi, snr)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            END
        '''

        insert_s_query = '''
        IF NOT EXISTS (SELECT 1 FROM [WeatherDB].[w].[Sensor] WHERE sensor_id = ?)
            BEGIN
                INSERT INTO [WeatherDB].[w].[Sensor] (sensor_id, sensor_type, place, bat_stat, bat_volt)
                VALUES (?, ?, ?, ?, ?);
            END
        '''

        insert_m_query = '''
        INSERT INTO [WeatherDB].[w].[Message] (sensor_id, gateway_id, time_, ext_temp, int_temp, pressure, light, humidity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        '''


        try:
            cursor.execute(create_table_query)
            connection.commit()
            print("Table created successfully.")
        except Exception as e:
            print(f"Error creating table: {e}")

        try:

            # Validate `rssi` and `snr` values to ensure no infinity values are passed to the database
            gateway['rssi'] = None if gateway['rssi'] in [float('inf'), float('-inf')] else gateway['rssi']
            gateway['snr'] = None if gateway['snr'] in [float('inf'), float('-inf')] else gateway['snr']

            cursor.execute(insert_g_query, (gateway['gateway_id'], gateway['gateway_id'], gateway['time'], gateway['latitude'], gateway['longitude'], gateway['altitude'], gateway['rssi'], gateway['snr']))
            cursor.execute(insert_s_query, (sensor['sensor_id'], sensor['sensor_id'], sensor['sensor_type'], sensor['place'], sensor['bat_stat'], sensor['bat_volt']))
            cursor.execute(insert_m_query, (message['sensor_id'], message['gateway_id'], message['time'], message['ext_temp'], message['int_temp'], message['pressure'], message['light'], message['humidity']))
            connection.commit()
            print("Data inserted successfully.")
        except Exception as e:
            print(f"Error inserting data: {e}")
        finally:
            cursor.close()
            connection.close()


        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=WIN-Q6CTI51L8BB;'
            'DATABASE=WeatherDB;'
            'Trusted_Connection=yes;'
        )

        cursor = conn.cursor()

        cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM [WeatherDB].[w].[Gateway] WHERE gateway_id = ?)
            BEGIN
                INSERT INTO [WeatherDB].[w].[Gateway] (gateway_id, time_, latitude, longitude, altitude, rssi, snr)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            END
        """, (gateway['gateway_id'], gateway['gateway_id'], gateway['time'], gateway['latitude'], gateway['longitude'], gateway['altitude'], gateway['rssi'], gateway['snr']))

        cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM [WeatherDB].[w].[Sensor] WHERE sensor_id = ?)
                    BEGIN
                        INSERT INTO [WeatherDB].[w].[Sensor] (sensor_id, sensor_type, place, bat_stat, bat_volt)
                        VALUES (?, ?, ?, ?, ?);
                    END
                """, (sensor['sensor_id'], sensor['sensor_id'], sensor['sensor_type'], sensor['place'], sensor['bat_stat'], sensor['bat_volt']))

        cursor.execute("""
                INSERT INTO [WeatherDB].[w].[Message] (sensor_id, gateway_id, time_, ext_temp, int_temp, pressure, light, humidity)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """, (message['sensor_id'], message['gateway_id'], message['time'], message['ext_temp'], message['int_temp'], message['pressure'], message['light'], message['humidity']))

        conn.commit()

        conn.close()

        print("Data inserted into the SQL Server database successfully!")


        save_results_to_file()
    except Exception as e:
        # Enhanced error reporting
        print("An error occurred!")
        print(f"Error message: {e}")
        print("Detailed traceback information:")
        traceback.print_exc()

def save_results_to_file():
    global results
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
    print(f"Saved {len(results)} parsed messages to {OUTPUT_FILE}")


def choose_best_gateway(rx_metadata):

    best_gateway = rx_metadata[0]['gateway_ids']['gateway_id']
    best_rssi = rx_metadata[0]['rssi']
    best_snr = rx_metadata[0]['snr']
    best_time = rx_metadata[0]['received_at']
    for gateway in rx_metadata:
        if gateway["rssi"] > best_rssi:
            best_gateway = gateway["gateway_ids"]["gateway_id"]
            best_rssi = gateway["rssi"]
            best_snr = gateway["snr"]
            best_time = gateway["received_at"]
        elif gateway["snr"] > best_snr:
            best_gateway = gateway["gateway_ids"]["gateway_id"]
            best_rssi = gateway["rssi"]
            best_snr = gateway["snr"]
            best_time = gateway["received_at"]
    result = {"gateway_id": best_gateway, "rssi": best_rssi, "snr": best_snr, "time": best_time}
    return result


def parse_message(data):

    # Decoding encoded message
    message = data['uplink_message']['frm_payload']
    encoded_bytes = message.encode('utf-8')
    encoded = base64.b64decode(encoded_bytes)
    print(encoded)


    # Append gateway
    location = data['uplink_message']['rx_metadata'][0]['location']
    if 'source' in location:
        location.popitem()  # Remove the last item
    latitude = location['latitude']
    longitude = location['longitude']
    altitude = location['altitude']
    other_info = choose_best_gateway(data['uplink_message']['rx_metadata'])
    gateway = {'gateway_id': other_info['gateway_id'], 'time' : other_info['time'], 'latitude': latitude, 'longitude': longitude, 'altitude': altitude, 'rssi': other_info['rssi'], 'snr': other_info['snr']}

    print(gateway)


    # Append sensor
    sensor_id = data['end_device_ids']['device_id']
    sensor = {'sensor_id': sensor_id}

    # Extract device ID and determine place
    if 'saxion' in sensor_id:
        sensor['place'] = 'Enschede'
    elif 'gronau' in sensor_id:
        sensor['place'] = 'Gronau'
    else:
        sensor['place'] = 'Wierden'

    if 'lht' in sensor_id:
        sensor['sensor_type'] = 'lht'
    else:
        sensor['sensor_type'] = 'mkr'


    # Printing encoded information in a proper way
    encoded_str = data['uplink_message']['frm_payload']
    decoded_data = base64.b64decode(encoded_str)


    # Appending information to the message
    light = None
    internal_temp = None
    pressure = None
    battery = None
    battery_stat = None
    time = data['uplink_message']['received_at']
    if 'lht' in data['end_device_ids']['device_id']:

        battery = ((decoded_data[0] << 8 | decoded_data[1]) & 0x3FFF) / 1000
        battery_stat = int(battery)
        external_temp = (decoded_data[2] << 8 | decoded_data[3]) / 100
        humidity = (decoded_data[4] << 8 | decoded_data[5]) / 10

        if decoded_data[6] == 5:
            light = (decoded_data[7] << 8 | decoded_data[8])
        elif decoded_data[6] == 1:
            internal_temp = (decoded_data[7] << 8 | decoded_data[8]) / 100
            internal_temp, external_temp = external_temp, internal_temp
        print(f"Battery: {battery}, Internal temperature: {internal_temp}, Humidity: {humidity}, External temperature: {external_temp}, Luminosity: {light}")

    else:

        external_temp = decoded_data[2] + decoded_data[3] / 10
        humidity = decoded_data[4]
        light = decoded_data[1]
        pressure = (decoded_data[0] / 2) + 950
        print(f"Temperature: {external_temp}, Humidity: {humidity}, Luminosity: {light}, Pressure: {pressure}")



    message = {'sensor_id': sensor['sensor_id'], 'gateway_id': gateway['gateway_id'], 'time': time, 'ext_temp': external_temp,
               'int_temp': internal_temp, 'pressure': pressure, 'light': light, 'humidity': int(humidity)}
    sensor['bat_volt'] = battery
    sensor['bat_stat'] = battery_stat

    print(sensor)
    print(message)

    return {'sensor': sensor, 'gateway': gateway, 'message': message}

def main():

    client = mqtt.Client()
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    print("Connecting to the broker...")
    client.connect(HOST, PORT, 60)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("Exiting...")
        save_results_to_file()
        client.disconnect()

if __name__ == "__main__":
    main()
