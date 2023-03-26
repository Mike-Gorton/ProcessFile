# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import os
import json
import pathlib
import psycopg2
import configparser
from IoT_Logger import api_logger

def get_configuration():
    global host
    config = configparser.ConfigParser()
    config.read('IoT.ini')
    host = config['configuration']['filelocation']

    BASE_DIR = pathlib.Path(__file__).parent.resolve()
    data_dir = f"{BASE_DIR}/files"

def open_connection():
    conn = None
    try:
        # connect to the PostgresSQL server
        api_logger.info('Connecting to the PostgresSQL database...')
        # connection establishment
        conn = psycopg2.connect(database="pi", user='pi', password='pi', host='192.168.1.44', port='5432')
        cur = conn.cursor()
        cur.execute('SELECT version()')
        api_logger.info('CONNECTION OPEN:PostgresSQL database version:')
        # display the PostgresSQL database server version
        db_version = cur.fetchone()
        api_logger.info(db_version)

    except (Exception, psycopg2.DatabaseError) as error:
        api_logger.error(error)

    return conn


def close_connection(conn):
    try:
        conn.close()
        api_logger.info('Closed connection to the PostgresSQL database')
    except (Exception, psycopg2.DatabaseError) as error:
        api_logger.error(error)
    finally:
        if conn is not None:
            conn.close()
            api_logger.info('Database connection closed.')

def process_file():

    conn = open_connection()
    api_logger.info('Directory to read ' + host)
    for x in os.listdir(host):
        print(x)
        if os.path.splitext(x)[1] == ".txt" and x.startswith("GHQ"):
            fullname = os.path.join(host, x)
            with open(f"{fullname}", "r") as file:
                content = file.readlines()
                for line in content:
                    line = line.strip('\n')
                    api_logger.info(line)
                    try:
                        json_data = json.loads(line)
                    except json.decoder.JSONDecodeError:
                        api_logger.info('Decode Error on file: ', x)
                        break
                    cursor = conn.cursor()
                    sql_insert(cursor, json_data)
                    conn.commit()
            file.close()
            os.remove(fullname)
    close_connection(conn)
def sql_insert(cursor, dictionary):

    job = dictionary['job']
    host = dictionary['host']
    timestamp = dictionary['timestamp']
    status = dictionary['status']
    sub_job = dictionary['sub_job']

    if float(dictionary['temperature']):
        temperature = float((dictionary['temperature']))
    else:
        temperature = float('0.00')

    sql = '''
    INSERT INTO device_log (
    devlog_ip, devlog_job, devlog_timestamp, devlog_status, devlog_subjob, devlog_temprature)
    VALUES( %s,%s,%s,%s,%s,%s) '''

    api_logger.info('Data Dump' + dictionary["job"] + dictionary["host"] + dictionary["timestamp"] + dictionary["status"] + dictionary["sub_job"] +
          dictionary["temperature"])
    cursor.execute(sql, (str(host), str(job), str(timestamp), str(status), str(sub_job), str(temperature)))

if __name__ == '__main__':
    global host
    get_configuration()
    process_file()
