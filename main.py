# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import os
import json
import pathlib
import psycopg2
from psycopg2.extras import Json

BASE_DIR = pathlib.Path(__file__).parent.resolve()
data_dir = f"{BASE_DIR}/files"


def open_connection():
    conn = None
    try:
        # connect to the PostgresSQL server
        print('Connecting to the PostgresSQL database...')
        # connection establishment
        conn = psycopg2.connect(database="pi", user='pi', password='pi', host='192.168.1.44', port='5432')
        cur = conn.cursor()
        cur.execute('SELECT version()')
        print('CONNECTION OPEN:PostgresSQL database version:')
        # display the PostgresSQL database server version
        db_version = cur.fetchone()
        print(db_version)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return conn


def close_connection(conn):
    try:
        conn.close()
        print('Closed connection to the PostgresSQL database')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def process_file():

    conn = open_connection()
    for x in os.listdir(data_dir):
        print(x)
        if os.path.splitext(x)[1] == ".txt" and x.startswith("GHQ"):
            fullname = os.path.join(data_dir, x)
            with open(f"{fullname}", "r") as file:
                content = file.readlines()
                for line in content:
                    line = line.strip('\n')
                    print(line)
                    json_data = json.loads(line)

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
    temperature = to_float((dictionary['temperature'][:4]))

    sql = '''
    INSERT INTO device_log (
    devlog_ip, devlog_job, devlog_timestamp, devlog_status, devlog_subjob, devlog_temprature)
    VALUES( %s,%s,%s,%s,%s,%s) '''

    print(dictionary["job"], dictionary["host"], dictionary["timestamp"], dictionary["status"], dictionary["sub_job"],
          dictionary["temperature"])
    cursor.execute(sql, (str(host), str(job), str(timestamp), str(status), str(sub_job), str(temperature)))


def to_float(s):
    n = float(s)
    return n


if __name__ == '__main__':
    process_file()
