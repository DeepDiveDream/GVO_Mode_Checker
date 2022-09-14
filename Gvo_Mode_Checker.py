#!/usr/bin/python3
import sys
import psycopg2
from psycopg2 import Error
import pyodbc
import json
from argparse import ArgumentParser
from configparser import ConfigParser
from datetime import datetime
import decimal


def connect_to_mssql(dbase, login, password, server):
    try:
        # conn_str = 'DRIVER={SQL Server}; SERVER=' + server + ';DATABASE=' + dbase + ';UID=' + login + ';PWD=' + password
        conn_str = 'Driver={ODBC Driver 18 for SQL Server}; SERVER=' + server + ';DATABASE=' + dbase + ';UID=' \
                   + login + '; PWD=' + password + ';TrustServerCertificate=yes;'
        conn = pyodbc.connect(conn_str)
        print(f"{currentScript} {datetime.now()} Соединение с БД {dbase}, сервер {server} успешно!")
        return conn
    except (Exception, Error) as connect_error:
        print(f"{currentScript} {datetime.now()} Ошибка соеденения с БД {dbase} сервер {server}: {connect_error}")



def connect_to_postgre(dbase, login, password, host, port):
    try:
        conn = psycopg2.connect(
            database=dbase,
            user=login,
            password=password,
            port=port,
            host=host)
        print(f"{currentScript} {datetime.now()} Соединение с БД {dbase}, сервер {host} успешно!")
        return conn
    except (Exception, Error) as connect_error:
        print(f"{currentScript} {datetime.now()} Ошибка соеденения с БД {dbase} сервер {host}: {connect_error}")


def fields(cursor):
    results = {}
    column = 0

    for d in cursor.description:
        results[d[0]] = column
        column += 1

    return results


if __name__ == '__main__':

    currentScript = '[Gvo_Mode_Checker]'

    parser = ArgumentParser()
    parser.add_argument('configPath', type=str, help='Path to config file', default='config.json', nargs='?')
    args = parser.parse_args()
    config_path = args.configPath

    with open(config_path, 'r') as f:
        config_data = json.load(f)
        ini_file_path = config_data['ini_file_path']

    config_ini = ConfigParser()
    config_ini.read(ini_file_path)

    postgre_user = config_ini.get('common', 'pguser')
    postgre_pass = config_ini.get('common', 'pgpassword')
    postgre_host = config_ini.get('common', 'pghost')
    postgre_port = config_ini.get('common', 'pgport')
    postgre_database = config_ini.get('common', 'pgdb')

    postgre_elsec_conn = connect_to_postgre(postgre_database, postgre_user, postgre_pass, postgre_host, postgre_port)

    if not postgre_elsec_conn:
        sys.exit(1)

    postgre_elsec_cursor = postgre_elsec_conn.cursor()

    postgre_elsec_cursor.execute(f"SELECT * FROM event_type where name = \'gvoModeOn\'")
    event_gvo_on = postgre_elsec_cursor.fetchone()[0]

    postgre_elsec_cursor.execute(f"SELECT * FROM event_type where name = \'gvoModeOff\'")
    event_gvo_off = postgre_elsec_cursor.fetchone()[0]

    postgre_elsec_cursor.execute(f"SELECT * FROM event_source_params('telescada')")
    fields_map_params = fields(postgre_elsec_cursor)
    params = postgre_elsec_cursor.fetchone()

    if params is None:
        print(f'{currentScript} {datetime.now()} Не заполнены параметры для источника telescada')
        sys.exit(2)

    event_source = params[fields_map_params['id']]
    param_dict = params[fields_map_params['params']]

    if len(param_dict) == 0:
        print(f'{currentScript} {datetime.now()} Не заполнены параметры для источника telescada')
        sys.exit(2)

    ip_address = param_dict["ip_scada_events"]
    login = param_dict["login_scada_events"]
    password = param_dict["password_scada_events"]
    dbase = param_dict["db_scada_events"]

    scada_conn = connect_to_mssql(dbase, login, password, ip_address)

    if not scada_conn:
        sys.exit(1)

    scada_cursor = scada_conn.cursor()

    scada_cursor.execute(f"Select TOP 1 code, dt, obj, user_ FROM [{dbase}].[dbo].[EvSwitchover] where code = 8012 Order By dt DESC")
    field_map_scada = fields(scada_cursor)

    gvo_on_date = 0
    gvo_off_date = 0

    res_gvo_on = scada_cursor.fetchall()

    if len(res_gvo_on) > 0:
        gvo_on_date = res_gvo_on[0][field_map_scada['dt']]

    scada_cursor.execute(f"Select TOP 1 code, dt, obj, user_ FROM [{dbase}].[dbo].[EvSwitchover] where code = 8005 Order By dt DESC")

    res_gvo_off = scada_cursor.fetchall()

    if len(res_gvo_off) > 0:
        gvo_off_date = res_gvo_off[0][field_map_scada['dt']]

    current_gvo_state = 0
    current_gvo_event_date = 0

    if gvo_on_date >= gvo_off_date:
        current_gvo_state = event_gvo_on
        current_gvo_event_date = gvo_on_date
    else:
        current_gvo_state = event_gvo_off
        current_gvo_event_date = gvo_off_date

    postgre_elsec_cursor.execute(f"SELECT id, data ->> 'event_value' as gvoMode, data ->> 'event_unix_dt' as dt "
                                 f"FROM event_source_data where source = {event_source} "
                                 f"and data ->> 'event_type'='GVO_MODE'")
    field_map_event = fields(postgre_elsec_cursor)
    res = postgre_elsec_cursor.fetchall()

    need_create_gvo_event = True

    if len(res) > 0:
        last_gvo = res[0]
        gvo_id = int(last_gvo[field_map_event['id']])
        last_gvo_unix_dt = decimal.Decimal(last_gvo[field_map_event['dt']])
        last_gvo_mode = int(last_gvo[field_map_event['gvomode']])

        if last_gvo_mode != current_gvo_state or current_gvo_event_date > last_gvo_unix_dt:
            need_create_gvo_event = True
        else:
            need_create_gvo_event = False

    if need_create_gvo_event:
        json_data = {
            "event_unix_dt": f"{current_gvo_event_date}",
            "event_type": "GVO_MODE",
            "event_value" : current_gvo_state
        }
        data = json.dumps(json_data)

        if len(res) > 0:
            query = f"UPDATE event_source_data SET " \
                f"created = '{datetime.now()}', " \
                f"source = {event_source}, " \
                f"data = '{data}' " \
                f"WHERE id = {gvo_id}"
        else:
            query = f"INSERT INTO  event_source_data (created, source, data)" \
                f" VALUES ('{datetime.now()}',{event_source},'{data}')"

        postgre_elsec_cursor.execute(query)
        postgre_elsec_conn.commit()

    postgre_elsec_cursor.close()
    postgre_elsec_conn.close()
    scada_cursor.close()
    scada_conn.close()
