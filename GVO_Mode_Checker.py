#!/usr/bin/python3
import sys
import psycopg2
from psycopg2 import Error
import pyodbc
import json
from argparse import ArgumentParser
from configparser import ConfigParser
from datetime import datetime, timedelta
import decimal


def connect_to_mssql(dbase, login, password, server, event_source):
    try:
        # conn_str = 'DRIVER={SQL Server}; SERVER=' + server + ';DATABASE=' + dbase + ';UID=' + login + ';PWD=' + password
        conn_str = 'Driver={ODBC Driver 18 for SQL Server}; SERVER=' + server + ';DATABASE=' + dbase + ';UID=' \
                   + login + '; PWD=' + password + ';TrustServerCertificate=yes;'
        conn = pyodbc.connect(conn_str)

        msg = f"{datetime.now()} | {currentScript} | source: {event_source} |" \
            f" Соединение с БД {dbase}, сервер {server} успешно!"
        # print(msg)
        # write_log(log_file_name, msg)
        write_connection_log(event_source, currentScript, server, dbase, True, msg)

        return conn

    except (Exception, Error) as connect_error:
        local_error = f"{datetime.now()} | {currentScript} | source: {event_source} | " \
            f"Ошибка соединения с БД {dbase}, сервер {server}: {connect_error}"
        print(local_error)
        write_log(log_file_name, local_error)
        write_connection_log(event_source, currentScript, server, dbase, False, local_error)


def connect_to_postgre(dbase, login, password, host, port):
    try:
        conn = psycopg2.connect(
            database=dbase,
            user=login,
            password=password,
            port=port,
            host=host)

        # msg = f"{datetime.now()} | {currentScript} | source: {event_source_main} |" \
        #     f" Соединение с БД {dbase}, сервер {host} успешно!"
        # print(msg)
        # write_log(log_file_name, msg)

        return conn

    except (Exception, Error) as connect_error:
        local_error = f'{datetime.now()} | {currentScript} | source: {event_source_main} | ' \
            f'Ошибка соединения с БД {dbase}, сервер {host}: {connect_error}'
        print(local_error)
        write_log(log_file_name, local_error)


def write_connection_log(source_id, script_name, host, db_name, result, message):
    message = message.replace("'", " ")
    query = f"INSERT INTO  connection_log (source_id, script_name,host,db_name, date_time, result, message)" \
        f" VALUES ({source_id}, '{script_name}' ,'{host}','{db_name}','{datetime.now()}', {result},'{message}')"
    postgre_cursor.execute(query)
    postgre_conn.commit()


def delete_old_connection_log():
    old_date = datetime.now() - timedelta(days=oldest_log_delta)

    query = f"DELETE FROM  connection_log WHERE date_time < timestamp '{old_date}'"
    postgre_cursor.execute(query)
    postgre_conn.commit()


def write_log(file_name, str_log):
    with open(file_name, 'a') as logfile:
        logfile.write(str_log + '\n')
        logfile.close()


def fields(cursor):
    results = {}
    column = 0

    for d in cursor.description:
        results[d[0]] = column
        column += 1

    return results


def check_event_source(event_source, param_dict):

    ip_address = param_dict["ip_scada_events"]
    login = param_dict["login_scada_events"]
    password = param_dict["password_scada_events"]
    dbase = param_dict["db_scada_events"]

    scada_conn = connect_to_mssql(dbase, login, password, ip_address, event_source)

    if not scada_conn:
        return

    scada_cursor = scada_conn.cursor()

    query = f"Select TOP 1 code, dt, obj, user_ FROM [{dbase}].[dbo].[{scada_event_table}] " \
            f"where code = 8012 Order By dt DESC"

    scada_cursor.execute(query)
    field_map_scada = fields(scada_cursor)

    gvo_on_date = -1
    gvo_off_date = -1

    res_gvo_on = scada_cursor.fetchall()

    if len(res_gvo_on) > 0:
        gvo_on_date = res_gvo_on[0][field_map_scada['dt']]

    query = f"Select TOP 1 code, dt, obj, user_ FROM [{dbase}].[dbo].[{scada_event_table}] " \
            f"where code = 8005 Order By dt DESC"

    scada_cursor.execute(query)

    res_gvo_off = scada_cursor.fetchall()

    if len(res_gvo_off) > 0:
        gvo_off_date = res_gvo_off[0][field_map_scada['dt']]

    #  Если не нашли ничего про ГВО
    if gvo_on_date == -1 and gvo_off_date == -1:
        return

    if gvo_on_date >= gvo_off_date:
        current_gvo_state = event_gvo_on
        current_gvo_event_date = gvo_on_date
    else:
        current_gvo_state = event_gvo_off
        current_gvo_event_date = gvo_off_date

    postgre_cursor.execute(f"SELECT id, data ->> 'event_value' as gvoMode, data ->> 'event_unix_dt' as dt "
                           f"FROM event_source_data where source = {event_source} "
                           f"and data ->> 'event_type'='GVO_MODE'")
    field_map_event = fields(postgre_cursor)
    res = postgre_cursor.fetchall()

    need_create_gvo_event = True

    gvo_id = 0

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
            "event_value": current_gvo_state
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

        postgre_cursor.execute(query)
        postgre_conn.commit()


def check_all_event_sources():
    for param in event_source_params:
        event_source = 0
        try:
            event_source = param[fields_map_params['id']]
            param_dict = param[fields_map_params['params']]

            if len(param_dict) == 0:
                local_error = f'{datetime.now()} | {currentScript} | source: {event_source} ' \
                    f'| Не заполнены параметры для источника "{event_source_name}"'
                print(local_error)
                write_log(log_file_name, local_error)
                write_connection_log(event_source, currentScript,
                                     postgre_host, postgre_database, False, local_error)
                continue

            check_event_source(event_source, param_dict)

        except (Exception, Error) as error:
            local_error = f'{datetime.now()} | {currentScript} | source: {event_source} | Ошибка: {error}'
            print(local_error)
            write_log(log_file_name, local_error)
            write_connection_log(event_source, currentScript, postgre_host, postgre_database, False, local_error)
            continue


if __name__ == '__main__':

    currentScript = '[Gvo_Mode_Checker]'
    scada_event_table = 'EvSwitchover'
    gvo_on_event_type_name = 'gvoModeOn'
    gvo_off_event_type_name = 'gvoModeOff'
    event_source_name = "telescada"
    event_source_main = 0
    log_file_name = None
    postgre_cursor = None
    postgre_conn = None
    postgre_host = None
    postgre_database = None

    oldest_log_delta = 30

    try:
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
        log_dir = config_ini.get('common', 'logdir')

        log_file_name = config_ini.get('gvo_checker', 'logfile')
        if log_dir != "":
            log_file_name = log_dir + "/" + log_file_name

        postgre_conn = connect_to_postgre(postgre_database, postgre_user, postgre_pass, postgre_host, postgre_port)

        if not postgre_conn:
            sys.exit(1)

        postgre_cursor = postgre_conn.cursor()

        result_msg = f"{datetime.now()} | {currentScript} | source: {event_source_main} |" \
            f" Соединение с БД {postgre_database}, сервер {postgre_host} успешно!"

        write_connection_log(event_source_main, currentScript, postgre_host, postgre_database, True, result_msg)

        postgre_cursor.execute(f"SELECT * FROM event_type where name = '{gvo_on_event_type_name}'")
        event_gvo_on = postgre_cursor.fetchone()[0]

        postgre_cursor.execute(f"SELECT * FROM event_type where name = '{gvo_off_event_type_name}'")
        event_gvo_off = postgre_cursor.fetchone()[0]

        postgre_cursor.execute(f"SELECT * FROM event_source_params('{event_source_name}')")
        fields_map_params = fields(postgre_cursor)
        event_source_params = postgre_cursor.fetchall()

        if event_source_params is None or len(event_source_params) == 0:
            str_error = f'{datetime.now()} | {currentScript} | source: {event_source_main} ' \
                f'| Не найдет источник событий "{event_source_name}", ' \
                f'БД "{postgre_database}", сервер "{postgre_host}"'
            print(str_error)
            write_log(log_file_name, str_error)
            write_connection_log(event_source_main, currentScript, postgre_host, postgre_database, True, str_error)
            sys.exit(1)

        delete_old_connection_log()

        check_all_event_sources()

    except (Exception, Error) as main_error:
        str_error = f"{datetime.now()} | {currentScript} | source: {event_source_main} | Ошибка: {main_error}"
        print(str_error)

        if log_file_name:
            write_log(log_file_name, str_error)

        if postgre_conn and postgre_cursor and postgre_host and postgre_database:
            write_connection_log(event_source_main, currentScript, postgre_host, postgre_database, True, str_error)

    finally:

        if postgre_cursor:
            postgre_cursor.close()

        if postgre_conn:
            postgre_conn.close()
