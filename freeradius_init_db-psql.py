# http://www.neubot.org/fighting-python-sqlite3-and-operationalerror-unable-open-database-file
import psycopg2
import logging
import requests
import uuid
import argparse
import os
import sys

applicationPath = os.path.dirname(os.path.realpath(__file__))
logging.basicConfig(format=u'%(levelname)-8s [%(asctime)s] %(message)s', level=logging.ERROR, filename=os.path.join(applicationPath, 'errors.log'))

# Разделитель
delimiter = ";"

# Идентификатор CMDB
cmdb_index_column = '_id'

# TODO: Добавить доп. тип сетевого устройства - кассу
# Типы устройств CMDB
cmdb_device_types = {"DesktopCI":           {"net_type": 'gn-desktop'},
                     "NettopCI":            {"net_type": 'gn-nettop'},
                     "NotebookCI":          {"net_type": 'gn-notebook'},
                     "VoIPPhoneCI":         {"net_type": 'gn-voip-phone'},
                     "VoIPGatewayCI":       {"net_type": 'gn-voip-gateway'},
                     "NetworkPrinterCI":    {"net_type": 'sn-printer'},
                     "PrintServerCI":       {"net_type": 'sn-print-server'},
                     "CashRegisterCI":      {"net_type": 'sn-pos'},
                     "IPCameraCI":          {"net_type": 'sn-security'},
                     "RecorderCI":          {"net_type": 'sn-security'},
                     "PACSControllerCI":    {"net_type": 'sn-security'},
                     "PrivateDeviceCI":     {"net_type": 'sn-personal'}}

# Игнорируемые рабочие места (например, склад)
cmdb_ignore_worklaces = ['Warehouseplace']

# Таблицы БД
db_tables = [{"name": "ud_device",
              "columns": [{"name": "id", "type": "INTEGER", "params": "NOT NULL UNIQUE"},
                          {"name": "f_place", "type": "INTEGER", "params": "NOT NULL"},
                          {"name": "c_default_net_type_const", "type": "VARCHAR(64)", "params": "NOT NULL"},
                          {"name": "c_custom_net_type_const", "type": "VARCHAR(64)", "params": "NULL"},
                          {"name": "c_mac", "type": "VARCHAR(12)", "params": "NOT NULL"},
                          {"name": "s_create_date", "type": "TIMESTAMP", "params": "DEFAULT NOW() NOT NULL"},
                          {"name": "s_modif_date", "type": "TIMESTAMP", "params": "NULL"}],
              "sync_ins": """
                    INSERT INTO ud_device(id, f_place, c_default_net_type_const, c_custom_net_type_const, c_mac)
                    SELECT ts.id, ts.f_place, ts.c_default_net_type_const, ts.c_custom_net_type_const, UPPER(ts.c_mac)
                    FROM tmp_device ts
                          LEFT JOIN ud_device s
                              ON ts.id = s.id
                    WHERE ts.g_session_id = %s
                            AND ts.c_error IS NULL
                            AND s.id IS NULL;""",
              "sync_upd": """
                    UPDATE ud_device s
                    SET f_place = ts.f_place, 
                        c_default_net_type_const = ts.c_default_net_type_const,  
                        c_custom_net_type_const = ts.c_custom_net_type_const, 
                        c_mac = UPPER(ts.c_mac),
                        s_modif_date = now()
                    FROM tmp_device ts
                    WHERE ts.id = s.id
                            AND ts.g_session_id = %s
                            AND ts.c_error IS NULL
                            AND (s.f_place != ts.f_place
                                    OR (s.f_place IS NULL AND ts.f_place IS NOT NULL)
                                    OR (s.f_place IS NOT NULL AND ts.f_place IS NULL)
                                    OR s.c_default_net_type_const != ts.c_default_net_type_const
                                    OR (s.c_default_net_type_const IS NULL AND ts.c_default_net_type_const IS NOT NULL)
                                    OR (s.c_default_net_type_const IS NOT NULL AND ts.c_default_net_type_const IS NULL)
                                    OR s.c_custom_net_type_const != ts.c_custom_net_type_const
                                    OR (s.c_custom_net_type_const IS NULL AND ts.c_custom_net_type_const IS NOT NULL)
                                    OR (s.c_custom_net_type_const IS NOT NULL AND ts.c_custom_net_type_const IS NULL)
                                    OR s.c_mac != UPPER(ts.c_mac)
                                    OR (s.c_mac IS NULL AND ts.c_mac IS NOT NULL)
                                    OR (s.c_mac IS NOT NULL AND ts.c_mac IS NULL));""",
              "sync_del": """
                    DELETE
                    FROM ud_device
                    WHERE id in ( SELECT s.id
                                  FROM ud_device s
                                        LEFT JOIN tmp_device ts
                                            ON ts.g_session_id = %s
                                                AND ts.id = s.id
                                                AND ts.c_error IS NULL
                                  WHERE ts.id IS NULL);"""},
             # -->
              {"name": "ud_place",
              "columns": [{"name": "id", "type": "INTEGER", "params": "NOT NULL UNIQUE"},
                          {"name": "c_code", "type": "VARCHAR(64)", "params": "NOT NULL"},
                          {"name": "s_create_date", "type": "TIMESTAMP", "params": "DEFAULT NOW() NOT NULL"},
                          {"name": "s_modif_date", "type": "TIMESTAMP", "params": "NULL"}],
              "sync_ins": """
                    INSERT INTO ud_place(id, c_code)
                    SELECT ts.id, ts.c_code
                    FROM tmp_place ts
                          LEFT JOIN ud_place s
                              ON ts.id = s.id
                    WHERE ts.g_session_id = %s
                            AND s.id IS NULL;""",
              "sync_upd": """
                    UPDATE ud_place s
                    SET c_code = ts.c_code,
                        s_modif_date = now()
                    FROM tmp_place ts
                    WHERE ts.id = s.id 
                            AND ts.g_session_id = %s
                            AND (s.c_code != ts.c_code
                                  OR (s.c_code IS NULL AND ts.c_code IS NOT NULL)
                                  OR (s.c_code IS NOT NULL AND ts.c_code IS NULL));""",
              "sync_del": """
                    DELETE
                    FROM ud_place
                    WHERE id in ( SELECT s.id
                                  FROM ud_place s
                                        LEFT JOIN tmp_place ts
                                            ON ts.g_session_id = %s
                                                AND ts.id = s.id
                                  WHERE ts.id IS NULL);"""},
             # -->
              {"name": "ud_switch",
              "columns": [{"name": "id", "type": "INTEGER", "params": "NOT NULL UNIQUE"},
                          {"name": "f_place", "type": "INTEGER", "params": "NOT NULL"},
                          {"name": "c_hostname", "type": "VARCHAR(255)", "params": "NOT NULL"},
                          {"name": "c_ip_address", "type": "VARCHAR(15)", "params": "NOT NULL"},
                          {"name": "c_radius_key", "type": "VARCHAR(20)", "params": "NOT NULL"},
                          {"name": "c_auth_key", "type": "VARCHAR(20)", "params": "NOT NULL"},
                          {"name": "s_create_date", "type": "TIMESTAMP", "params": "DEFAULT NOW() NOT NULL"},
                          {"name": "s_modif_date", "type": "TIMESTAMP", "params": "NULL"}],
              "sync_ins": """
                    INSERT INTO ud_switch(id, f_place, c_hostname, c_ip_address, c_radius_key, c_auth_key)
                    SELECT ts.id, ts.f_place, ts.c_hostname, ts.c_ip_address, ts.c_radius_key, ts.c_auth_key
                    FROM tmp_switch ts
                          LEFT JOIN ud_switch s
                              ON ts.id = s.id
                    WHERE ts.g_session_id = %s
                            AND ts.c_error IS NULL
                            AND s.id IS NULL;""",
              "sync_upd": """
                    UPDATE ud_switch s
                    SET f_place = ts.f_place, 
                        c_hostname = ts.c_hostname, 
                        c_ip_address = ts.c_ip_address, 
                        c_radius_key = ts.c_radius_key,
                        c_auth_key = ts.c_auth_key,
                        s_modif_date = now()
                    FROM tmp_switch ts
                    WHERE ts.id = s.id
                            AND ts.g_session_id = %s
                            AND ts.c_error IS NULL
                            AND (s.f_place != ts.f_place
                                  OR (s.f_place IS NULL AND ts.f_place IS NOT NULL)
                                  OR (s.f_place IS NOT NULL AND ts.f_place IS NULL)
                                  OR s.c_ip_address != ts.c_ip_address
                                  OR (s.c_ip_address IS NULL AND ts.c_ip_address IS NOT NULL)
                                  OR (s.c_ip_address IS NOT NULL AND ts.c_ip_address IS NULL)
                                  OR s.c_radius_key != ts.c_radius_key
                                  OR (s.c_radius_key IS NULL AND ts.c_radius_key IS NOT NULL)
                                  OR (s.c_radius_key IS NOT NULL AND ts.c_radius_key IS NULL)
                                  OR s.c_auth_key != ts.c_auth_key
                                  OR (s.c_auth_key IS NULL AND ts.c_auth_key IS NOT NULL)
                                  OR (s.c_auth_key IS NOT NULL AND ts.c_auth_key IS NULL)                                  
                                  OR s.c_hostname != ts.c_hostname
                                  OR (s.c_hostname IS NULL AND ts.c_hostname IS NOT NULL)
                                  OR (s.c_hostname IS NOT NULL AND ts.c_hostname IS NULL));""",
              "sync_del": """
                    DELETE
                    FROM ud_switch
                    WHERE id in ( SELECT s.id
                                  FROM ud_switch s
                                        LEFT JOIN tmp_switch ts
                                            ON ts.g_session_id = %s
                                                AND ts.id = s.id
                                                AND ts.c_error IS NULL
                                  WHERE ts.id IS NULL);"""},
             # -->
              {"name": "ud_network",
              "columns": [{"name": "id", "type": "INTEGER", "params": "NOT NULL UNIQUE"},
                          {"name": "c_name", "type": "VARCHAR(255)", "params": "NOT NULL"},
                          {"name": "n_vlan_id", "type": "INTEGER", "params": "NOT NULL"},
                          {"name": "s_create_date", "type": "TIMESTAMP", "params": "DEFAULT NOW() NOT NULL"},
                          {"name": "s_modif_date", "type": "TIMESTAMP", "params": "NULL"}],
              "sync_ins": """
                    INSERT INTO ud_network(id, c_name, n_vlan_id)
                    SELECT ts.id, ts.c_name, ts.n_vlan_id
                    FROM tmp_network ts
                          LEFT JOIN ud_network s
                              ON ts.id = s.id
                    WHERE ts.g_session_id = %s
                            AND ts.c_error IS NULL
                            AND s.id IS NULL;""",
              "sync_upd": """
                    UPDATE ud_network s
                    SET c_name = ts.c_name, 
                        n_vlan_id = ts.n_vlan_id,
                        s_modif_date = now()
                    FROM tmp_network ts
                    WHERE ts.id = s.id
                            AND ts.g_session_id = %s
                            AND ts.c_error IS NULL
                            AND (s.n_vlan_id != ts.n_vlan_id
                                  OR (s.n_vlan_id IS NULL AND ts.n_vlan_id IS NOT NULL)
                                  OR (s.n_vlan_id IS NOT NULL AND ts.n_vlan_id IS NULL))
                            AND (s.c_name != ts.c_name
                                  OR (s.c_name IS NULL AND ts.c_name IS NOT NULL)
                                  OR (s.c_name IS NOT NULL AND ts.c_name IS NULL));""",
              "sync_del": """
                    DELETE
                    FROM ud_network
                    WHERE id in ( SELECT s.id
                                  FROM ud_network s
                                        LEFT JOIN tmp_network ts
                                            ON ts.g_session_id = %s
                                                AND ts.id = s.id
                                                AND ts.c_error IS NULL
                                  WHERE ts.id IS NULL);"""},
             # -->
             {"name": "ud_network_type",
              "columns": [{"name": "id", "type": "INTEGER", "params": "NOT NULL UNIQUE"},
                          {"name": "c_const", "type": "VARCHAR(64)", "params": "NOT NULL"},
                          {"name": "s_create_date", "type": "TIMESTAMP", "params": "DEFAULT NOW() NOT NULL"},
                          {"name": "s_modif_date", "type": "TIMESTAMP", "params": "NULL"}],
              "sync_ins": """
                    INSERT INTO ud_network_type(id, c_const)
                    SELECT ts.id, ts.c_const
                    FROM tmp_network_type ts
                          LEFT JOIN ud_network_type s
                              ON ts.id = s.id
                    WHERE ts.g_session_id = %s
                            AND ts.c_error IS NULL
                            AND s.id IS NULL;""",
              "sync_upd": """
                    UPDATE ud_network_type s
                    SET c_const = ts.c_const,
                        s_modif_date = now()
                    FROM tmp_network_type ts
                    WHERE ts.id = s.id
                            AND ts.g_session_id = %s
                            AND ts.c_error IS NULL
                            AND (s.c_const != ts.c_const
                                  OR (s.c_const IS NULL AND ts.c_const IS NOT NULL)
                                  OR (s.c_const IS NOT NULL AND ts.c_const IS NULL));""",
              "sync_del": """
                    DELETE
                    FROM ud_network_type
                    WHERE id in ( SELECT s.id
                                  FROM ud_network_type s
                                        LEFT JOIN tmp_network_type ts
                                            ON ts.g_session_id = %s
                                                AND ts.id = s.id
                                                AND ts.c_error IS NULL
                                  WHERE ts.id IS NULL);"""},
             # -->
             {"name": "ud_switch_network",
              "columns": [{"name": "id", "type": "INTEGER", "params": "PRIMARY KEY NOT NULL"},
                          {"name": "f_switch", "type": "INTEGER", "params": "NOT NULL"},
                          {"name": "f_network", "type": "INTEGER", "params": "NOT NULL"},
                          {"name": "s_create_date", "type": "TIMESTAMP", "params": "DEFAULT NOW() NOT NULL"},
                          {"name": "s_modif_date", "type": "TIMESTAMP", "params": "NULL"}],
              "sync_ins": """
                    INSERT INTO ud_switch_network(id, f_switch, f_network)
                    SELECT ts.id, ts.f_switch, ts.f_network
                    FROM tmp_switch_network ts
                          LEFT JOIN ud_switch_network s
                              ON ts.id = s.id
                    WHERE ts.g_session_id = %s
                            AND s.id IS NULL;""",
              "sync_upd": """
                    UPDATE ud_switch_network s
                    SET f_switch = ts.f_switch, 
                        f_network = ts.f_network,
                        s_modif_date = now()
                    FROM tmp_switch_network ts
                    WHERE ts.id = s.id 
                            AND ts.g_session_id = %s
                            AND (s.f_switch != ts.f_switch
                                  OR (s.f_switch IS NULL AND ts.f_switch IS NOT NULL)
                                  OR (s.f_switch IS NOT NULL AND ts.f_switch IS NULL)
                                  OR s.f_network != ts.f_network
                                  OR (s.f_network IS NULL AND ts.f_network IS NOT NULL)
                                  OR (s.f_network IS NOT NULL AND ts.f_network IS NULL));""",
              "sync_del": """
                    DELETE
                    FROM ud_switch_network
                    WHERE id in ( SELECT s.id
                                  FROM ud_switch_network s
                                        LEFT JOIN tmp_switch_network ts
                                            ON ts.g_session_id = %s
                                                AND ts.id = s.id
                                  WHERE ts.id IS NULL);"""},
            # -->
             {"name": "ud_network_network_type",
             "columns": [{"name": "id", "type": "INTEGER", "params": "NOT NULL UNIQUE"},
                         {"name": "f_network", "type": "INTEGER", "params": "NOT NULL"},
                         {"name": "f_network_type", "type": "INTEGER", "params": "NOT NULL"},
                         {"name": "s_create_date", "type": "TIMESTAMP", "params": "DEFAULT NOW() NOT NULL"},
                         {"name": "s_modif_date", "type": "TIMESTAMP", "params": "NULL"}],
              "sync_ins": """
                    INSERT INTO ud_network_network_type(id, f_network, f_network_type)
                    SELECT ts.id, ts.f_network, ts.f_network_type
                    FROM tmp_network_network_type ts
                          LEFT JOIN ud_network_network_type s
                              ON ts.id = s.id
                    WHERE ts.g_session_id = %s
                            AND s.id IS NULL;""",
              "sync_upd": """
                    UPDATE ud_network_network_type s
                    SET f_network = ts.f_network, 
                        f_network_type = ts.f_network_type,
                        s_modif_date = now()
                    FROM tmp_network_network_type ts
                    WHERE ts.id = s.id
                            AND ts.g_session_id = %s
                            AND (s.f_network != ts.f_network
                                  OR s.f_network_type != ts.f_network_type);""",
              "sync_del": """
                    DELETE
                    FROM ud_network_network_type
                    WHERE id in ( SELECT s.id
                                  FROM ud_network_network_type s
                                        LEFT JOIN tmp_network_network_type ts
                                            ON ts.g_session_id = %s
                                                AND ts.id = s.id
                                  WHERE ts.id IS NULL);"""},
            # -->
             {"name": "sd_session",
              "columns": [{"name": "id", "type": "UUID", "params": "PRIMARY KEY"},
                          {"name": "b_status", "type": "BOOLEAN", "params": "NULL"},
                          {"name": "s_start_date", "type": "TIMESTAMP", "params": "DEFAULT NOW() NOT NULL"},
                          {"name": "s_end_date", "type": "TIMESTAMP", "params": "NULL"}],
              "sync_ins": None,
              "sync_upd": None,
              "sync_del": None},
             # -->
             {"name": "sd_sessions_log",
              "columns": [{"name": "id", "type": "SERIAL", "params": "PRIMARY KEY"},
                          {"name": "f_session", "type": "UUID", "params": "NOT NULL"},
                          {"name": "c_type", "type": "VARCHAR(64)", "params": "NOT NULL"},
                          {"name": "n_id", "type": "INTEGER", "params": "NOT NULL"},
                          {"name": "c_inventory_number", "type": "VARCHAR(64)", "params": "NULL"},
                          {"name": "c_message", "type": "TEXT", "params": "NOT NULL"},
                          {"name": "s_create_date", "type": "TIMESTAMP", "params": "DEFAULT NOW() NOT NULL"}],
              "sync_ins": None,
              "sync_upd": None,
              "sync_del": None},
             # -->
             {"name": "radacct",
              "columns": [{"name": "RadAcctId", "type": "BIGSERIAL", "params": "PRIMARY KEY"},
                          {"name": "AcctSessionId", "type": "VARCHAR(64)", "params": "NOT NULL"},
                          {"name": "AcctUniqueId", "type": "VARCHAR(32)", "params": "NOT NULL UNIQUE"},
                          {"name": "UserName", "type": "VARCHAR(253)", "params": ""},
                          {"name": "GroupName", "type": "VARCHAR(253)", "params": ""},
                          {"name": "Realm", "type": "VARCHAR(64)", "params": ""},
                          {"name": "NASIPAddress", "type": "INET", "params": "NOT NULL"},
                          {"name": "NASPortId", "type": "VARCHAR(15)", "params": ""},
                          {"name": "NASPortType", "type": "VARCHAR(32)", "params": ""},
                          {"name": "AcctStartTime", "type": "TIMESTAMP with time zone", "params": ""},
                          {"name": "AcctStopTime", "type": "TIMESTAMP with time zone", "params": ""},
                          {"name": "AcctSessionTime", "type": "BIGINT", "params": ""},
                          {"name": "AcctAuthentic", "type": "VARCHAR(32)", "params": ""},
                          {"name": "ConnectInfo_start", "type": "VARCHAR(50)", "params": ""},
                          {"name": "ConnectInfo_stop", "type": "VARCHAR(50)", "params": ""},
                          {"name": "AcctInputOctets", "type": "BIGINT", "params": ""},
                          {"name": "AcctOutputOctets", "type": "BIGINT", "params": ""},
                          {"name": "CalledStationId", "type": "VARCHAR(50)", "params": ""},
                          {"name": "CallingStationId", "type": "VARCHAR(50)", "params": ""},
                          {"name": "AcctTerminateCause", "type": "VARCHAR(32)", "params": ""},
                          {"name": "ServiceType", "type": "VARCHAR(32)", "params": ""},
                          {"name": "XAscendSessionSvrKey", "type": "VARCHAR(10)", "params": ""},
                          {"name": "FramedProtocol", "type": "VARCHAR(32)", "params": ""},
                          {"name": "FramedIPAddress", "type": "INET", "params": ""},
                          {"name": "AcctStartDelay", "type": "INTEGER", "params": ""},
                          {"name": "AcctStopDelay", "type": "INTEGER", "params": ""}],
              "sync_ins": None,
              "sync_upd": None,
              "sync_del": None},
             # -->
             {"name": "radpostauth",
              "columns": [{"name": "id", "type": "BIGSERIAL", "params": "PRIMARY KEY"},
                          {"name": "username", "type": "VARCHAR(253)", "params": "NOT NULL"},
                          {"name": "reply", "type": "VARCHAR(32)", "params": ""},
                          {"name": "nasid", "type": "VARCHAR(50)", "params": ""},
                          {"name": "nasip ", "type": "VARCHAR(15)", "params": ""},
                          {"name": "nasport ", "type": "INTEGER", "params": ""},
                          {"name": "authdate", "type": "TIMESTAMP with time zone", "params": "DEFAULT NOW() NOT NULL"}],
              "sync_ins": None,
              "sync_upd": None,
              "sync_del": None},
             # -->
             {"name": "radreply",
              "columns": [{"name": "id", "type": "SERIAL", "params": "PRIMARY KEY"},
                          {"name": "UserName", "type": "VARCHAR(64)", "params": "NOT NULL DEFAULT ''"},
                          {"name": "Attribute", "type": "VARCHAR(64)", "params": "NOT NULL DEFAULT ''"},
                          {"name": "op", "type": "CHAR(2)", "params": "NOT NULL DEFAULT '='"},
                          {"name": "Value", "type": "VARCHAR(253)", "params": "NOT NULL DEFAULT ''"}],
              "sync_ins": None,
              "sync_upd": None,
              "sync_del": None},
             # -->
             {"name": "ud_guest_device",
              "columns": [{"name": "id", "type": "SERIAL", "params": "PRIMARY KEY"},
                          {"name": "c_mac", "type": "VARCHAR(12)", "params": "NOT NULL"},
                          {"name": "c_nas_ip", "type": "VARCHAR(15)", "params": "NOT NULL"},
                          {"name": "n_nas_port", "type": "INT", "params": "NOT NULL"},
                          {"name": "s_create_date", "type": "TIMESTAMP", "params": "DEFAULT NOW() NOT NULL"}],
              "sync_ins": None,
              "sync_upd": None,
              "sync_del": None}]

# Временные таблицы БД
db_tmp_tables = [{"name": "tmp_device",
                 "data_binding": "device",
                 "columns": [{"name": "id", "type": "INTEGER", "params": "", "binding": "id"},
                             {"name": "g_session_id", "type": "UUID", "params": "", "binding": "session_id"},
                             {"name": "f_place", "type": "INTEGER", "params": "", "binding": "place"},
                             {"name": "c_default_net_type_const", "type": "VARCHAR(64)", "params": "", "binding": "default_net_type_const"},
                             {"name": "c_custom_net_type_const", "type": "VARCHAR(64)", "params": "", "binding": "custom_net_type_const"},
                             {"name": "c_mac", "type": "VARCHAR(12)", "params": "", "binding": "mac"},
                             {"name": "c_inventory_number", "type": "VARCHAR(64)", "params": "", "binding": "inventory"},
                             {"name": "c_error", "type": "TEXT", "params": "NULL", "binding": None}],
                  "query_valid": """
                        UPDATE tmp_device
                        SET c_error = CASE WHEN f_place IS NULL
                                          THEN 'Не указано расположение. '
                                          ELSE ''
                                      END ||
                                      CASE WHEN c_mac IS NULL
                                          THEN 'Отсуствует MAC адрес.'
                                          ELSE ''
                                      END
                        WHERE g_session_id = %s
                                AND (f_place IS NULL
                                        OR c_mac IS NULL);""",
                  "query_log": """
                        INSERT INTO sd_sessions_log (f_session, c_type, n_id, c_inventory_number, c_message)
                        SELECT g_session_id, 'device', id, c_inventory_number, c_error
                        FROM tmp_device
                        WHERE g_session_id = %s
                              AND c_error IS NOT NULL;""",
                  "query_clear": """
                        DELETE FROM tmp_device WHERE g_session_id = %s;"""},
                 # -->
                 {"name": "tmp_place",
                 "data_binding": "place",
                 "columns": [{"name": "id", "type": "INTEGER", "params": "", "binding": "id"},
                             {"name": "g_session_id", "type": "UUID", "params": "", "binding": "session_id"},
                             {"name": "c_code", "type": "VARCHAR(64)", "params": "", "binding": "code"}],
                  "query_valid": None,
                  "query_log": None,
                  "query_clear": """
                        DELETE FROM tmp_place WHERE g_session_id = %s;"""},
                 # -->
                 {"name": "tmp_switch",
                 "data_binding": "switch",
                 "columns": [{"name": "id", "type": "INTEGER", "params": "", "binding": "id"},
                             {"name": "g_session_id", "type": "UUID", "params": "", "binding": "session_id"},
                             {"name": "f_place", "type": "INTEGER", "params": "", "binding": "place"},
                             {"name": "c_hostname", "type": "VARCHAR(255)", "params": "", "binding": "hostname"},
                             {"name": "c_inventory_number", "type": "VARCHAR(64)", "params": "", "binding": "inventory"},
                             {"name": "c_ip_address", "type": "VARCHAR(15)", "params": "", "binding": "ip_address"},
                             {"name": "c_radius_key", "type": "VARCHAR(20)", "params": "", "binding": "radius_key"},
                             {"name": "c_auth_key", "type": "VARCHAR(20)", "params": "", "binding": "auth_key"},
                             {"name": "c_error", "type": "TEXT", "params": "NULL", "binding": None}],
                 "query_valid": """
                        UPDATE tmp_switch
                        SET c_error = CASE WHEN f_place IS NULL
                                          THEN 'Не указано расположение. '
                                          ELSE ''
                                      END ||
                                      CASE WHEN c_hostname IS NULL
                                          THEN 'Не указано сетевое имя устройства. '
                                          ELSE ''
                                      END ||
                                      CASE WHEN c_ip_address IS NULL
                                          THEN 'Не указан IP адрес устройства. '
                                          ELSE ''
                                      END ||
                                      CASE WHEN c_radius_key IS NULL
                                          THEN 'Не задан пароль сетевого устройства NAS. '
                                          ELSE ''
                                      END ||
                                      CASE WHEN c_auth_key IS NULL
                                          THEN 'Не задан пароль клиентского доступа.'
                                          ELSE ''
                                      END
                        WHERE g_session_id = %s
                                AND (f_place IS NULL
                                        OR c_ip_address IS NULL
                                        OR c_radius_key IS NULL
                                        OR c_auth_key IS NULL
                                        OR c_hostname IS NULL); """,
                 "query_log": """
                        INSERT INTO sd_sessions_log (f_session, c_type, n_id, c_inventory_number, c_message)
                        SELECT g_session_id, 'switch', id, c_inventory_number, c_error
                        FROM tmp_switch
                        WHERE g_session_id = %s
                             AND c_error IS NOT NULL;""",
                  "query_clear": """
                        DELETE FROM tmp_switch WHERE g_session_id = %s;"""},
                 # -->
                 {"name": "tmp_network",
                  "data_binding": "network",
                  "columns": [{"name": "id", "type": "INTEGER", "params": "", "binding": "id"},
                              {"name": "g_session_id", "type": "UUID", "params": "", "binding": "session_id"},
                              {"name": "c_name", "type": "VARCHAR(255)", "params": "", "binding": "name"},
                              {"name": "n_vlan_id", "type": "INTEGER", "params": "", "binding": "vlan_id"},
                              {"name": "c_error", "type": "TEXT", "params": "NULL", "binding": None}],
                  "query_valid": """
                        UPDATE tmp_network
                        SET c_error = CASE WHEN n_vlan_id IS NULL
                                          THEN 'Отсутствует идентификатор VLAN'
                                          ELSE ''
                                      END ||
                                      CASE WHEN c_name IS NULL
                                          THEN 'Отсутствует наименование сети. '
                                          ELSE ''
                                      END
                        WHERE g_session_id = %s
                                AND (n_vlan_id IS NULL 
                                      OR c_name IS NULL);""",
                  "query_log": """
                        INSERT INTO sd_sessions_log (f_session, c_type, n_id, c_inventory_number, c_message)
                        SELECT g_session_id, 'network', id, NULL, c_error
                        FROM tmp_network
                        WHERE g_session_id = %s
                             AND c_error IS NOT NULL;""",
                  "query_clear": """
                        DELETE FROM tmp_network WHERE g_session_id = %s;"""},
                 # -->
                 {"name": "tmp_network_type",
                  "data_binding": "network_type",
                  "columns": [{"name": "id", "type": "INTEGER", "params": "", "binding": "id"},
                              {"name": "g_session_id", "type": "UUID", "params": "", "binding": "session_id"},
                              {"name": "c_const", "type": "VARCHAR(64)", "params": "", "binding": "const"},
                              {"name": "c_error", "type": "TEXT", "params": "NULL", "binding": None}],
                  "query_valid": """
                        UPDATE tmp_network_type
                        SET c_error = 'Не задана константа типа'
                        WHERE g_session_id = %s
                                AND c_const IS NULL;""",
                  "query_log": """
                        INSERT INTO sd_sessions_log (f_session, c_type, n_id, c_inventory_number, c_message)
                        SELECT g_session_id, 'network_type', id, NULL, c_error
                        FROM tmp_network_type
                        WHERE g_session_id = %s
                             AND c_error IS NOT NULL;""",
                  "query_clear": """
                        DELETE FROM tmp_network_type WHERE g_session_id = %s;"""},
                 # -->
                 {"name": "tmp_switch_network",
                  "data_binding": "switch_network",
                  "columns": [{"name": "id", "type": "INTEGER", "params": "", "binding": "id"},
                              {"name": "g_session_id", "type": "UUID", "params": "", "binding": "session_id"},
                              {"name": "f_switch", "type": "INTEGER", "params": "", "binding": "switch"},
                              {"name": "f_network", "type": "INTEGER", "params": "", "binding": "network"}],
                  "query_valid": """
                        DELETE
                        FROM tmp_switch_network
                        WHERE id IN ( SELECT sn.id
                                      FROM tmp_switch_network sn
                                            INNER JOIN tmp_switch s
                                              ON sn.f_switch = s.id
                                                  AND sn.g_session_id = s.g_session_id
                                            INNER JOIN tmp_network n
                                              ON sn.f_network = n.id
                                                  AND sn.g_session_id = n.g_session_id
                                      WHERE sn.g_session_id = %s
                                              AND (s.c_error IS NOT NULL
                                                    OR n.c_error IS NOT NULL));                                    
                  """,
                  "query_log": None,
                  "query_clear": """
                        DELETE FROM tmp_switch_network WHERE g_session_id = %s;"""},
                 # -->
                 {"name": "tmp_network_network_type",
                  "data_binding": "network_network_type",
                  "columns": [{"name": "id", "type": "INTEGER", "params": "", "binding": "id"},
                              {"name": "g_session_id", "type": "UUID", "params": "", "binding": "session_id"},
                              {"name": "f_network", "type": "INTEGER", "params": "", "binding": "network"},
                              {"name": "f_network_type", "type": "INTEGER", "params": "", "binding": "network_type"}],
                  "query_valid": """
                        DELETE 
                        FROM tmp_network_network_type
                        WHERE id IN ( SELECT nnt.id
                                      FROM tmp_network_network_type nnt
                                            INNER JOIN tmp_network n
                                              ON nnt.f_network = n.id
                                                  AND nnt.g_session_id = n.g_session_id
                                            INNER JOIN tmp_network_type nt
                                              ON nnt.f_network_type = nt.id
                                                  AND nnt.g_session_id = nt.g_session_id
                                      WHERE nnt.g_session_id = %s
                                              AND (n.c_error IS NOT NULL
                                                    OR nt.c_error IS NOT NULL));                                    
                  """,
                  "query_log": None,
                  "query_clear": """
                        DELETE FROM tmp_network_network_type WHERE g_session_id = %s;"""}]

# Представления
db_views = [{"name": "nas",
             "definition": """
                        SELECT id,                                  -- SERIAL
                               c_ip_address AS nasname,             -- VARCHAR(128) NOT NULL
                               c_hostname   AS shortname,           -- VARCHAR(32) NOT NULL
                               'other'      AS type,                -- VARCHAR(30) NOT NULL DEFAULT 'other'
                               NULL         AS ports,               -- int4
                               c_radius_key AS secret,              -- VARCHAR(60) NOT NULL
                               NULL         AS server,              -- VARCHAR(64)
                               NULL         AS community,           -- VARCHAR(50)
                               NULL         AS description          -- VARCHAR(200)
                        FROM ud_switch;"""},
            {"name": "radcheck",
             "definition": """
                        SELECT id,                                  -- SERIAL
                               c_mac        AS UserName,            -- VARCHAR(64) NOT NULL DEFAULT ''
                               'Password'   AS Attribute,           -- VARCHAR(64) NOT NULL DEFAULT ''
                               '=='         AS op,                  -- CHAR(2) NOT NULL DEFAULT '=='
                               ''           AS Value                -- VARCHAR(253) NOT NULL DEFAULT ''
                        FROM ud_device
                        UNION ALL
                        SELECT id * -1,
                               c_mac AS username,
                               'Password' AS attribute,
                               '==' AS op,
                               '' AS value
                        FROM ud_guest_device;"""},
            {"name": "radgroupcheck",
             "definition": """
                        SELECT row_number() OVER (ORDER BY un.id, us.id)    AS id,          -- SERIAL
                               un.c_name || '_' ||
                               us.c_hostname || '_' ||
                               CAST(un.id AS TEXT) || '_' ||
                               CAST(us.id AS TEXT)                          AS GroupName,   -- VARCHAR(64) NOT NULL DEFAULT ''
                               t.attribute                                  AS Attribute,   -- VARCHAR(64) NOT NULL DEFAULT ''
                               '=='                                         AS op,          -- CHAR(2) NOT NULL DEFAULT '=='
                               CASE t.attribute
                                     WHEN 'NAS-IP-Address' THEN c_ip_address
                                     WHEN 'NAS-Port-Type' THEN '15'
                                     WHEN 'Service-Type' THEN '7'
                                     WHEN 'Password' THEN c_auth_key
                                     ELSE ''
                               END                                          AS Value        -- VARCHAR(253) NOT NULL DEFAULT ''
                        FROM ud_switch us
                              INNER JOIN ud_switch_network usn
                                  ON us.id = usn.f_switch
                              INNER JOIN ud_network un
                                  ON usn.f_network = un.id
                              CROSS JOIN (SELECT 'NAS-IP-Address' AS attribute
                                          UNION ALL
                                          SELECT 'NAS-Port-Type'  AS attribute
                                          UNION ALL
                                          SELECT 'Service-Type'   AS attribute
                                          UNION ALL
                                          SELECT 'Password'       AS attribute) t;"""},
            {"name": "radgroupreply",
             "definition": """
                        SELECT row_number() OVER (ORDER BY un.id, us.id)    AS id,          -- SERIAL
                               un.c_name || '_' ||
                               us.c_hostname || '_' ||
                               CAST(un.id AS TEXT) || '_' ||
                               CAST(us.id AS TEXT)                          AS GroupName,   -- VARCHAR(64) NOT NULL DEFAULT ''
                               t.attribute                                  AS Attribute,   -- VARCHAR(64) NOT NULL DEFAULT ''
                               '='                                          AS op,          -- CHAR(2) NOT NULL DEFAULT '='
                               CASE t.attribute
                                     WHEN 'Tunnel-Medium-Type' THEN '6'
                                     WHEN 'Tunnel-Type' THEN 'VLAN'
                                     WHEN 'Tunnel-Private-Group-Id' THEN CAST(un.n_vlan_id AS VARCHAR(253))
                                     ELSE ''
                               END                                          AS Value        -- VARCHAR(253) NOT NULL DEFAULT ''
                        FROM ud_switch us
                              INNER JOIN ud_switch_network usn
                                  ON us.id = usn.f_switch
                              INNER JOIN ud_network un
                                  ON usn.f_network = un.id
                              CROSS JOIN (SELECT 'Tunnel-Medium-Type'         AS attribute
                                          UNION ALL
                                          SELECT 'Tunnel-Type'                AS attribute
                                          UNION ALL
                                          SELECT 'Tunnel-Private-Group-Id'    AS attribute) t;"""},
            {"name": "radusergroup",
             "definition": """
                        SELECT d.c_mac                        AS UserName,      -- VARCHAR(64) NOT NULL DEFAULT ''
                               n.c_name || '_' ||
                               s.c_hostname || '_' ||
                               CAST(n.id AS TEXT) || '_' ||
                               CAST(s.id AS TEXT)             AS GroupName,     -- VARCHAR(64) NOT NULL DEFAULT ''
                               0                              AS priority       -- INTEGER NOT NULL DEFAULT 0
                        FROM (SELECT dev.c_mac,
                                     dev.f_place,
                                     dev.c_custom_net_type_const,
                                     dev.c_default_net_type_const
                              FROM ud_device dev
                              UNION ALL
                              SELECT gdev.c_mac,
                                     sw.f_place,
                                     NULL                           AS c_custom_net_type_const,
                                     'sn-guest'                     AS c_default_net_type_const
                              FROM ud_guest_device gdev
                                      INNER JOIN ud_switch sw
                                        ON gdev.c_nas_ip = sw.c_ip_address) d
                              INNER JOIN ud_place p
                                ON d.f_place = p.id
                              INNER JOIN ud_switch s
                                ON p.id = s.f_place
                              INNER JOIN ud_switch_network sn
                                ON s.id = sn.f_switch
                              INNER JOIN ud_network n
                                ON sn.f_network = n.id
                              INNER JOIN ud_network_network_type nnt
                                ON n.id = nnt.f_network
                              INNER JOIN ud_network_type nt
                                ON nnt.f_network_type = nt.id
                                    AND COALESCE(d.c_custom_net_type_const, d.c_default_net_type_const) = nt.c_const;"""}]

db_row_query = [
    """
        CREATE OR REPLACE FUNCTION tri_ud_guest_device() RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                -- Если устройство зарегистрировано, не заносим его в буфер гостевых устройств
                IF EXISTS ( SELECT 1
                            FROM ud_device d
                                  INNER JOIN ud_switch s
                                    ON d.f_place = s.f_place
                            WHERE c_mac = NEW.c_mac
                                    AND s.c_ip_address = NEW.c_nas_ip) THEN
                    -- Если устройство переместилось из гостевой сети, удаляем из буфера гостевых устройств
                    IF EXISTS (SELECT 1 FROM ud_guest_device WHERE c_mac = NEW.c_mac) THEN
                        DELETE
                        FROM ud_guest_device
                        WHERE c_mac = NEW.c_mac;
                    END IF;
        
                    RETURN NULL;
                END IF;
        
                -- Если утройтсво уже есть в буфере - не заносим его туда повторно
                IF EXISTS (SELECT 1 FROM ud_guest_device WHERE c_mac = NEW.c_mac) THEN
                    RETURN NULL;
                END IF;
        
            END IF;
        
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;    
    """,
    """
        DO
        $$
        BEGIN
        IF NOT EXISTS(SELECT 1
                      FROM information_schema.triggers
                      WHERE event_object_table = 'ud_guest_device'
                              AND trigger_name = 'tri_ud_guest_device')
          THEN
            CREATE TRIGGER tri_ud_guest_device BEFORE INSERT ON ud_guest_device
              FOR EACH ROW EXECUTE PROCEDURE tri_ud_guest_device();
          END IF;
        END;
        $$"""]


def import_data(cmdb_url, cmdb_user, cmdb_pwd, db_host, db_user, db_pwd, db_name):
    """
    Загрузка данных из cmdbuild и импорт в БД

    :param cmdb_url: URL REST-API CMDB
    :param cmdb_user: Пользователь CMDB
    :param cmdb_pwd: Пароль пользователя CMDB
    :param db_host: Hostname/IP БД сервера RADIUS
    :param db_user: Пользователь БД
    :param db_pwd: Пароль пользователя БД
    :param db_name: Имя БД
    :return: True - успешная загрузка, False - ошибка
    """
    retVal = True
    errorMsg = None
    con = None
    session_id = uuid.uuid4().hex

    try:
        con = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (db_name, db_user, db_host, db_pwd))
        con.set_session(autocommit=True)
    except Exception as e:
        errorMsg = "Ошибка подключения к БД: " + str(e)
        retVal = False

    # Инициализация БД
    if retVal:
        if not db_init(con) or not db_start_load_session(con, session_id):
            errorMsg = "Ошибка инициализации БД"
            retVal = False

    # Загрузка данных во временный буфер
    if retVal:
        if not db_load_data_in_buffer(con, session_id, cmdb_url, cmdb_user, cmdb_pwd):
            errorMsg = "Ошибка загрузки данных из CMDB"
            retVal = False

    # Запуск процедуры импорта в рабочую БД
    if retVal:
        if not db_sync_data(con, session_id):
            errorMsg = "Ошибка при импорте данных в рабочую БД"
            retVal = False

    # Очистка временных таблиц
    if retVal:
        if not db_clear_tmp_tables(con, session_id):
            errorMsg = "Ошибка при очистке буферов БД"
            retVal = False

    # Завершаем сессию загрузки
    if con is not None:
        db_end_load_session(con, session_id, retVal)
        con.close()

    # Логируем ошибку, если есть
    if errorMsg is not None:
        logging.error(errorMsg)

    return retVal

def db_load_data_in_buffer(con, session_id, cmdb_url, cmdb_user, cmdb_pwd):
    """
    Загрузка данных из CMDB во временный буфер

    :param con: Соединение
    :param session_id: Идентификатор сессии
    :param cmdb_url: URL REST-API CMDB
    :param cmdb_user: Пользователь CMDB
    :param cmdb_pwd: Пароль пользователя CMDB

    :return: True - успешно, False - ошибка
    """

    data = cmdb_get_data(session_id, cmdb_url, cmdb_user, cmdb_pwd)
    if data is None:
        return False

    # Сохранение данных во временные таблицы
    for table in db_tmp_tables:
        if db_save_db_table(con, table["name"], table["columns"], data[table["data_binding"]]) == False:
            return False

    return True

def db_start_load_session(con, session_id):
    """
    Начало сессии загрузки

    :param con: Соединение
    :param session_id: Идентификатор сессии
    :return: True - успешно, False - ошибка
    """

    cur = con.cursor()
    query = "INSERT INTO sd_session (id) VALUES (%s);"

    try:
        cur.execute(query, [session_id])
        con.commit()
    except psycopg2.Error as e:
        logging.error(str(e))
        return False

    return True

def db_end_load_session(con, session_id, status = False):
    """
    Окончание сессии загрузки

    :param con: Соединение
    :param session_id: Идентификатор сессии
    :return: True - успешно, False - ошибка
    """

    cur = con.cursor()
    query = """ UPDATE sd_session 
                SET b_status = %s,
                    s_end_date = current_timestamp
                WHERE id = %s;"""

    try:
        cur.execute(query, [status, session_id])
    except psycopg2.Error as e:
        logging.error(str(e))
        return False

    return True

def db_save_db_table(con, name, columns, data):
    """
    Сохранение таблицы
    :param con: Соединение
    :param name: Наименование таблицы
    :param columns: Список колонок
    :param data: Данные
    :return: True - успешно, Flase - ошибка
    """

    cur = con.cursor()

    # Исключаем колонки без привязок
    cols = [col for col in columns if col["binding"] is not None]
    q = "INSERT INTO %s (%s) VALUES (%s)" % (name, ",".join([col["name"] for col in cols]), ",".join(['%s' for r in range(len(cols))]))

    rows = db_get_table_data(data, cols)

    try:
        if len(rows) != 0:
            cur.executemany(q, rows)
        return True

    except psycopg2.Error as e:
        logging.error(str(e))

    return False

def db_get_table_data(data, columns):
    """
    Формирование списка кортежей

    :param data: Список словарей с данными
    :param columns: Колонки таблицы с привязками к данным
    :return: Список кортежей
    """
    return [[data[key][col["binding"]] for col in columns] for key in data.keys()]

def db_validate_data(con, session_id):
    """
    Валидация загруженных из CMDB данных
    По правильному - интерфейс CMDB должен отдавать только правильные данные, а модуль работать без проверок

    :param con: Соединение с БД
    :return: True - успешно, False - ошибка
    """

    try:
        cur = con.cursor()

        for query in db_tmp_tables:
            if query["query_valid"] is not None:
                cur.execute(query["query_valid"], [session_id])

            if query["query_log"] is not None:
                cur.execute(query["query_log"], [session_id])

    except psycopg2.Error as e:
        logging.error(str(e))
        return False

    return True

def db_sync_data(con, session_id):
    """
    Синхронизация буфера с рабочими таблицами

    :param con: Соединение
    :param session_id: Идентификатор сессии
    :return: True - успешная синхронизация, False - ошибка
    """
    cur = con.cursor()

    # Запуск валидации данных перед загрузкой
    if not db_validate_data(con, session_id):
        logging.error("Ошибка валидации загруженных данных")
        return False

    # Вставки
    for tab in db_tables:
        if tab["sync_ins"] is not None:
            try:
                cur.execute(tab["sync_ins"], [session_id])
            except psycopg2.Error as e:
                logging.error(str(e))
                return False

    # Обновления
    for tab in db_tables:
        if tab["sync_upd"] is not None:
            try:
                cur.execute(tab["sync_upd"], [session_id])
            except psycopg2.Error as e:
                logging.error(str(e))
                return False

    # Удаления
    for tab in db_tables:
        if tab["sync_del"] is not None:
            try:
                cur.execute(tab["sync_del"], [session_id])
            except psycopg2.Error as e:
                logging.error(str(e))
                return False

    return True

def db_clear_tmp_tables(con, session_id):
    """
    Очистка временных таблиц

    :param con: Соединение
    :param session_id: Идентификатор сессии
    :return: True - успешно, False - ошибка
    """

    cur = con.cursor()

    for tab in db_tmp_tables:
        try:
            cur.execute(tab["query_clear"], [session_id])
        except psycopg2.Error as e:
            logging.error(str(e))
            return False

    return True

def db_init(con):
    '''
    Database Init

    :param con: DB Connection
    :return: True - Success Init, False - Error Init
    '''
    cur = con.cursor()

    t = db_tables + db_tmp_tables

    # Формируем запросы на создание таблиц
    tab_querys = ["CREATE TABLE IF NOT EXISTS %s (%s);" % (tab["name"],
                                                           ",".join(["%s %s %s" % (col["name"], col["type"], col["params"])
                                                                     for col in tab["columns"]]))
                  for tab in t]

    view_querys = ["CREATE OR REPLACE VIEW %s AS %s" % (view["name"], view["definition"]) for view in db_views]

    try:
        # Формирование таблиц
        for q in tab_querys:
            cur.execute(q);

        # Формирование представлений
        for q in view_querys:
            cur.execute(q)

        for q in db_row_query:
            cur.execute(q)

        con.commit()
    except psycopg2.Error as e:
        logging.error(str(e))

        return False

    return True

def db_save_auth_log(mac, nas_ip, nas_port, nas_name, status):
    """
    Логирование запроса на аутентификацию

    :param mac: MAC адрес
    :param nas_ip: IP адрес устройства NAS
    :param nas_port: Порт устройства NAS
    :param nas_name: Имя устройства NAS
    :param status: Статус авторизации
    :return: None
    """

    query = """INSERT INTO sd_auth_log (c_mac, c_nas_ip, n_nas_port, c_nas_name, b_status) VALUES (%s, %s, %s, %s, %s)"""
    db_execute_query(query, [mac, nas_ip, nas_port, nas_name, status])

def db_get_device_info(mac, nasip = None, auth_key = None):
    """
    Загрузка данных по устройству

    :param mac: MAC адрес устройства
    :param nasip: IP адрес NAS, на котором авторизуется устройство
    :param auth_key: Ключ аутентификации
    :return: Список возможных вариантов авторизации устройства на оборудовании
    """

    # Корректировка переданных параметров
    mac = mac.upper()
    mac = mac.replace(':', '').replace('-', '').replace('-', '').replace('\'', '').replace('"', '')

    if nasip is not None:
        nasip = nasip.replace('\'', '').replace('"', '')

    if auth_key is not None:
        auth_key = auth_key.replace('\'', '').replace('"', '')


        # Запрос
    q = """ SELECT  d.c_mac, 
                    s.c_ip_address, 
                    n.n_vlan_id
            FROM ud_device d
                  INNER JOIN ud_place p
                    ON d.f_place = p.id
                  INNER JOIN ud_switch s
                    ON p.id = s.f_place
                  INNER JOIN ud_switch_network sn
                    ON s.id = sn.f_switch
                  INNER JOIN ud_network n
                    ON sn.f_network = n.id
                  INNER JOIN ud_network_network_type nnt
                    ON n.id = nnt.f_network
                  INNER JOIN ud_network_type nt
                    ON nnt.f_network_type = nt.id
                        AND IFNULL(d.c_custom_net_type_const, d.c_default_net_type_const) = nt.c_const
            WHERE d.c_mac = %s
                    AND (s.c_ip_address = %s OR %s IS NULL)
                    AND (s.c_auth_key = %s OR %s IS NULL)
            ORDER BY d.c_mac, s.c_ip_address, n.n_vlan_id;"""

    return db_execute_query(q, [mac, nasip, nasip, auth_key, auth_key])

def db_execute_query(query, param = None, retRes = True):
    """
    Выполнение произвольного запроса

    :param query: Запрос
    :param param: Параметры запроса
    :return: Результат запроса, None - ошибка
    """

    try:
        con = sqlite3.connect(db_name)
        con.isolation_level = None

        cur = con.cursor()
        cur.execute(query, param)

        if retRes:
            return {"columns": cur.description,
                    "data": cur.fetchall()}

    except psycopg2.Error as e:
        logging.error(str(e))
        print(str(e))

    if retRes:
        return None

def cmdb_get_data(session_id, cmdb_url, cmdb_user, cmdb_pwd):
    """
    Загрузка данных cmdbuild

    :param session_id: Идентификатор сессии
    :param cmdb_url: URL REST-API CMDB
    :param cmdb_user: Пользователь CMDB
    :param cmdb_pwd: Пароль пользователя CMDB

    :return: Словарь загруженных данных с ключами по типам устройств:
                - network - сети
                - device - устройства
                - switch - коммутаторы
                - switch_network - связь коммутаторов с сетями
    """

    token = cmdb_comm_auth(cmdb_url, cmdb_user, cmdb_pwd)

    if token is None:
        return None

    workplaces = cmdb_get_workplaces(cmdb_url, token)
    network_types = cmdb_get_network_types(cmdb_url, token, session_id)
    networks = cmdb_get_networks(cmdb_url, token, session_id)
    devices = cmdb_get_devices(cmdb_url, token, session_id, workplaces, network_types)
    switches = cmdb_get_switches(cmdb_url, token, session_id, workplaces)
    switch_network = cmdb_get_switch_network_relation(cmdb_url, token, session_id)
    places = cmdb_get_places(cmdb_url, token, session_id)
    network_network_types = cmdb_get_network_network_type(cmdb_url, token, session_id)
    network_types = cmdb_get_network_types(cmdb_url, token, session_id)

    if workplaces is None \
            or networks is None \
            or devices is None \
            or switches is None \
            or switch_network is None \
            or places is None \
            or network_network_types is None \
            or network_types is None:
        return None

    return {"network": networks,
            "device": devices,
            "switch": switches,
            "place": places,
            "switch_network": switch_network,
            "network_network_type": network_network_types,
            "network_type": network_types}

def cmdb_get_networks(cmdb_url, token, session_id):
    """
    Загрузка локальных сетей

    :param token: Токен доступа
    :param session_id: Идентификатор сессии
    :param net_types: Типы сетей
    :return: Список локальных сетей, None - в случае ошибки
    """

    attr = ['VlanID', 'Code']
    networks = cmdb_comm_get_class_cards(cmdb_url, token, "Network", attr)

    # В случае ошибки загрузки данных - возвращаем ошибку
    if networks is None:
        return None

    # Генерируем и возвращаем словарь с данными
    return {key: {"id": key,
                  "session_id": session_id,
                  "name": networks[key]["Code"],
                  "vlan_id": networks[key]["VlanID"]}
            for key in networks.keys()}


    """
    net = cmdb_comm_get_class_cards(token, "Network", ['VlanID'])
    relations = cmdb_comm_get_domain_cards(token, 'NetworkTypeNetwork')

    # В случае ошибки загрузки данных - возвращаем ошибку
    if net is None or relations is None:
        return None

    # Если каких-то данных нет - возвращаем пустой словарь
    if len(net) == 0 or len(relations) == 0:
        return dict()

    # Генерируем и возвращаем словарь с данными
    return {relations[key]["_destinationId"]:
                  {"id": relations[key]["_destinationId"],
                   "session_id": session_id,
                   "vlan_id": net[relations[key]["_destinationId"]]["VlanID"],
                   "type_id": relations[key]["_sourceId"]}
              for key in relations.keys()}
    """

def cmdb_get_network_network_type(cmdb_url, token, session_id):
    """
    Загрузка связей "сеть - тип сети"

    :param cmdb_url: URL REST-API CMDB
    :param token: Токен доступа
    :param session_id: Идентификатор сессии
    :return: Список связей сетей с типами сетей, None - ошибка
    """

    network_relations = cmdb_comm_get_domain_cards(cmdb_url, token, 'NetworkTypeNetwork')

    # В случае ошибки загрузки данных - возвращаем ошибку
    if network_relations is None:
        return None

    # Генерируем и возвращаем словарь с данными
    return {key: {"id": key,
                  "session_id": session_id,
                  "network": network_relations[key]["_destinationId"],
                  "network_type": network_relations[key]["_sourceId"]} for key in network_relations.keys()}

def cmdb_get_network_types(cmdb_url, token, session_id):
    """
    Загрузка типов подсетей

    :param cmdb_url: URL REST-API CMDB
    :param token: Токен доступа
    :param session_id: Идентификатор сессии

    :return: Список типов подсетей, None - в случае ошибки
    """

    network_types = cmdb_comm_get_class_cards(cmdb_url, token, "NetworkType", ['Code'])

    # В случае ошибки загрузки данных - возвращаем ошибку
    if network_types is None:
        return None

    # Генерируем и возвращаем словарь с данными
    return {key: {"id": key,
                  "session_id": session_id,
                  "const": network_types[key]["Code"]}
            for key in network_types.keys()}

def cmdb_get_workplaces(cmdb_url, token):
    """
    Загрузка списка рабочих мест

    :param cmdb_url: URL REST-API CMDB
    :param token: Токен доступа

    :return: Список рабочих мест, None - в случае ошибки
    """
    attr = ['Office', 'WorkplaceType']
    workplaces = cmdb_comm_get_class_cards(cmdb_url, token, "Workplace", attr)
    workplace_type = cmdb_comm_get_lookup_type_values(cmdb_url, token, "WorkplaceType")

    if workplaces is None or workplace_type is None:
        return None

    #for key in workplaces.keys():
        #print(workplaces[key])
        #print(workplaces[key].keys())
        #print(workplace_type)

    #    for ck in workplaces[key].keys():
    #        try:
    #            g = workplaces[key][ck] if ck != "WorkplaceType" else workplace_type[workplaces[key][ck]]["code"]
    #        except:
    #            print('--------')
    #            print(workplaces[key])
    #            print(workplaces[key][ck])
    #            print(ck)
    #            print(key)
    #            print('--------')

    # Перепаковываем словарь (добавляем константу типа рабочего места)
    return {key: {ck: workplaces[key][ck] if ck != "WorkplaceType" else workplace_type[workplaces[key][ck]]["code"]
                  for ck in workplaces[key].keys()}
            for key in workplaces.keys()}

def cmdb_get_devices(cmdb_url, token, session_id, workplaces, network_types):
    """
    Загрузка сетевых устройств

    :param cmdb_url: URL REST-API CMDB
    :param token: Токен доступа
    :param session_id: Идентификатор сессии

    :return: Список сетевых устройств, None - в случае ошибки
    """
    retVal = dict()
    attr = ['Code', 'Workplace', 'MACAddress', 'NetworkType']

    if workplaces is None:
        return None

    for device_type in cmdb_device_types.keys():
        devices = cmdb_comm_get_class_cards(cmdb_url, token, device_type, attr)

        if devices is None:
            return None

#        print('1111')
#        for key in devices.keys():
#            if devices[key]["Workplace"] is None:
#               print(devices[key]["MACAddress"])

#        device_list = {key: {"id": key,
#                             "session_id": session_id,
#                             "place": workplaces[devices[key]["Workplace"]]['Office'] if devices[key]["Workplace"] is not None else None,
#                             "default_net_type_const": cmdb_device_types[device_type]["net_type"],
#                             "custom_net_type_const": network_types[devices[key]["NetworkType"]]["const"] if devices[key]["NetworkType"] is not None else None,
#                             "mac": devices[key]["MACAddress"],
#                             "inventory": devices[key]["Code"]}
#                       for key in devices.keys() if workplaces[devices[key]["Workplace"]]["WorkplaceType"] not in cmdb_ignore_worklaces}


        # Генерируем и словарь с данными
        device_list = {key: {"id": key,
                             "session_id": session_id,
                             "place": workplaces[devices[key]["Workplace"]]['Office'] if devices[key]["Workplace"] is not None else None,
                             "default_net_type_const": cmdb_device_types[device_type]["net_type"],
                             "custom_net_type_const": network_types[devices[key]["NetworkType"]]["const"] if devices[key]["NetworkType"] is not None else None,
                             "mac": devices[key]["MACAddress"],
                             "inventory": devices[key]["Code"]}
                       for key in devices.keys() if workplaces[devices[key]["Workplace"]]["WorkplaceType"] not in cmdb_ignore_worklaces}

        # Формируем общий словарь, который вернем
        retVal.update(device_list)

    # Генерируем и возвращаем словарь с данными
    return retVal

def cmdb_get_switches(cmdb_url, token, session_id, workplaces):
    """
    Загрузка списка коммутаторов

    :param cmdb_url: URL REST-API CMDB
    :param token: Токен доступа
    :param session_id: Идентификатор сессии
    :param workplaces: Список рабочих мест

    :return: Список коммутаторов, None - в случае ошибки
    """
    if workplaces is None:
        return None

    attr = ['Workplace', 'Hostname', 'IPAddress', 'SwitchKey', 'RadiusKey', 'Code']
    switches = cmdb_comm_get_class_cards(cmdb_url, token, 'SwitchCI', attr)

    if switches is None:
        return None

    # Генерируем и возвращаем словарь с данными
    return {key: {"id": key,
                  "session_id": session_id,
                  "place": workplaces[switches[key]["Workplace"]]["Office"] if switches[key]["Workplace"] is not None else None,
                  "hostname": switches[key]["Hostname"],
                  "inventory": switches[key]["Code"],
                  "ip_address": switches[key]["IPAddress"],
                  "radius_key": switches[key]["RadiusKey"],
                  "auth_key": switches[key]["SwitchKey"]}
            for key in switches.keys() if workplaces[switches[key]["Workplace"]]["WorkplaceType"] not in cmdb_ignore_worklaces}

def cmdb_get_switch_network_relation(cmdb_url, token, session_id):
    """
    Загрузка связи коммутаторов с сетями

    :param cmdb_url: URL REST-API CMDB
    :param token: Токен доступа
    :param session_id: Идентификатор сессии

    :return: Список связей, None - в случае ошибки
    """
    relations = cmdb_comm_get_domain_cards(cmdb_url, token, 'NetworkSwitchCI')

    if relations is None:
        return None

    # Генерируем и возвращаем словарь с данными
    return {key: {"id": key,
                  "session_id": session_id,
                  "switch": relations[key]["_destinationId"],
                  "network": relations[key]["_sourceId"]} for key in relations.keys()}

def cmdb_get_places(cmdb_url, token, session_id):
    """
    Загрузка списка помещений

    :param cmdb_url: URL REST-API CMDB
    :param token: Токен доступа
    :param session_id: Идентификатор сессии

    :return: Список помещений, None - в случае ошибки
    """
    attr = ['Code',]
    places = cmdb_comm_get_class_cards(cmdb_url, token, 'Office', attr)

    if places is None:
        return None

    # Генерируем и возвращаем словарь с данными
    return {key: {"id": key,
                  "session_id": session_id,
                  "code": places[key]["Code"]}
            for key in places.keys()}

def cmdb_comm_auth(cmdb_url, cmdb_user, cmdb_pwd):
    """
    Авторизация в cmdbuild

    :param cmdb_url: URL REST-API CMDB
    :param cmdb_user: Пользователь CMDB
    :param cmdb_pwd: Пароль пользователя CMDB

    :return: Токен доступа, None при ошибке
    """
    try:
        r = requests.post("".join([cmdb_url, '/sessions']), json={"username": cmdb_user, "password": cmdb_pwd})
        auth = {"CMDBuild-Authorization": r.json()["data"][cmdb_index_column]}
        return auth

    except requests.exceptions.RequestException as e:
        logging.error(str(e))

    return None

def cmdb_comm_get_domain_cards(cmdb_url, token, name):
    """
    Загрузка связи типа M-M

    :param cmdb_url: URL REST-API CMDB
    :param token: Токен доступа
    :param name: Наименование связи

    :return: Список связей, None - в случае ошибки
    """

    try:
        cards = requests.get("".join([cmdb_url, '/domains/', name, '/relations']), headers=token)
        cards = cards.json()["data"]

        # Генерируем словарь
        #rows = [{attr: card[attr] for attr in [cmdb_index_column, "_sourceId", "_destinationId"]} for card in cards]
        rows = {card[cmdb_index_column]: {(attr if attr != cmdb_index_column else 'id'): card[attr] for attr in [cmdb_index_column, "_sourceId", "_destinationId"]} for card in cards}

        return rows if len(rows) > 0 else dict()

    except requests.exceptions.RequestException as e:
        logging.error(str(e))

    return None

def cmdb_comm_get_class_cards(cmdb_url, token, name, attr):
    """
    Загрузка карточек класса

    :param cmdb_url: URL REST-API CMDB
    :param token: Токен доступа
    :param name: Наименование класса
    :param attr: Список загружаемых атрибутов

    :return: Список карточек класса, None - в случае ошибки
    """
    if cmdb_comm_check_class_attr(cmdb_url, token, name, attr) == False:
        return None

    full_attr = list(attr)

    if cmdb_index_column not in full_attr:
        full_attr.append(cmdb_index_column)

    try:
        cards = requests.get("".join([cmdb_url, '/classes/', name, '/cards']), headers=token)
        cards = cards.json()["data"]

        # Генерируем словарь
        rows = {card[cmdb_index_column]: {(a if a != cmdb_index_column else 'id'): card[a]
                                          for a in full_attr}
                for card in cards}
        return rows if len(rows) > 0 else dict()

    except requests.exceptions.RequestException as e:
        logging.err(str(e))

    return None

def cmdb_comm_get_lookup_type_values(cmdb_url, token, name):
    """
    Загрузка значений перечислений

    :param cmdb_url: URL REST-API CMDB
    :param token: Токен доступа
    :param name: Наименование перечисления

    :return: Список значений перечисления, None - в случае ошибки
    """

    attr = [cmdb_index_column, 'code', 'description']

    try:
        values = requests.get("".join([cmdb_url, '/lookup_types/', name, '/values']), headers=token)
        values = values.json()["data"]

        # Генерируем словарь
        rows = {value[cmdb_index_column]: {(a if a != cmdb_index_column else 'id'): value[a] for a in attr} for value in values}
        return rows if len(rows) > 0 else dict()

    except requests.exceptions.RequestException as e:
        logging.err(str(e))

    return None

def cmdb_comm_check_class_attr(cmdb_url, token, name, attr_list):
    """
    Проверка атрибутов класса

    :param cmdb_url: URL REST-API CMDB
    :param token: Токен доступа
    :param name: Наименование класса
    :param attr_list: Список атрибутов

    :return: True - успешная проверка, False - ошибка
    """

    retVal = set(attr_list)
    curr_attr = set(cmdb_comm_get_class_attr(cmdb_url, token, name))

    if retVal.issubset(curr_attr):
        return True

    return False

def cmdb_comm_get_class_attr(cmdb_url, token, name):
    """
    Загрузка атрибутов класса

    :param cmdb_url: URL REST-API CMDB
    :param token: Токен доступа
    :param name: Наименование класса

    :return: Список атрибутов класса
    """
    try:
        attr = requests.get("".join([cmdb_url, '/classes/', name, '/attributes']), headers=token)
        retVal = [attr["name"] for attr in attr.json()["data"]]
        return retVal

    except requests.exceptions.RequestException as e:
        logging.error(str(e))

    return None

def comm_get_params():
    """
    Парсинг параметров командной строки

    :return: Переданные параметры командной строки
    """

    parser = argparse.ArgumentParser(description='Integration module with CMDB')
    subparser = parser.add_subparsers()

    # Update command
    parser_a = subparser.add_parser('update', help='Update local DB from CMDB')
    # -->
    parser_a.add_argument('-сh', dest='q_cmdb_url', type=str, help='URL CMDB DB')
    parser_a.add_argument('-cu', dest='q_cmdb_user', type=str, help='CMDB User')
    parser_a.add_argument('-cp', dest='q_cmdb_pwd', type=str, help='CMDB User Password')
    # -->
    parser_a.add_argument('-dh', dest='q_db_host', type=str, help='DB Hostname')
    parser_a.add_argument('-du', dest='q_db_user', type=str, help='DB User')
    parser_a.add_argument('-dp', dest='q_db_pwd', type=str, help='DB User Password')
    parser_a.add_argument('-dn', dest='q_db_name', type=str, help='DB Name')
    # -->
    parser_a.set_defaults(cmd='update')

    # Check command
    parser_b = subparser.add_parser('check', help='Check device')
    # -->
    parser_b.add_argument('-m', dest='q_mac', type=str, help='MAC Address')
    parser_b.add_argument('-i', dest='q_nasip', type=str, help='NAS IP Address', required=False)
    parser_b.add_argument('-k', dest='q_key', type=str, help='Auth Key', required=False)
    # -->
    parser_b.set_defaults(cmd='check')

    # Get VLAN command
    parser_c = subparser.add_parser('get-vlan', help='Get device VLAN')
    # -->
    parser_c.add_argument('-m', dest='q_mac', type=str, help='MAC Address')
    parser_c.add_argument('-i', dest='q_nasip', type=str, help='NAS IP Address')
    parser_c.add_argument('-p', dest='q_nas_port', type=str, help='NAS Port')
    parser_c.add_argument('-n', dest='q_nas_name', type=str, help='NAS Name')
    parser_c.add_argument('-k', dest='q_key', type=str, help='Auth Key')
    # -->
    parser_c.set_defaults(cmd='get-vlan')

    # Query command
    parser_d = subparser.add_parser('query', help='Run query in the local DB')
    # -->
    parser_d.add_argument('-q', dest='q_sql', type=str, help='SQL Query')
    # -->
    parser_d.set_defaults(cmd='query')

    # Если параметры не переданы - печатаем help
    if len(sys.argv) == 1:
        parser.print_help()
        return None

    else:
        return parser.parse_args()

def fr_get_auth(mac, nas_ip, nas_port, nas_name, auth_key):
    """
    Возвращает статус авторизации устройства

    :param mac: MAC адрес
    :param nas_ip: IP адрес устройства NAS
    :param nas_port: Порт устройства NAS
    :param nas_name: Имя устройства NAS
    :param auth_key: Ключ аутентификации

    :return: Идентификатор VLAN - успешная аутентификация, None - ошибка аутентификации
    """
    retVal = None

    # Корректировка переданных параметров
    mac = mac.replace('\'', '').replace('"', '')
    nas_ip = nas_ip.replace('\'', '').replace('"', '')
    nas_port = nas_port.replace('\'', '').replace('"', '')
    nas_name = nas_name.replace('\'', '').replace('"', '')
    auth_key = auth_key.replace('\'', '').replace('"', '')

    res = db_get_device_info(mac, nas_ip, auth_key)

    if res is not None and len(res["data"]) == 1:
        retVal = res["data"][0][2]

    db_save_auth_log(mac, nas_ip, nas_port, nas_name, True if retVal is not None else False)
    return retVal

# Entry Point
if __name__ == "__main__":
    args = comm_get_params()

    if args is not None:
        if args.cmd == 'update':
            import_data(args.q_cmdb_url, args.q_cmdb_user, args.q_cmdb_pwd, args.q_db_host, args.q_db_user, args.q_db_pwd, args.q_db_name)

        elif args.cmd == 'check':
            res = db_get_device_info(args.q_mac, args.q_nasip, args.q_key)

            if res is not None:
                if len(res["data"]) != 0:
                    print(delimiter.join(r[0] for r in res["columns"]))

                    for r in res["data"]:
                        print(delimiter.join([str(rr) for rr in r]))

        elif args.cmd == 'get-vlan':
            print(fr_get_auth(args.q_mac, args.q_nasip, args.q_nas_port, args.q_nas_name, args.q_key))

        elif args.cmd == 'query':
            db_execute_query(args.q_sql)
