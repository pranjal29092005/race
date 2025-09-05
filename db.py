import os
import json
import psycopg2
from psycopg2.extras import DictCursor

def load_db_config():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(cur_dir, "db_config.json")
    with open(config_file, "r") as f:
        db_config = json.load(f)
    return db_config

def get_db_connection(env = "prod"):
    db_config = load_db_config()[env]
    conn_params = f"host={db_config['host']} port=5432 user={db_config['user']} password={db_config['pw']} dbname={db_config['database']}"
    return psycopg2.connect(conn_params)

def run_query(conn, query, **kwargs):
    cursor_factory = kwargs.get("cursor_factory", DictCursor)
    fetch_all = kwargs.get("fetch_all", False)
    with conn.cursor(cursor_factory=cursor_factory) as curs:
        curs.execute(query)
        if fetch_all:
            result = curs.fetchall()
        else:
            result = curs.fetchone()
    return result