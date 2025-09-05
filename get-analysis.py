import os, sys
import psycopg2, psycopg2.extras
import argparse, json
import pandas as pd
import er_db
import shutil
import subprocess
import event_info

rscript = shutil.which('Rscript')

cur_dir = os.path.dirname(os.path.abspath(__file__))
keys = [s.lower() for s in er_db.get_db_keys()]
parser = argparse.ArgumentParser(prog='get-etf')
parser.add_argument('--env', dest='env', choices=keys, default='uat')
parser.add_argument('--id', dest='id', required=True, type=int)

args = parser.parse_args()
db_info = er_db.get_db_info(args.env)
user_id = 5

with er_db.get_db_conn(args.env) as conn:
    try:
        analysis = er_db.fetch_one(conn, f'select * from "GetAnalysis"({args.id}, {user_id})')
        obj = analysis['analysis_object']
        
        event = obj['Events'][0]
        event_id = event['EventID']
        sev_model_id = event['SeverityModelID']
        name = event['EventName']
        event_info.get_event_shape(conn, sev_model_id, event_id, args.id)
    except Exception as e:
        print(f'Exception: {e}')
        
