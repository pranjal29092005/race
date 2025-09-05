import os, sys
import er_db, er_utils
import argparse
import pandas as pd
import psycopg2
import psycopg2.extras

keys = [s.lower() for s in er_db.get_db_keys()]
parser = argparse.ArgumentParser(prog='get_events')
parser.add_argument(
    '--ids', dest='event_ids', required=True, nargs='+', type=int)
parser.add_argument('--env', dest='env', choices=keys, default='uat')

args = parser.parse_args()
event_ids = ','.join(map(str, args.event_ids))

query = 'update race."M_EVENT" set "User_ID"=-1 where "ID" in ({})'.format(
    event_ids)

with er_db.get_db_conn(args.env) as conn:
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query)
