import os, sys
import er_db, er_utils
import argparse
import pandas as pd
import psycopg2
import psycopg2.extras

keys = [s.lower() for s in er_db.get_db_keys()]
parser = argparse.ArgumentParser(prog='get_events')
parser.add_argument('--set', dest='event_set_id', required=True, type=int)
parser.add_argument('--env', dest='env', choices=keys, default='uat')
parser.add_argument('--max-results', dest='max_results', default=10)

args = parser.parse_args()
db_info = er_db.get_db_info(args.env)

query = 'select * from race."M_EVENT" where "EVENT_SET_ID" in ({}) and "User_ID">0 order by "ID" desc limit {}'.format(
    args.event_set_id, args.max_results)

with er_db.get_db_conn(args.env) as conn:
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query)
    data = []
    for row in cursor:
        data.append({
            'id': row['ID'],
            'label': row['LABL'],
            'event_num': row['event_num'],
            'begin': row['EVENT_BEGIN_DATE']
        })
    er_utils.tabulate_df(pd.DataFrame(data))
