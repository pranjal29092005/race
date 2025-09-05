import os, sys, json
import er_db, er_aws, argparse
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import er_utils

parser = argparse.ArgumentParser(prog='get-eg-details')
parser.add_argument('--env', '-e', dest='env', type=str, required=True, choices=['prod', 'integration', 'alpha'])
parser.add_argument('--id', '-i', dest='eg_id', type=int, required=True)

argv = parser.parse_args()

query = f'select portfolio_ids from get_portfolios_exposure_group({argv.eg_id})'
query = f'select portfolio_name, portfolio_id, audit_id from race.m_portfolio where portfolio_id in ({query}) order by portfolio_id'

items = []
with er_db.get_db_conn(argv.env) as conn:
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query)
    res = cursor.fetchall()
    for r in res:
        item = {}
        for k, v in r.items():
            item[k] = v
        items.append(item)

er_utils.tabulate_df(pd.DataFrame(items))
