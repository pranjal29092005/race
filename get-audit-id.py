import os, sys
import argparse
import er_db
from psycopg2.extras import RealDictCursor

parser = argparse.ArgumentParser(prog='get-audit-id')
parser.add_argument('--env', dest='env', choices=['alpha', 'prod', 'integration'], type=str, required=True)
parser.add_argument('--id', dest='id', type=int, required=True)
parser.add_argument('-p', dest='portfolio', action='store_true')

argv = parser.parse_args()
audit_id = 0

with er_db.get_db_conn(argv.env) as conn:
    query = f'select audit_id from race.m_portfolio where portfolio_id={argv.id}'
    if not argv.portfolio:
        query = f'select "AUDIT_ID" as audit_id from race."M_ASSET_SCHEDULE" where "ID"={argv.id}'
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query)
    res = cursor.fetchone()
    print(res['audit_id'])
