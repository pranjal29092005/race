import os, sys, json, argparse
import er_db
import psycopg2, psycopg2.extras
from psycopg2.extras import DictCursor

parser = argparse.ArgumentParser(prog='create-race-test-from-alert')
parser.add_argument('--env', dest='env', required=True, type=str, choices=['alerts', 'alerts2'])
parser.add_argument('--exp-id', dest='exp_id', required=False, type=int)
parser.add_argument('--event-id', dest='event_id', required=False, type=int)
parser.add_argument('--id', dest='alert_id', required=False, type=int)

argv = parser.parse_args()

if not argv.alert_id and not alert.event_id and not alert.exp_id:
    print('none of alert id, event id or exp id specified')
    sys.exit(-1)

env = 'prod' if argv.env == 'alerts' else 'prod2'
out_name = f'alert_{env}_{argv.exp_id}_{argv.event_id}.json'

alert_json = None
with er_db.get_db_conn(env) as conn:
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(
        f'select alert_json from race.b_alert_processing_data_detail where event_id={argv.event_id} and exp_id={argv.exp_id}'
    )
    rows = cursor.fetchall()
    if rows:
        alert_json = rows[0]['alert_json']

if alert_json:
    with open(out_name, 'w') as f:
        f.write(json.dumps(alert_json, indent=4))
        print(f'Alert json written to {out_name}')
