import os, sys, json
import rasterio
import argparse, er_db
from icecream import ic
import pandas as pd

this_file = os.path.abspath(__file__)
this_dir = os.path.dirname(this_file)
conf_json = os.path.join(os.path.dirname(this_dir), 'Aws.cfg')

parser = argparse.ArgumentParser(prog='get-event')
parser.add_argument('--env', '-e', dest='env', choices=['integration', 'alpha', 'prod'], required=True)
parser.add_argument('--id', '-i', dest='id', type=int, required=True)
argv = parser.parse_args()

env = argv.env.capitalize()
with open(conf_json, 'r') as f:
    conf = json.load(f)[env]

sev_id = None
fn = None
with er_db.get_db_conn(env) as conn:
    sev_id = er_db.get_sev_model_id(conn, argv.id)
    if sev_id is None:
        sys.exit(-1)
    query = f'select "SHAPE" from "GetEventSeverity"({sev_id},{argv.id})'
    res = er_db.exec_stmt(conn, query)
    data = res['SHAPE']
    fn = os.path.join(os.getcwd(), f'{argv.id}.tiff')
    with open(fn, 'wb') as f:
        f.write(data)

if fn is None:
    sys.exit(-1)

with rasterio.open(fn, 'r') as src:
    data = []
    data.append({'corner': 'NW', 'longitude': src.bounds.left, 'latitude': src.bounds.top})
    data.append({'corner': 'NE', 'longitude': src.bounds.right, 'latitude': src.bounds.top})
    data.append({'corner': 'SE', 'longitude': src.bounds.right, 'latitude': src.bounds.bottom})
    data.append({'corner': 'SW', 'longitude': src.bounds.left, 'latitude': src.bounds.bottom})
    fn = f'bbox_{argv.id}.csv'
    pd.DataFrame(data).to_csv(fn, index=False)
