import os, sys, json
import msgpack
from pyarrow import parquet as pq
import argparse
import er_db, er_aws
import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor
import subprocess, shutil
import struct
import pandas as pd

parser = argparse.ArgumentParser(prog='get-zipcode-data')
parser.add_argument('--exp-id', type=int, required=False, dest='exp_id')
parser.add_argument('--eg-id', type=int, required=False, dest='eg_id')
parser.add_argument('--env', type=str, required=True, dest='env')
parser.add_argument('-p', action='store_true', dest='portfolio')

cur_dir = os.path.dirname(os.path.abspath(__file__))
race_cfg_file = os.path.join(os.path.dirname(cur_dir), 'Race.cfg')

conf = {}

with open(race_cfg_file, 'r') as f:
    conf = json.load(f)

argv = parser.parse_args()
if not argv.exp_id and not argv.eg_id:
    print('Exposure/Exposure group id not specified. Bailing out')
    sys.exit(-1)

exps = []

if argv.exp_id:
    exps.append(argv.exp_id)

master_dir = os.environ.get('RACE_MASTER_FOLDER')


def get_exp_dir(audit_id):
    return os.path.join(master_dir, conf['AssetScheduleDataFolder'], argv.env.lower(), f'{audit_id}_0')


def read_zipcode_file(fn):
    out = []
    with open(fn, 'rb') as f:
        header = f.read(32)
        header_size, num_elems, size_per_elem, version = struct.unpack('4q', header)
        data = []
        if version == 3:
            data = msgpack.unpackb(f.read(header_size))
            data = list(map(lambda s: s[1:], data))
        zc = msgpack.unpack(f)
        for z in zc:
            if z >= len(data):
                out.append('')
            else:
                out.append(data[z])
    return out


if argv.eg_id:
    query = f"select portfolio_ids from get_portfolios_exposure_group({argv.eg_id})"
    query = f"select portfolio_id from race.m_portfolio where portfolio_id in ({query}) order by portfolio_id"
    with er_db.get_db_conn(argv.env) as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        res = cursor.fetchall()
        for r in res:
            exps.append(r['portfolio_id'])

exp_data = {}
to_download = []
for exp in exps:
    with er_db.get_db_conn(argv.env) as conn:
        audit_id = er_db.get_audit_id_v2(conn, exp, True)[0]
        exp_dir = get_exp_dir(audit_id)
        zip_code = os.path.join(exp_dir, 'm_zipcode.bin')
        state_code = os.path.join(exp_dir, 'm_state_code.bin')
        country_code = os.path.join(exp_dir, 'm_country_code.bin')
        if not os.path.exists(zip_code):
            to_download.append(exp)
        exp_data[exp] = {
            'portfolio_id': exp,
            'audit_id': audit_id,
            'exp_dir': exp_dir,
            'zipcode_file': zip_code,
            'state_file': state_code,
            'country_file': country_code
        }

for exp in to_download:
    py = shutil.which('python')
    fn = os.path.join(cur_dir, 'get-exp-binaries.py')
    subprocess.call([py, fn, '--id', exp, '-p', '--env', argv.env])

dfs = []
for k, v in exp_data.items():
    zip_code = read_zipcode_file(v['zipcode_file'])
    state_code = read_zipcode_file(v['state_file'])
    country_code = read_zipcode_file(v['country_file'])
    df = pd.DataFrame({'zip_code': zip_code, 'state': state_code, 'country': country_code})
    df['portfolio_id'] = v['portfolio_id']
    df['audit_id'] = v['audit_id']
    dfs.append(df)

final_df = pd.concat(dfs)
final_df.to_csv('codes.csv', index=False)
