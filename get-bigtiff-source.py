import os, sys, json
import argparse, hashlib
from icecream import ic
import er_db, er_aws, er_utils
import psycopg2.extras
import subprocess

parser = argparse.ArgumentParser(prog='get-bigtiff-source')
parser.add_argument('--env', dest='env', choices=['prod', 'integration', 'uat', 'alpha'], default='prod')
parser.add_argument('--id', dest='id', required=True)
parser.add_argument('-binfiles', dest='bin', action='store_true')
parser.add_argument('-tif', dest='tif', action='store_true')
parser.add_argument('--with-cli-script', dest='write_cli_script', action='store_true')
parser.add_argument('-config', dest='config', action='store_true')
argv = parser.parse_args()

cur_dir = os.path.dirname(os.path.abspath(__file__))
db_config_file = os.path.join(os.path.dirname(cur_dir), 'Aws.cfg')
race_config_file = os.path.join(os.path.dirname(cur_dir), 'Race.cfg')
conf = None
race_dirs = None

with open(db_config_file, 'r') as f:
    conf = json.load(f)

with open(race_config_file, 'r') as f:
    race_dirs = json.load(f)

env_key = argv.env.capitalize()
bucket = conf[env_key]['EVENT_S3_BUCKET']
query = f'select "EVENT_SEV_MODEL_ID" from race."B_EVENT_SEVERITY" where "EVENT_ID"={argv.id}'
sid = None

with er_db.get_db_conn(env_key) as conn:
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query)
    row = cursor.fetchone()
    sid = row['EVENT_SEV_MODEL_ID']
    if sid is None:
        sys.exit(-1)
    cursor.execute(f'select big_tiff_flag from "GetEventSeverity"({sid},{argv.id})')
    row = cursor.fetchone()
    if row['big_tiff_flag'] not in ['Y', 'y']:
        print("Not a bigtiff event")
        sys.exit(-1)

footprint_hash = hashlib.md5(bytes(f'{argv.id}_{sid}', 'utf-8')).hexdigest().upper()
ic(f's3://{bucket}/{footprint_hash}')

if race_dirs is None:
    out_dir = os.getcwd()
else:
    out_dir = os.path.join(os.environ['RACE_MASTER_FOLDER'], race_dirs['BigTiffEventsFolder'], argv.env)


def download(session, extn):
    out = f'{argv.id}_{sid}.{extn}'
    key = f'{footprint_hash}/{out}'
    print(f'Fetching {out}...')
    session.download_file(bucket, key, out=os.path.join(out_dir, out))
    if extn == '7z':
        print(f'Extracting bin files...')
        er_utils.extract_archive(os.path.join(out_dir, out), os.path.join(out_dir, f'{argv.id}_{sid}'))


def write_json():
    this_dir = os.path.join(out_dir, f'{argv.id}_{sid}')
    os.makedirs(this_dir, exist_ok=True)
    json_file = os.path.join(this_dir, 'input.json')
    obj = {
        'Tiff File': os.path.join(out_dir, f'{argv.id}_{sid}.tif'),
        'Environment': argv.env.capitalize(),
        'Tile Size': 1024,
        'Event Id': int(argv.id),
        'Severity Model Id': sid,
        'threshold': 1.0,
        'scalingFactor': 1.0
    }
    with open(json_file, 'w') as f:
        json.dump(obj, f, indent=4)


def write_cli_script():
    out_file = os.path.join(cur_dir, '..', 'build', f'bigtiff_create_{argv.id}')
    input_json = os.path.join(out_dir, f'{argv.id}_{sid}', 'input.json')
    with open(out_file, 'w') as f:
        f.write(
            f'''#!/usr/bin/env bash

source $PWD/scripts/{argv.env.lower()}.sh
$PWD/Tools/BigTiff/Cli/BigTiffCli --input={input_json} --event-id={int(argv.id)} --sev-model-id={sid}
        '''
        )
    subprocess.call(['chmod', '+x', out_file])


session = er_aws.AwsSession(os.environ['S3_ACCESS_KEY'], os.environ['S3_SECRET_KEY'])
if argv.bin or argv.tif:
    if argv.bin:
        download(session, '7z')
    if argv.tif:
        download(session, 'tif')
        write_json()
        if argv.write_cli_script:
            write_cli_script()

if argv.config:
    config_file = f'{footprint_hash}/config.json'
    config = session.read_json(bucket, config_file)
    if config is not None:
        ic(config)
    else:
        ic('No config')
