import os, sys, json
import msgpack, argparse
import er_utils
import pandas as pd
import subprocess, shutil
import numpy as np
import er_db, click
import glob, hashlib

python = shutil.which('python')

parser = argparse.ArgumentParser(prog='change-section-name-to-layer-number')
parser.add_argument('--env', dest='env', required=True, type=str)
parser.add_argument('--id', dest='id', required=False, type=int)
parser.add_argument('--audit-id', '-a', dest='audit_id', required=False, type=int)

cur_dir = os.path.dirname(os.path.abspath(__file__))

argv = parser.parse_args()

if argv.id:
    with er_db.get_db_conn(argv.env) as conn:
        audit_id, _ = er_db.get_audit_id_v2(conn, argv.id, True)
else:
    audit_id = argv.audit_id

if audit_id is None:
    click.secho('Could not find audit id', fg='red')
    sys.exit(-1)

click.secho(f'Processing audit id: {audit_id}', fg='black', bg='green', bold=True)
subprocess.call(
    [
        python,
        os.path.join(cur_dir, 'get-exp-binaries.py'), '--env', argv.env, '--audit-id',
        str(audit_id), '-p', '--no-parquet'
    ]
)

exp_dir = os.path.join(os.environ['RACE_MASTER_FOLDER'], 'AssetScheduleDataFolder', argv.env.lower(), f'{audit_id}_0')

section_name_file = os.path.join(exp_dir, 'data_f_coverage_section_name.bin')
if not os.path.exists(section_name_file):
    click.secho('No section name file found. Nothing to do', bg='blue', fg='white')
    sys.exit(0)

click.secho('Converting data ...', fg='blue', nl=False)
with open(section_name_file, 'rb') as f:
    section_data = msgpack.unpack(f)


def str_to_int(x):
    if x is None or x == '0' or x == '':
        return None
    try:
        return (int(x))
    except:
        return None


modified = []
for v in section_data:
    modified.append(str_to_int(v))

with open(os.path.join(exp_dir, 'data_f_coverage_layer_number.bin'), 'wb') as f:
    msgpack.pack(modified, f)

done_txt = os.path.join(exp_dir, 'done.txt')
if os.path.exists(done_txt):
    os.remove(done_txt)
click.secho(' done', fg='blue')

exp_7z = os.path.join(os.environ['HOME'], f'{audit_id}_0.7z')
subprocess.call(['7z', 'a', '-y', exp_7z, '*.bin'], cwd=exp_dir)

click.secho('Uploading file to s3 ... ', fg='blue', nl=False)

conf = er_db.get_db_info(argv.env)
bucket = conf['ASSET_S3_BUCKET']
key = f'{audit_id}_0'
prefix = hashlib.md5(bytes(key, 'utf-8')).hexdigest().upper()
s3_path = f's3://{bucket}/{prefix}/{key}.7z'

subprocess.call(['aws', 's3', 'cp', exp_7z, s3_path])

click.secho('done', fg='blue')
