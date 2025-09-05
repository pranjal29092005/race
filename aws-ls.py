#!/usr/bin/env python
import os, sys, json, subprocess
import argparse, er_db, hashlib

parser = argparse.ArgumentParser(prog='aws-ls')
parser.add_argument('--id', '-i', dest='exp_id', type=int, required=False)
parser.add_argument('--audit', '-a', dest='audit_id', type=int, required=False)
parser.add_argument('--env', choices=['alpha', 'integration', 'prod'], required=True)
parser.add_argument('--portfolio', '-p', action='store_true', dest='portfolio')

argv = parser.parse_args()

if (not argv.exp_id and not argv.audit_id):
    print('Audit or schedule id is expected')
    sys.exit(-1)

if not argv.audit_id:
    with er_db.get_db_conn(argv.env) as conn:
        audit_id, portfolio = er_db.get_audit_id_v2(conn, argv.exp_id, argv.portfolio)
else:
    audit_id = argv.audit_id

cur_dir = os.path.dirname(os.path.abspath(__file__))
db_config_file = os.path.join(os.path.dirname(cur_dir), 'Aws.cfg')
conf = None

with open(db_config_file, 'r') as f:
    conf = json.load(f)
env_key = argv.env.capitalize()
bucket = conf[env_key]['ASSET_S3_BUCKET']

sch_id = 0 if argv.portfolio else argv.exp_id
print(f'{audit_id}_{sch_id}')
prefix = hashlib.md5(bytes(f'{audit_id}_{sch_id}', 'utf-8')).hexdigest().upper()
cmd = f'aws s3 ls s3://{bucket}/{prefix}/'
print(cmd)
subprocess.call(cmd.split())
