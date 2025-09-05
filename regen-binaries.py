import os, sys
import pandas as pd
import click, subprocess
from icecream import ic
import shutil
import hashlib
import er_aws

fn = '/home/surya/Downloads/TMH active exposure_ regenerated binaries (1).csv'
df = pd.read_csv(fn)
python = shutil.which('python')
od_prefix = os.environ.get('RACE_BIN_SOURCES_DIR')

for idx, row in df.iterrows():
    aid = row['audit_id']
    exp_dir = os.path.join(od_prefix, f'{aid}_0')
    cfile = os.path.join(exp_dir, f'data_f_contract_{aid}_0.txt')
    if not os.path.exists(cfile):
        pass
        # click.secho(f'Portfolio data for {row["portfolio_name"]} ({aid}) does not exist', fg='white', bg='red')
    else:
        pass
    # os.makedirs(exp_dir, exist_ok=True)
    # cmd = [python, 'get-bin-sources.py', '--env', 'prod', '-a', str(aid), '-p']
    # subprocess.call(cmd)

bucket = 'prod-p-b-er'
session = er_aws.AwsSession(os.environ['S3_ACCESS_KEY'], ['S3_SECRET_KEY'])
'''
d = []
for idx, row in df.iterrows():
    click.secho(f'Checking for {row["portfolio_name"]} ({idx+1}/{len(df)})', fg='blue')
    aid = row['audit_id']
    key = hashlib.md5(bytes(f'{aid}_0', 'utf-8')).hexdigest().upper()

    status = {}
    for s in ['fac', 'treaty', 'cat_xol']:
        status['audit_id'] = aid
        status['portfolio_id'] = row['portfolio_id']
        status['portfolio_name'] = row['portfolio_name']
        this_key = f'{key}/{s}.parquet'
        cmd = (f'aws s3 ls s3://{bucket}/{this_key}')
        x = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = x.communicate()
        if len(out) == 0:
            status[s] = False
        else:
            status[s] = True
    d.append(status)

ndf = pd.DataFrame(d)
ic(ndf)
ndf.to_csv('~/bin_status.csv')
'''

fn = '/home/surya/bin-remaining.csv'
df = pd.read_csv(fn)
for idx, row in df.iterrows():
    aid = row['audit_id']
    pid = row['portfolio_id']
    od = os.path.join(os.environ['RACE_BIN_SOURCES_DIR'], f'{aid}_0')
    cwd = os.path.normpath(os.path.join(os.getcwd(), '../build'))
    cmd = [os.path.join(cwd, 'bin-create-test'), str(aid), 'prod']
    subprocess.call(cmd, cwd=os.path.join(os.getcwd(), '../build'))
