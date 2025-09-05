import os, sys
import psycopg2, psycopg2.extras
import argparse, json
import boto3
import pandas as pd
import er_db, er_aws, er_utils
import click
import zipfile

cur_dir = os.path.dirname(os.path.abspath(__file__))

keys = [s.lower() for s in er_db.get_db_keys()]
parser = argparse.ArgumentParser(prog='get-etf')
parser.add_argument('--env', dest='env', choices=keys, required=True)
parser.add_argument('--id', dest='id', required=False, type=int)
parser.add_argument('-a', dest='audit_id', required=False, type=int)
parser.add_argument('-p', dest='portfolio', action='store_true')
parser.add_argument('-x', dest='to_excel', action='store_true')

args = parser.parse_args()
if args.id is None and args.audit_id is None:
    print('Either audit id or id is needed.')
    sys.exit(-1)

db_info = er_db.get_db_info(args.env)
bucket = db_info['IMPORT_S3_BUCKET']

prefix = None
audit_trail = []

aws_session = er_aws.AwsSession(os.environ['S3_ACCESS_KEY'], os.environ['S3_SECRET_KEY'])


def get_file_list_from_prefix(pfx):
    if not pfx:
        return None
    return aws_session.list_files_from_s3(Bucket=bucket, Prefix=pfx)


audit_id = args.audit_id

with er_db.get_db_conn(args.env) as conn:
    if audit_id is None:
        audit_id, portfolio = er_db.get_audit_id_v2(conn, args.id, args.portfolio)
        print(audit_id)

    prefix = er_db.get_file_path(conn, audit_id)
    print(f'Prefix: {prefix}')
    audit_trail = er_db.get_audit_trail(conn, audit_id)
    print(audit_trail)

file_list = get_file_list_from_prefix(prefix)
if file_list is None:
    with er_db.get_db_conn(args.env) as conn:
        for ad in audit_trail:
            prefix = er_db.get_file_path(conn, ad)
            file_list = get_file_list_from_prefix(prefix)
            if file_list is not None:
                break

if file_list is None:
    sys.exit(-1)

etfs = file_list[file_list['Key'].str.contains('\\.etf$')].copy().reset_index().drop('index', axis=1)
etfs['Item'] = etfs.index + 1

if etfs.empty:
    sys.exit(-1)

cols = ['Item', 'Key', er_utils.lm_utc, er_utils.lm_ist]
if len(etfs) == 1:
    er_utils.tabulate_df(etfs[cols])
    file = etfs.iloc[0]['Key']
else:
    er_utils.tabulate_df(etfs[cols])
    item_num = input(
        f'\nThere are multiple items matching your query. Please specify the file you want to download. (1 - {len(etfs)})'
    )
    item_num = int(item_num)
    file = etfs.loc[etfs.Item == item_num]['Key'].iloc[0]

out_file = f'{audit_id}.etf'
print(out_file)
aws_session.download_file(bucket, file, out=out_file)

if not args.to_excel:
    sys.exit(0)

if (not os.path.exists(out_file)):
    sys.exit(0)

pfx, extn = os.path.splitext(out_file)
excel_name = f'{pfx}.xlsx'
writer = pd.ExcelWriter(excel_name, engine='xlsxwriter')
with zipfile.ZipFile(out_file, 'r') as zf:
    names = zf.namelist()
    for name in names:
        sheet_name = name.split('.')[0]
        print(f'Preparing sheet {sheet_name}...')
        with zf.open(name, 'r') as f:
            df = pd.read_csv(f)
            try:
                df.to_excel(writer, sheet_name=sheet_name)
            except ValueError:
                print(f'skipping {sheet_name}')
writer.close()
print(f'Excel file: {excel_name}')
