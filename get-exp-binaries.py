import os, sys, json, glob
import er_aws, er_utils, er_db
import argparse, shutil
import hashlib
from icecream import ic
import subprocess, tempfile

parser = argparse.ArgumentParser(prog='get-exposure-binaries')
parser.add_argument('--env', dest='env', required=True, type=str, default='uat')
parser.add_argument('--id', dest='id', required=False, type=int)
parser.add_argument('--audit-id', '-a', dest='audit_id', required=False, type=int)
parser.add_argument('-p', action='store_true', dest='portfolio')
parser.add_argument('-t', action='store_true', dest='extract_to_tmp_dir')
parser.add_argument('--no-parquet', action='store_true', dest='ignore_parquet')

args = parser.parse_args()

if args.id is None and args.audit_id is None:
    print('Please specify audit id or (schedule/portfolio) id')
    sys.exit(-1)

db_info = er_db.get_db_info(args.env)
bucket = db_info['ASSET_S3_BUCKET']
aws_session = er_aws.AwsSession(os.environ['S3_ACCESS_KEY'], os.environ['S3_SECRET_KEY'])


def get_dest_dir():
    if args.extract_to_tmp_dir:
        return tempfile.gettempdir()
    master_folder = os.environ.get('RACE_MASTER_FOLDER')
    if master_folder is None:
        return os.getcwd()
    cur_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    race_cfg_file = os.path.join(cur_dir, 'Race.cfg')
    with open(race_cfg_file, 'r') as f:
        cfg = json.load(f)
        return os.path.join(master_folder, cfg['AssetScheduleDataFolder'], args.env)
    return os.getcwd()


def change_file_case(out_dir):
    files = glob.glob(os.path.join(out_dir, '*'))
    for f in files:
        base = os.path.basename(f)
        if base.endswith('parquet'):
            continue
        os.rename(f, os.path.join(out_dir, base.lower()))
    with open(os.path.join(out_dir, 'done.txt'), 'w') as f:
        pass


def fetch_valuation_parquet_files(bucket, prefix, out_dir):
    key = f'{prefix}/valuation.json'
    valuation_config = aws_session.read_json(bucket, key)
    if valuation_config is None:
        return
    perils = valuation_config['Cause Of Loss']
    for p in perils:
        key = f'{prefix}/valuation_{p}.parquet'
        print(f'Fetching {os.path.basename(key)}')
        aws_session.download_file(bucket, key, out=os.path.join(out_dir, os.path.basename(key)))


def fetch_reinsurance_parquet_files(bucket, prefix, out_dir):
    key = f'{prefix}/reinsurance.json'
    reins_config = aws_session.read_json(bucket, key)
    if reins_config is None:
        return
    for k, _ in reins_config['Checksum'].items():
        key = f'{prefix}/{k}'
        print(f'Fetching {os.path.basename(key)}')
        aws_session.download_file(bucket, key, out=os.path.join(out_dir, os.path.basename(key)))


with er_db.get_db_conn(args.env) as conn:
    if args.audit_id is not None:
        audit_id = args.audit_id
        is_portfolio = args.portfolio
    else:
        sch_id = args.id
        audit_id, is_portfolio = er_db.get_audit_id_v2(conn, sch_id, args.portfolio)
    if audit_id:
        engine = hashlib.md5()
        if is_portfolio:
            sch_id = 0
        engine.update(f'{audit_id}_{sch_id}'.encode('utf-8'))
        prefix = engine.hexdigest().upper()
        key = f'{prefix}/{audit_id}_{sch_id}.7z'
        ic(key)

        out_file = os.path.join(get_dest_dir(), os.path.basename(key))
        out_dir = os.path.splitext(out_file)[0]
        ic(out_dir)
        doneFile = os.path.join(out_dir, 'done.txt')
        if (os.path.exists(doneFile)):
            sys.exit(0)
        print(f'Downloading {out_file} from s3 ...')
        aws_session.download_file(bucket, key, out=out_file)
        print(f'Extracting {out_file} to {out_dir} ...')
        er_utils.extract_archive(out_file, out_dir)
        change_file_case(out_dir)
        with open(doneFile, 'w') as f:
            pass
        print('Extraction finished')
        if args.ignore_parquet:
            sys.exit(0)
        fetch_valuation_parquet_files(bucket, prefix, out_dir)
        fetch_reinsurance_parquet_files(bucket, prefix, out_dir)
