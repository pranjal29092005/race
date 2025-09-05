import os, sys, json, argparse, msgpack, re
import subprocess, shutil, hashlib, click
import psycopg2
import sqlalchemy
from sqlalchemy.sql import text
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import er_aws, er_db
import tempfile
import numpy as np

race_dir = subprocess.check_output(['git', 'rev-parse', '--show-toplevel']).decode('utf-8').split('\n')[0]
aws_cfg = os.path.join(race_dir, 'Aws.cfg')

parser = argparse.ArgumentParser(prog='write-cause-of-loss-parquet')
parser.add_argument('--id', '-i', dest='exp_id', required=False, help='Exposure Id', type=int)
parser.add_argument('--audit-id', '-a', dest='audit_id', required=False, help='Audit Id', type=int)
parser.add_argument('--env', '-e', dest='env', required=True, help='Environment', choices=['prod', 'integration'])
argv = parser.parse_args()

if argv.exp_id is None and argv.audit_id is None:
    print('Either audit id or exposure id is needed to process cause of loss')
    sys.exit(-1)

aws_config = None
with open(aws_cfg, 'r') as f:
    aws_config = json.load(f)[argv.env.capitalize()]

asset_bucket = aws_config['ASSET_S3_BUCKET']
audit_id = argv.audit_id
if argv.exp_id:
    with er_db.get_db_conn(argv.env) as conn:
        audit_id, portfolio = er_db.get_audit_id(conn, argv.exp_id)
        if not portfolio:
            print(f'specified exposure id {argv.exp_id} is not a portfolio')
            sys.exit(-1)

exp_identifier = f'{audit_id}_0'
aws_prefix = hashlib.md5(exp_identifier.encode()).hexdigest().upper()
aws_key = f'{aws_prefix}/{exp_identifier}.7z'

python = shutil.which('python')
exp_bin_script = os.path.join(race_dir, 'Helpers', 'get-exp-binaries.py')

exp_bin_dir = os.path.join(tempfile.gettempdir(), exp_identifier)
if not os.path.exists(exp_bin_dir):
    subprocess.call([python, exp_bin_script, '--env', argv.env, '-p', '-t', '-a', str(audit_id)])


def get_peril_table():
    query = '''
select ph.peril_id, ph.sub_peril_id, ph.peril_name, ph.sub_peril_name, pp.peril_code peril_code, sp.peril_code sub_peril_code
FROM
(select peril_id,peril_name,sub_peril_id,sub_peril_name from "GetPerilHierarchy"()) ph
INNER JOIN
(select peril_id,peril_name, peril_code from "GetSubPerilData"()) sp
ON
ph.sub_peril_id = sp.peril_id
INNER JOIN
(select peril_id, peril_name, peril_code from "GetPerilData"()) pp
ON
ph.peril_id = pp.peril_id
    '''
    engine = er_db.get_sql_alchemy_engine(argv.env)
    with engine.connect() as conn:
        res = conn.execute(text(query))
        df = pd.DataFrame(res.fetchall())
        df = df[['peril_name', 'peril_code', 'sub_peril_name', 'sub_peril_code']]
        df = df[df.peril_code != 'ALLP']
    return df


def read_coverage_cause_of_loss(dir_name):
    fn = os.path.join(dir_name, 'p_base_terms_list_without_currency_conversion.bin')
    if (not os.path.exists(fn)):
        return None

    data = set()
    if not os.path.exists(fn):
        return None
    else:
        with open(fn, 'rb') as f:
            term_list = msgpack.unpack(f)
            terms = []
            for term in term_list:
                terms.append({'offset': term[6], 'number': term[7], 'term_type': term[9]})
            return pd.DataFrame(terms)


def read_layer_cause_of_loss(dir_name, exp_identifier):
    fn = os.path.join(dir_name, f'data_f_layer_{exp_identifier}_cause_of_loss.bin')
    if not os.path.exists(fn):
        return None

    data = set()
    if not os.path.exists(fn):
        return None
    else:
        with open(fn, 'rb') as f:
            cols = msgpack.unpack(f)
            for c in cols:
                data.update(re.split('\\+', c))
    return data


def check_entry_for_condition(s):
    return len(s.strip()) > 0


def read_condition_string_entries(dir_name):
    fn = os.path.join(dir_name, 'data_f_coverage_condition.bin')
    if (os.path.exists(fn)):
        with open(fn, 'rb') as f:
            entries = msgpack.unpack(f)
            df = pd.DataFrame({'entry': entries})
            df['has_condition'] = df['entry'].apply(check_entry_for_condition)
            return df
    return None


def get_coverage_col_ids(dir_name, col_df):
    fn = os.path.join(dir_name, 'data_f_coverage_cause_of_loss.bin')
    df = None
    with open(fn, 'rb') as f:
        data = msgpack.unpack(f)
        df = pd.DataFrame({'id': data})
    df['idx'] = range(0, len(df))
    df = df.merge(col_df, on='id', how='inner')
    df.sort_values(by='idx', inplace=True)
    return df[['cause of loss']]


def read_col_id_map(dir_name):
    fn = os.path.join(dir_name, 'coltoidmap.bin')
    with open(os.path.join(exp_bin_dir, 'coltoidmap.bin'), 'rb') as f:
        col_id_map = msgpack.unpack(f)
    cols = []
    for k, v in col_id_map.items():
        cols.append({'cause of loss': k, 'id': v})
    return pd.DataFrame(cols)


def get_table_from_coverages(dir_name):
    coverage_col = read_coverage_cause_of_loss(exp_bin_dir)
    conditions = read_condition_string_entries(exp_bin_dir)
    df = pd.concat([coverage_col, conditions], axis=1)
    df_with_out_exclusion = df[df.term_type != 1]
    df_with_exclusion = df.loc[(df['term_type'] == 1) & (df.has_condition == True)]
    df = pd.concat([df_with_out_exclusion, df_with_exclusion], axis=0)
    return df[['offset', 'number']]


def get_cause_of_loss_names_from_coverages(cov_table, col_names):
    names = set()
    for idx, row in cov_table.iterrows():
        offset = row['offset']
        num = row['number']
        for i in range(offset, offset + num):
            names.add(col_names.loc[i]['cause of loss'])
    return names


def get_cause_of_loss_names_from_valuation(bucket, exp_identifier, s3_session):
    valuation_json_key = f'{aws_prefix}/valuation.json'
    config = s3_session.read_json(bucket, valuation_json_key)
    if config is None:
        return set()
    items = set(config.get('Cause Of Loss', []))
    items.discard('ALLP')
    return items


def separate_codes(col_names, peril_table):
    peril_list = peril_table.groupby('peril_code').agg(peril_name=('peril_name', 'min')).reset_index()
    peril_set = set(peril_list['peril_code'].unique())

    sub_peril_list = peril_table.groupby('sub_peril_code').agg(sub_peril_name=('sub_peril_name', 'min')).reset_index()
    sub_peril_set = set(sub_peril_list['sub_peril_code'].unique())

    ret_peril_list = set()
    ret_sub_peril_list = set()
    ret_other_list = set()

    for c in col_names:
        if c in peril_set:
            ret_peril_list.add(c)
        elif c in sub_peril_set:
            ret_sub_peril_list.add(c)
        else:
            ret_other_list.add(c)

    return (ret_peril_list, ret_sub_peril_list, ret_other_list)


def consolidate_col_items(perils, sub_perils, others, peril_table):
    ret = set()
    if others:
        return set(peril_table['sub_peril_code'].unique())
    for p in perils:
        sps_for_this_peril = set(peril_table[peril_table.peril_code == p]['sub_peril_code'].unique())
        common = sub_perils.intersection(sps_for_this_peril)
        if common:
            ret.update(sps_for_this_peril)
        else:
            ret.add(p)
    ret.update(sub_perils)


def write_parquet_file(col_codes, peril_table, out_file):
    perils, sub_perils, _ = separate_codes(col_codes, peril_table)

    peril_list = peril_table.groupby('peril_code').agg(peril_name=('peril_name', 'min')).reset_index()
    sub_peril_list = peril_table.groupby('sub_peril_code').agg(sub_peril_name=('sub_peril_name', 'min')).reset_index()

    df_perils = peril_table[peril_table.peril_code.isin(perils)]
    df_sub_perils = peril_table[peril_table.sub_peril_code.isin(sub_perils)]
    df = pd.concat([df_perils, df_sub_perils])
    df = df[['peril_code', 'peril_name', 'sub_peril_code', 'sub_peril_name']]
    df = df.rename(
        columns={
            'peril_code': 'Peril Code',
            'sub_peril_code': 'Sub Peril Code',
            'peril_name': 'Peril Name',
            'sub_peril_name': 'Sub Peril Name'
        }
    )
    table = pa.Table.from_pandas(df, preserve_index=False)
    indices = pc.sort_indices(table, sort_keys=[('Peril Code', 'ascending')])
    table = pc.take(table, indices)
    pq.write_table(table, out_file)


def write_only_perils(peril_table, out_file):
    df = peril_table.groupby('peril_code').agg(peril_name=('peril_name', 'min')).reset_index()
    df.rename(columns={'peril_name': 'Peril Name', 'peril_code': 'Peril Code'}, inplace=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    indices = pc.sort_indices(table, sort_keys=[('Peril Code', 'ascending')])
    table = pc.take(table, indices)
    pq.write_table(table, out_file)


if __name__ == '__main__':
    print('\n')
    akey = os.environ['S3_ACCESS_KEY']
    skey = os.environ['S3_SECRET_KEY']
    s3_session = er_aws.AwsSession(akey, skey)

    peril_table = get_peril_table()
    click.secho('Reading layer cause of loss ... ', nl=False, bold=True)
    col_names_from_layer_table = read_layer_cause_of_loss(exp_bin_dir, exp_identifier)
    click.secho('Done')

    click.secho('Reading coverage cause of loss ... ', nl=False, bold=True)
    col_df = read_col_id_map(exp_bin_dir)
    col_names = get_coverage_col_ids(exp_bin_dir, col_df)
    cov_table = get_table_from_coverages(exp_bin_dir)
    col_names_from_cov_table = get_cause_of_loss_names_from_coverages(cov_table, col_names)
    click.secho('Done')

    click.secho('Reading valuation cause of loss ... ', nl=False, bold=True)
    col_names_from_valuation_table = get_cause_of_loss_names_from_valuation(asset_bucket, exp_identifier, s3_session)
    click.secho('Done')

    cols_found = col_names_from_cov_table
    if col_names_from_layer_table is not None:
        cols_found.update(col_names_from_layer_table)

    if col_names_from_valuation_table is not None:
        cols_found.update(col_names_from_valuation_table)

    perils, subperils, others = separate_codes(cols_found, peril_table)
    out_file = 'cause_of_loss.parquet'

    if not perils and not subperils and others:
        write_only_perils(peril_table, out_file)
    else:
        out = consolidate_col_items(perils, subperils, others, peril_table)
        out = sorted(out)
        write_parquet_file(out, peril_table, out_file)

    key = f'{aws_prefix}/{os.path.basename(out_file)}'
    click.secho(f'Uploading file to s3 (s3://{asset_bucket}/{key}) ... ', nl=False, bold=True, fg='yellow')
    s3_session.upload_file(asset_bucket, key, file=out_file)
    click.secho('Done', bold=True, fg='green')
    os.remove(out_file)

    print('\n')
