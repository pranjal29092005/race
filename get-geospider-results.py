import os, sys, json
import pyarrow as pa
from icecream import ic
import pyarrow.parquet as pq
import pyarrow.compute as pc
from psycopg2.extras import RealDictCursor
import er_aws, er_db
import argparse
import pandas as pd
import itertools
import er_utils

parser = argparse.ArgumentParser(prog='get-geospider-results')
parser.add_argument('--results-key', '-r', dest='rkey', required=True)
parser.add_argument('--eg-id', '-g', dest='eg_id', required=False)
parser.add_argument('--exp-id', '-x', dest='exp_id', required=False)
parser.add_argument('--env', '-e', dest='env', required=True)
parser.add_argument('--out', '-o', dest='out', required=False)

argv = parser.parse_args()
bucket = 'er-race-results'
aws_session = er_aws.AwsSession(os.environ['S3_ACCESS_KEY'], os.environ['S3_SECRET_KEY'])

audit_ids = []
name_df = None

with er_db.get_db_conn(argv.env) as conn:
    if argv.eg_id:
        audit_ids = er_db.get_audit_ids_for_exp_group(conn, argv.eg_id)
    elif argv.exp_id:
        audit_ids = [er_db.get_audit_id_v2(conn, argv.exp_id, True)[0]]
    if len(audit_ids) > 0:
        aid_str = ','.join(map(lambda i: str(i), audit_ids))
        query = f'select portfolio_id, audit_id, portfolio_name from race.m_portfolio where audit_id in ({aid_str})'
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        res = cursor.fetchall()
        df = pd.DataFrame(res)
        name_df = df.rename(
            columns={
                'portfolio_id': 'Portfolio Id',
                'audit_id': 'Audit Id',
                'portfolio_name': 'Portfolio Name'
            }
        )


def read_selection_parquet(key):
    ff = f's3://{bucket}/{key}'
    try:
        df = pq.read_table(ff)
        if df.num_rows > 0:
            return df
        print(f'no results for {key}')
        return None
    except FileNotFoundError:
        print(f'{key} not found')
        return None


tables = []
prefixes = []
for a in audit_ids:
    selkey = f'geo-spider/{argv.rkey}/{a}/selection.parquet'
    prefixes.append(os.path.dirname(selkey))
    df = read_selection_parquet(selkey)
    if df is not None:
        path = pa.array(itertools.repeat(f's3://{bucket}/geo-spider/{argv.rkey}/{a}', df.num_rows))
        df = df.append_column('path', path)
        audit_id = pa.array(itertools.repeat(a, df.num_rows))
        df = df.append_column('audit_id', audit_id)
        tables.append(df)

result_files = []
for p in prefixes:
    df = aws_session.list_files_from_s3(Bucket=bucket, Prefix=p)
    if df is not None:
        result_files.extend(df['Key'].tolist())

result_files = set(map(lambda s: f's3://{bucket}/{s}', result_files))

if len(tables) == 0:
    print('no tables to fetch')
    sys.exit(-1)

table = pa.concat_tables(tables)
combination = table.column('Combination')
combination = pc.binary_join_element_wise(
    pc.replace_substring_regex(pc.ascii_lower(combination), '[ =,]', '_'), '.parquet', ''
)

filename = pc.binary_join_element_wise(table['path'], combination, '/')
table = table.append_column('filename', filename)
table = table.drop(['path'])

completed = pc.is_in(table['filename'], pa.array(result_files))
table = table.append_column('completed', completed)

df = pa.TableGroupBy(table, 'audit_id').aggregate([('completed', 'sum'),
                                                   ('filename', 'count_distinct')]).rename_columns(
                                                       ['Audit Id', 'Completed', 'Expected']
                                                   ).select([2, 1, 0]).to_pandas()
df = name_df.merge(df, on='Audit Id')
er_utils.tabulate_df(df)

if argv.out:
    pq.write_table(table, argv.out)
