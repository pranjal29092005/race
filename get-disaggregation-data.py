import os, sys, msgpack
import argparse
import click, er_db
import subprocess, shutil, struct
from icecream import ic
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

parser = argparse.ArgumentParser(prog='get-disaggregation-data')
parser.add_argument('--env', dest='env', choices=['prod', 'integration', 'alpha'], required=True)
parser.add_argument('--id', dest='id', required=False, type=int)
parser.add_argument('--audit-id', dest='aid', required=False, type=int)
parser.add_argument('--portfolio', '-p', action='store_true', dest='portfolio')

this_dir = os.path.dirname(os.path.abspath(__file__))
ic(this_dir)

argv = parser.parse_args()
if not argv.id and not argv.aid:
    click.secho('No audit or schedule id specified', bg='red', fg='white')
    sys.exit(-1)

audit_id = argv.aid
if argv.id and not argv.aid:
    with er_db.get_db_conn(argv.env) as conn:
        audit_id, _ = er_db.get_audit_id_v2(conn, argv.id, argv.portfolio)

if audit_id is None:
    click.secho('No audit id found', fg='white', bg='red')
    sys.exit(-1)

cmd = [
    shutil.which('python'),
    os.path.join(this_dir, 'get-exp-binaries.py'), '--env', argv.env, '-a',
    str(audit_id), '--no-parquet'
]
if argv.portfolio:
    cmd.append('-p')


def read_enum_values(f, header):
    header_size, num_rows, elem_size, version = struct.unpack('4q', header)
    if version == 3:
        return msgpack.unpackb(f.read(header_size))
    return []


def get_enum_values(indices, enums):
    ret = []
    for idx in indices:
        try:
            val = enums[idx]
            val = val[1:] if val.startswith(':') else val
            ret.append(val)
        except IndexError:
            ret.append('')
    return ret


def read_msgpack(d, fn, **kwargs):
    header = None
    data = None
    read_header = kwargs.get('header')
    as_enum = kwargs.get('enum')
    enum_values = []
    with open(os.path.join(d, fn), 'rb') as f:
        if read_header or as_enum:
            header = f.read(32)
        if as_enum:
            enum_values = read_enum_values(f, header)
        data = msgpack.unpack(f)
        if as_enum:
            return get_enum_values(data, enum_values)
    return data


subprocess.call(cmd)
dest_dir = os.path.join(os.environ.get('RACE_MASTER_FOLDER'), 'AssetScheduleDataFolder', argv.env, f'{audit_id}_0')
ic(dest_dir)

lats = read_msgpack(dest_dir, 'm_latitude.bin', header=True)
lons = read_msgpack(dest_dir, 'm_longitude.bin', header=True)
state = read_msgpack(dest_dir, 'm_state_code.bin', enum=True)
county = read_msgpack(dest_dir, 'm_county_code.bin', enum=True)
country = read_msgpack(dest_dir, 'm_country_code.bin', enum=True)
zip_code = read_msgpack(dest_dir, 'm_zipcode.bin', enum=True)

df = pd.DataFrame(
    {
        'state': state,
        'county': county,
        'country': country,
        'latitude': lats,
        'longitude': lons,
        'zip_code': zip_code
    }
)

out_file = f'disagg_{audit_id}.parquet'
click.secho(f'writing disaggregation data to {out_file}', bold=True, fg='yellow')
pq.write_table(pa.Table.from_pandas(df), out_file)
