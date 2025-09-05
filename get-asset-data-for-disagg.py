import os, sys, json
import binfile, er_db
import argparse
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

parser = argparse.ArgumentParser(prog='get-asset-data-for-disagg')
parser.add_argument('--env', dest='env', type=str, choices=['prod', 'integration', 'alpha'], required=True)
parser.add_argument('--id', dest='id', type=int, required=True)
parser.add_argument('-p', action='store_true', dest='portfolio')

argv = parser.parse_args()
audit_id = None

conn = er_db.get_db_conn(argv.env)
with conn:
    audit_id, portfolio = er_db.get_audit_id_v2(conn, argv.id, argv.portfolio)
    pass

exp_dir = os.path.join(os.environ['RACE_MASTER_FOLDER'], 'AssetScheduleDataFolder', argv.env.lower())
if argv.portfolio:
    exp_dir = os.path.join(exp_dir, f'{audit_id}_0')
else:
    exp_dir = os.path.join(exp_dir, f'{audit_id}_{argv.id}')

files = {
    'asset_number': ('m_asset_number.bin', np.uint64),
    'asset_name': ('m_asset_name.bin', None),
    'latitude': ('m_latitude.bin', np.float32),
    'zipcode': ('m_zipcode.bin', None),
    'city': ('m_city.bin', None),
    'address_1': ('m_address_line_1.bin', None),
    'address_2': ('m_address_line_2.bin', None),
    'longitude': ('m_longitude.bin', np.float32)
}

table = binfile.read_arrays(exp_dir, files)

files = {
    'country': 'm_country_code.bin',
    'state': 'm_state_code.bin',
    'county': 'm_county_code.bin',
}
enum_table = binfile.read_enums(exp_dir, files)

for k, _ in files.items():
    table = table.append_column(k, enum_table.column(k))

pq.write_table(table, 'asset_data.parquet')

conn.close()
