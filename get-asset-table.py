#!/usr/bin/env python
import os, sys, json
import argparse
import er_db
import click
import importlib
from icecream import ic
import binfile
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

exp_module = importlib.import_module('get-exp')


def get_exposure(conn, argv):
    if argv.audit_id:
        audit_id = argv.audit_id
        exp_id = er_db.get_exposure_id(conn, argv.audit_id, argv.portfolio)
    elif argv.id:
        audit_id, _ = er_db.get_audit_id_v2(conn, argv.id, argv.portfolio)
        exp_id = argv.id

    if not audit_id or not exp_id:
        click.secho("No audit or exposure id specified", bold=True, fg="red")
        sys.exit(-1)
    exp_name = er_db.get_exp_name(conn, exp_id, argv.portfolio)
    return exp_module.Exposure(
        audit_id=audit_id, exp_id=exp_id, exp_name=exp_name, db_conn=conn, portfolio=argv.portfolio, env=argv.env
    )


def prepare_asset_table(bin_dir, portfolio):
    table = None
    file_info = {
        'Latitude': ('m_latitude.bin', np.float32),
        'Longitude': ('m_longitude.bin', np.float32),
        'Asset Name': ('m_asset_name.bin', None),
        'Asset Number': ('m_asset_number.bin', np.uint64),
        'Asset Schedule Id': ('m_asset_schedule_id.bin', np.uint32)
    }
    con_table = None
    if portfolio:
        asset_level_parquet = os.path.join(bin_dir, 'asset_level_contract_data.parquet')
        if os.path.exists(asset_level_parquet):
            con_table = pq.read_table(asset_level_parquet)
            con_table = con_table.select(['ContractRows', 'Contract Number'])
        else:
            file_info['ContractRows'] = ('m_assetlevelcontractrownum.bin', np.uint32)
    table = binfile.read_arrays(bin_dir, file_info)
    file_info = {
        'State': 'm_state_code.bin',
        'Country': 'm_country_code.bin',
        'Zipcode': 'm_zipcode.bin',
        'County': 'm_county_code.bin',
        'City': 'm_city.bin',
        'Cresta': 'm_cresta.bin',
        'Occupancy': 'm_occupancy_code.bin',
        'Geocoded Resolution': 'm_geocoded_resolution_code.bin'
    }
    enum_table = binfile.read_enums(bin_dir, file_info)
    for c in enum_table.column_names:
        table = table.append_column(c, enum_table.column(c))
    if con_table is not None:
        for c in con_table.column_names:
            table = table.append_column(c, con_table.column(c))
    if table is not None:
        rows = pa.array(range(0, table.num_rows))
        table = table.append_column('Asset Row', rows)
        columns = table.column_names
        table = table.select(sorted(columns))
        fn = os.path.join(bin_dir, 'asset_table.parquet')
        pq.write_table(table, fn)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='get-asset-table')
    parser.add_argument('-p', dest='portfolio', action='store_true')
    parser.add_argument('--env', '-e', dest='env', choices=['alpha', 'prod', 'integration'], required=True)
    parser.add_argument('--id', '-i', dest='id', type=int)
    parser.add_argument('--audit', '-a', dest='audit_id', type=int)
    argv = parser.parse_args()

    with er_db.get_db_conn(argv.env) as conn:
        exp = get_exposure(conn, argv)
        bin_dir = exp.get_bin_dir()
        if not os.path.exists(os.path.join(bin_dir, 'done.txt')):
            exp.get_exp_binaries()
        prepare_asset_table(bin_dir, argv.portfolio)
