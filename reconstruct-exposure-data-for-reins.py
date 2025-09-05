import os, sys, json, re
import er_db
import argparse
from icecream import ic
import click
import binfile
import numpy as np
from psycopg2.extras import RealDictCursor
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import glob
from colnames import Asset as asset
from colnames import Layer as layer
from colnames import Contract as contract
from colnames import Reinsurance as reins

parser = argparse.ArgumentParser(prog='reconstruct-exposure-data')
parser.add_argument('--env', '-e', dest='env', choices=['prod', 'alpha', 'integration'], required=True)
parser.add_argument('--id', '-i', dest='id', type=int, required=True)
parser.add_argument('--portfolio', '-p', action='store_true')
parser.add_argument('--out-dir', '-o', dest='out_dir', type=str, required=False)


class Reconstruct:
    def __init__(self, asset_dir, audit_id):
        super().__init__()
        self.argv = argv
        self.asset_dir = asset_dir
        self.audit_id = audit_id
        self.dir = os.path.join(self.asset_dir, f'{audit_id}_0')
        self.asset_data = None
        self.contract_data = None
        self.layer_data = None
        self.asset_files = {}
        self.contract_files = {}
        self.layer_files = {}
        self.layer_parquet_file = None
        self.asset_parquet_file = None
        self.contract_parquet_file = None
        self.initialize_filenames()

    def init_layer_filenames(self):
        key = f'data_f_layer_{self.audit_id}_0'
        self.layer_files = {
            layer.layer_inception_date: (f'{key}_{layer.layer_inception_date}.bin', np.uint32),
            layer.layer_expiration_date: (f'{key}_{layer.layer_expiration_date}.bin', np.uint32),
            layer.layer_number: (f'{key}_{layer.layer_number}.bin', None),
            layer.lob: (f'{key}_{layer.lob}.bin', None),
            layer.contract_number: (f'{key}_{layer.contract_number}.bin', None)
        }
        for k, v in self.layer_files.items():
            self.layer_files[k] = (v[0].lower().replace(' ', '_'), v[1])

    def init_contract_filenames(self):
        key = f'data_f_contract_{self.audit_id}_0'
        self.contract_files = {
            'as_id_con_row': f'data_m_asset_schedule_{self.audit_id}_0_assetschid_to_contractrownum_map',
            'as_name_con_row': f'{key}_assetschname_to_contractrownum_map',
            'con_name_row': f'{key}_contractname_to_contractrownum_map'
        }
        for k, v in self.contract_files.items():
            self.contract_files[k] = os.path.join(self.dir, f'{v}.bin')

    def init_asset_filenames(self):
        pass

    def initialize_filenames(self):
        self.init_layer_filenames()
        self.init_contract_filenames()
        self.init_asset_filenames()

    def create_asset_data(self):
        mapping = {
            asset.latitude: ('m_latitude.bin', np.float32),
            asset.longitude: ('m_longitude.bin', np.float32),
            asset.contract_row_num: ('m_assetlevelcontractrownum.bin', np.uint32),
            asset.asset_name: ('m_asset_name.bin', None),
            asset.inception_date: ('m_inception_date.bin', np.uint32),
            asset.expiration_date: ('m_expiration_date.bin', np.uint32),
            asset.asset_schedule_id: ('m_asset_schedule_id.bin', np.uint32),
            asset.asset_number: ('m_asset_number.bin', np.uint64)
        }

        condition_files = glob.glob(os.path.join(self.dir, 'm_condition*bin'))
        condition_reg = re.compile('m_([^.]*)\\.bin')
        for cf in condition_files:
            fn = os.path.basename(cf)
            m = condition_reg.match(fn)
            if m is not None:
                mapping[m[1]] = (fn, None)

        return binfile.read_arrays(self.dir, mapping, True)

    def create_contract_data(self):
        cd = {
            'as_id_con_row':
                binfile.BinFile(self.contract_files['as_id_con_row']).read_dictionary(
                    key=asset.asset_schedule_id,
                    value=contract.contract_row_num,
                    key_type=np.uint32,
                    value_type=np.uint32
                ),
            'as_name_con_row':
                binfile.BinFile(self.contract_files['as_name_con_row']).read_dictionary(
                    key=contract.covered_asset_schedule_name, value=contract.contract_row_num, value_type=np.uint32
                ),
            'con_name_row':
                binfile.BinFile(
                    self.contract_files['con_name_row']
                ).read_dictionary(key=contract.contract_number, value=contract.contract_row_num, value_type=np.uint32)
        }
        table = cd['as_id_con_row'].join(cd['as_name_con_row'], join_type='inner', keys=[contract.contract_row_num])
        table = table.join(cd['con_name_row'], join_type='inner', keys=[contract.contract_row_num])

        key = f'data_f_contract_{self.audit_id}_0'
        con_elems = {
            contract.cedant_name: (f'{key}_cedant_name.bin', None),
            contract.lob: (f'{key}_lob.bin', None),
            contract.producer: (f'{key}_producer.bin', None)
        }
        elems = binfile.read_arrays(self.dir, con_elems, False)

        for c in elems.column_names:
            try:
                table = table.append_column(c, elems.column(c))
            except Exception as e:
                click.secho(f'Not adding {c}. Reason: {e}', fg='red', bold=True)
                pass
        return table

    def create_layer_data(self):
        ld = binfile.read_arrays(self.dir, self.layer_files, False)
        if not ld.num_rows:
            return
        self.layer_data = ld.join(
            self.contract_data.select([layer.contract_number, contract.contract_row_num]),
            keys=[layer.contract_number],
            join_type='inner'
        )

    def remove_table_column(self, table, col_name):
        try:
            idx = table.column_names.index(col_name)
            return table.remove_column(idx)
        except ValueError:
            return table

    def execute(self):
        ad = self.create_asset_data()
        cd = self.create_contract_data()
        self.contract_data = ad.group_by(asset.contract_row_num).aggregate(
            [(asset.inception_date, 'hash_one'), (asset.expiration_date, 'hash_one')]
        ).rename_columns([contract.contract_row_num, contract.inception_date,
                          contract.expiration_date]).join(cd, keys=contract.contract_row_num,
                                                          join_type='inner').sort_by(contract.contract_row_num)
        self.asset_data = self.remove_table_column(ad, asset.expiration_date)
        self.asset_data = self.remove_table_column(self.asset_data, asset.inception_date)
        self.asset_data = self.asset_data.append_column(
            asset.asset_row, pa.array(np.arange(self.asset_data.num_rows, dtype=np.uint32))
        )
        self.create_layer_data()

    def update_input_json(self, dir_name):
        input_json = os.path.join(dir_name, 'input.json')
        if not os.path.exists(input_json):
            return
        inp = {}
        with open(input_json, 'r') as fp:
            inp = json.load(fp)
            cur_data = {
                'Asset Data': self.asset_parquet_file,
                'Layer Data': self.layer_parquet_file,
                'Contract Data': self.contract_parquet_file
            }
            inp['Exposure Data'] = cur_data
        with open(input_json, 'w') as fp:
            json.dump(inp, fp, indent=4)

    def save(self, dir_name):
        os.makedirs(dir_name, exist_ok=True)
        self.asset_parquet_file = os.path.join(dir_name, 'asset_data.parquet')
        self.layer_parquet_file = os.path.join(dir_name, 'layer_data.parquet')
        self.contract_parquet_file = os.path.join(dir_name, 'contract_data.parquet')
        pq.write_table(self.asset_data, self.asset_parquet_file)
        pq.write_table(self.contract_data, self.contract_parquet_file)
        if self.layer_data is not None:
            pq.write_table(self.layer_data, self.layer_parquet_file)
        click.secho(f"Files saved to {dir_name}", fg='cyan', bold=True)
        self.update_input_json(dir_name)


if __name__ == '__main__':
    argv = parser.parse_args()
    asset_dir = os.path.join(os.environ.get('RACE_MASTER_FOLDER'), 'AssetScheduleDataFolder', argv.env.lower())
    audit_id = 0
    with er_db.get_db_conn(argv.env) as conn:
        query = f'select audit_id from race.m_portfolio where portfolio_id={argv.id}'
        if not argv.portfolio:
            query = f'select "AUDIT_ID" as audit_id from race."M_ASSET_SCHEDULE" where "ID"={argv.id}'
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        res = cursor.fetchone()
        audit_id = res['audit_id']
    recon = Reconstruct(asset_dir, audit_id)
    recon.execute()
    if argv.out_dir is not None:
        recon.save(argv.out_dir)
