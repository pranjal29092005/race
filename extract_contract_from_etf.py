import pandas as pd
import er_etf
import os, sys, argparse
import zipfile, tempfile

parser = argparse.ArgumentParser(prog='extract contract from etf')
parser.add_argument('--etf', dest='etf', required=True, type=str)
parser.add_argument('--contract', dest='contract_id', required=True, type=str)
parser.add_argument('--name', dest='name', type=str)
parser.add_argument('--no-layers', action='store_true')

argv = parser.parse_args()

etf_name = '{}.etf'.format(argv.name.lower().replace(' ', '_'))
etf = er_etf.read_etf(argv.etf)
srch = argv.contract_id

asset_schedule_type = str(etf['contract'].dtypes['covered_asset_schedule_name'])
if 'int' in asset_schedule_type:
    srch = int(srch)

contract = etf['contract'].loc[etf['contract'].covered_asset_schedule_name == srch].copy()
if contract.empty:
    print(f'no asset schedule named {srch}')
    contract = etf['contract'].loc[etf['contract'].contract_number == srch].copy()
    if contract.empty:
        print(f'no contract named {srch}')
        sys.exit(0)

print(contract)

left = contract[['covered_asset_schedule_name']].rename(columns={'covered_asset_schedule_name': 'asset_schedule_name'})

int_cols = [
    'parent_asset_number', 'risk_owner_identifier', 'number_of_buildings', 'square_footage', 'number_of_stories',
    'lowest_floor_occupied', 'highest_floor_occupied', 'number_of_people'
]

asset = pd.merge(left, etf['asset'], how='inner')
asset = asset.astype({k: 'Int64' for k in filter(lambda col: col in asset.columns, int_cols)})

valuation = pd.merge(left, etf['valuation'], how='inner')

if 'schedule_name' in etf['exposureset'].columns:
    exposureset = pd.merge(
        left, etf['exposureset'], left_on='asset_schedule_name', right_on='schedule_name', how='inner'
    ).drop(columns=['schedule_name'])
else:
    exposureset = pd.merge(left, etf['exposureset'], how='inner')

coverage = pd.merge(contract[['program_name']], etf['coverage'], how='inner')

name = argv.name if argv.name else f'single-contract-{srch}'

if argv.no_layers:
    layer = etf['layer'].iloc[0:0, :].copy()
    if 'no_layers' not in etf_name:
        prefix, suffix = os.path.splitext(etf_name)
        etf_name = f'{prefix}_no_layers{suffix}'
        name = f'{name}_no_layers'
else:
    layer = pd.merge(contract[['contract_number']], etf['layer'], how='inner')

portfolio = pd.DataFrame([{'Portfolio_Name': name}])

contract['inception_date'] = contract.inception_date.dt.strftime('%m-%d-%Y')
contract['expiration_date'] = contract.expiration_date.dt.strftime('%m-%d-%Y')

with tempfile.TemporaryDirectory() as tdir, \
  zipfile.ZipFile(etf_name, 'w', zipfile.ZIP_DEFLATED) as zf:
    for df in etf.keys():
        if 'r_info' in df or 'r_scope' in df:
            continue
        fn = os.path.join(tdir, f'{df}.csv')
        globals()[df].to_csv(fn, index=False)
        zf.write(fn, os.path.basename(fn))
