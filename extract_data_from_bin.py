import os, sys, argparse, json
import msgpack
import glob
import pandas as pd
import jmespath as jp
import struct
from datetime import datetime
import numpy as np

parser = argparse.ArgumentParser(prog='Extract data from bin files')
parser.add_argument('--in-dir', dest='in_dir', type=str, required=False)
parser.add_argument('--out-file', dest='out_file', type=str, required=True)
argv = parser.parse_args()

cur_dir = os.path.dirname(os.path.abspath(__file__))
bin_meta_json = os.path.join(cur_dir, 'bin_metadata.json')
md = {}
with open(bin_meta_json, 'r') as f:
    md.update(json.load(f))

term_type = pd.DataFrame({'term_type_str': md['term_type'], 'term_type': range(0, len(md['term_type']))})

in_dir = os.getcwd()
if argv.in_dir:
    in_dir = argv.in_dir


def change_files_case():
    for f in glob.glob('*'):
        nf = f.lower()
        os.rename(f, nf)


os.chdir(in_dir)

change_files_case()

prog_names_file = glob.glob('program_names.bin')

portfolio = True if prog_names_file else False

cur_dir = os.path.dirname(os.path.abspath(__file__))


def to_date(val):
    year = val >> 16
    rem = val - (year << 16)
    month = rem >> 8
    day = rem - (month << 8)
    return datetime(year, month, day)


def read_enums_from_header(fp):
    standard_header_size = 32
    header = fp.read(standard_header_size)
    header_size, num_elements, each_elem_size, version = struct.unpack('4q', header)
    remaining_header = max(0, header_size - standard_header_size)
    if remaining_header > 0:
        num_enum_items = remaining_header // each_elem_size
        enum_items = struct.unpack(f'{each_elem_size}p' * num_enum_items, fp.read(remaining_header))
        enum_items = list(map(lambda s: s.decode().rstrip('\x00'), enum_items))
        return enum_items
    return None


def read_data(fn, read_header):
    data = None
    enum_items = None
    with open(fn, 'rb') as f:
        if read_header:
            enum_items = read_enums_from_header(f)
        try:
            data = msgpack.unpack(f)
            if enum_items:
                data = list(map(lambda i: enum_items[i], data))
        except msgpack.ExtraData:
            return None
    return data


def read_file(fn):
    data = None
    if not (os.path.exists(fn)):
        print(f'file {fn} does not exist')

    data = read_data(fn, False)
    if data is None:
        data = read_data(fn, True)

    return data


def get_metadata_for_file(fn):
    rv = {k: None for k in 'col key value'.split()}
    for k, v in md.items():
        if k in fn:
            if not v:
                rv['col'] = k
                return rv
            for kk in rv.keys():
                vv = v.get(kk)
                if vv is not None:
                    rv[kk] = vv
    return rv


def read_data_from_file(fn):
    data = read_file(fn)
    if not data:
        return

    this_md = get_metadata_for_file(fn)
    col = this_md['col']

    if (isinstance(data, dict)):
        col_a = this_md['key']
        col_b = this_md['value']
        df = {col_a: [], col_b: []}
        for k, v in data.items():
            df[col_a].append(k)
            df[col_b].append(v)
        globals()[col] = pd.DataFrame(df)
    elif (isinstance(data, list)):
        globals()[col] = pd.DataFrame({this_md['col']: data})


def combine_cause_of_loss(cause_of_loss_separated, cov):
    out = {'col_ids': [], 'col_names': []}
    idx = 0
    cov['end'] = cov['col_count'].cumsum()
    cov['start'] = cov['end'].shift().fillna(0).astype(np.int64)

    names_array = np.array(cause_of_loss_separated['col_name'].tolist())
    ids_array = np.array(cause_of_loss_separated['col_id'].astype('str').tolist())

    def _fun(x):
        s = x['start']
        e = x['end']
        return '+'.join(names_array[s:e]), '+'.join(ids_array[s:e])

    cov[['col_names', 'col_ids']] = cov.apply(_fun, axis=1, result_type='expand')
    return cov.drop(columns=['start', 'end'])


def write_coverage_details(col_data, prog_data):
    cov = pd.DataFrame(
        {
            'col_count': read_file('data_f_coverage_cause_of_loss_count.bin'),
            'coverage_order': read_file('data_f_coverage_coverage_order.bin'),
            'program_id': read_file('data_f_coverage_program_name.bin'),
            'deductible': read_file('data_f_coverage_deductible_value.bin'),
            'min_deductible': read_file('data_f_coverage_min_deductible_value.bin'),
            'max_deductible': read_file('data_f_coverage_max_deductible_value.bin'),
            'condition_name': read_file('data_f_coverage_condition_name.bin'),
            'term_type': read_file('data_f_coverage_term_type.bin')
        }
    ).astype({'term_type': np.int64})
    cov = pd.merge(cov, term_type)

    col_contract = read_file('data_f_coverage_cause_of_loss.bin')
    cause_of_loss_separated = pd.merge(
        pd.DataFrame({
            'col_id': col_contract,
            'id': range(len(col_contract))
        }), col_data
    ).sort_values(by='id').drop(columns='id')

    cov = combine_cause_of_loss(cause_of_loss_separated, cov).sort_values(by=['program_id', 'coverage_order'])
    return pd.merge(cov, prog_data)


def read_value_columns():
    files = glob.glob('f_value*')
    files.append('f_tiv.bin')
    for fn in files:
        read_data_from_file(fn)

    value_column_names = list(filter(lambda s: s.startswith('value_'), md.keys()))
    value_column_names.append('tiv')
    return pd.concat([globals()[k] for k in value_column_names], axis='columns')


for pat in [
    'm_latitude', 'm_longitude', 'm_asset_name', 'm_asset_number', 'm_expiration_date', 'm_inception_date',
    'm_policy_type', '*contractname_to_contractrownum_map', 'm_assetlevelcontractrownum', 'coltoidmap',
    'f_valuation_date', "programnametoidmap"
]:
    fn = glob.glob(f'{pat}.bin')
    data = None
    if not fn:
        continue
    if (isinstance(fn, list)):
        fn = fn[0]

    read_data_from_file(fn)

value_columns = read_value_columns()

df = pd.concat(
    [asset_number, asset_name, latitude, longitude, inception_date, expiration_date, asset_contract_map, policy_type],
    axis='columns'
)

print('Asset details ...')
df = pd.merge(df, contractname_rownum, left_on='asset_contract_map', right_on='contract_rownum')
df = pd.concat([df, value_columns, valuation_date], axis='columns')
combined = pd.merge(df, program_name_to_id, left_on='contract_name', right_on='program_name')
if combined.empty:
    combined = pd.merge(df, program_name_to_id, left_on='contract_rownum', right_on='program_id')

if combined.empty:
    print("Empty dataframe resulted")
    sys.exit(-1)
else:
    df = combined

if portfolio:
    print('Coverages ...')
    cov = write_coverage_details(cause_of_loss, program_name_to_id)
    prefix, extn = os.path.splitext(argv.out_file)
    cov.to_csv(f'{prefix}_coverages{extn}', index=False)

for dt in ['inception_date', 'expiration_date', 'valuation_date']:
    df[dt] = df[dt].apply(to_date)

df.to_csv(argv.out_file, index=False)
