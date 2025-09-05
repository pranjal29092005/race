import os, sys, argparse
import msgpack
import pandas as pd

parser = argparse.ArgumentParser(prog='Binary to csv converter')
parser.add_argument('--out', dest='out', type=str, required=True)
parser.add_argument('--in', dest='inputs', required=True, nargs='+', type=str)
argv = parser.parse_args()


def read_data(fn):
    name = os.path.splitext(os.path.basename(fn))[0]
    data = []
    if os.path.exists(fn):
        try:
            with open(fn, 'rb') as f:
                data = msgpack.unpack(f)
        except msgpack.ExtraData:
            print('coming here')
            with open(fn, 'rb') as f:
                f.read(32)  #read header
                data = msgpack.unpack(f)
        except Exception as e:
            print(f"Exception: {e}")
            sys.exit(-1)
    if (isinstance(data, dict)):
        rv = {'keys': [], 'values': []}
        for k, v in data.items():
            rv['keys'].append(k)
            rv['values'].append(v)
        return rv
    else:
        return {name: data}


data_map = {}
for data in map(read_data, argv.inputs):
    data_map.update(data)

pd.DataFrame.from_dict(data_map,
                       orient='index').transpose().to_csv(argv.out,
                                                          index=False)
