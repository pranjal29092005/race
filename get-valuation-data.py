import os, sys, msgpack, json, argparse
import struct
from icecream import ic
import numpy as np
import pandas as pd

parser = argparse.ArgumentParser(prog='get-valuation-data')
parser.add_argument('--dir', dest='dir', required=True, type=str)


class ReadValuationData:
    def __init__(self, dir, **kwargs):
        self.dir = dir

    def read_v3_data(self, f, header_size, num_elements):
        if header_size == 0:
            return msgpack.unpack(f)
        else:
            dd = msgpack.unpackb(f.read(header_size))
            dd = list(map(lambda s: s[1:], dd))
            data = msgpack.unpack(f)
            return list(map(lambda i: dd[i] if i != np.iinfo('uint32').max else '', data))

    def read_valuation(self, fn):
        with open(fn, 'rb') as f:
            header = f.read(32)
            header_size, num_elements, each_elem_size, version = struct.unpack('4q', header)
            if version == 3:
                return self.read_v3_data(f, header_size, num_elements)

    def execute(self):
        tiv = self.read_valuation(os.path.join(self.dir, 'f_tiv.bin'))
        val_date = self.read_valuation(os.path.join(self.dir, 'f_valuation_date.bin'))
        val_cur_code = self.read_valuation(os.path.join(self.dir, 'f_valuation_currency_code.bin'))
        as_id = self.read_valuation(os.path.join(self.dir, 'f_asset_schedule_id.bin'))
        as_num = self.read_valuation(os.path.join(self.dir, 'f_asset_number.bin'))
        df = pd.DataFrame(
            {
                'asset_schedule_id': as_id,
                'asset_number': as_num,
                'val_date': val_date,
                'tiv': tiv,
                'cur_code': val_cur_code
            }
        )
        return df


if __name__ == '__main__':
    argv = parser.parse_args()
    rvd = ReadValuationData(argv.dir)
    df = rvd.execute()
