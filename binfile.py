import msgpack
import pandas as pd
import os, sys, json
import struct
from icecream import ic
import numpy as np
import pyarrow as pa
import click


class BinFile:
    def __init__(self, fn):
        super().__init__()
        self.fn = fn
        self.std_header_size = 32
        self.dir_name = os.path.dirname(fn)
        self.types = {
            'int': pa.uint32(),
            'int64': pa.uint64(),
            'float': pa.float32(),
            'double': pa.float64(),
            'str': pa.string()
        }

    def read_dictionary(self, **kwargs):
        if not os.path.exists(self.fn):
            return pa.table({})

        with open(self.fn, 'rb') as f:
            x = msgpack.unpack(f, strict_map_key=False)
            key = kwargs.get('key')
            value = kwargs.get('value')
            key_type = kwargs.get('key_type')
            value_type = kwargs.get('value_type')
            if key is not None and value is not None:
                keys = []
                values = []
                for k, v in x.items():
                    keys.append(k)
                    values.append(v)
                this_dict = {
                    key: keys if key_type is None else np.array(keys, dtype=key_type),
                    value: values if value_type is None else np.array(values, dtype=value_type)
                }
                return pa.table(this_dict)
        return pa.table({})

    def read_enum_values_v2(self, fp, header_size, each_elem_size):
        remaining_header = max(0, header_size - self.std_header_size)
        if remaining_header > 0:
            num_enum_values = remaining_header // each_elem_size
            enum_values = struct.unpack(f'{each_elem_size}p' * num_enum_values, fp.read(remaining_header))
            enum_values = list(map(lambda s: s.decode().rstrip('\x00'), enum_values))
            return enum_values
        return []

    def read_enum_values_v3(self, fp, header_size):
        enum_values = None
        if header_size > 0:
            enum_values = msgpack.unpackb(fp.read(header_size))
            enum_values = list(map(lambda s: s[1:], enum_values))
        return enum_values

    def read_enum(self):
        data = []
        if not os.path.exists(self.fn):
            return data
        with open(self.fn, 'rb') as f:
            header = f.read(self.std_header_size)
            header_size, num_elements, each_elem_size, version = struct.unpack('4q', header)
            enum_values = []
            if version == 2:
                enum_values = self.read_enum_values_v2(f, header_size, each_elem_size)
            else:
                enum_values = self.read_enum_values_v3(f, header_size)
            data = msgpack.unpack(f)
            ret = []
            if enum_values is None:
                return pa.array(data)
            for idx in data:
                if idx >= len(enum_values):
                    ret.append('')
                else:
                    ret.append(enum_values[idx])
        return pa.array(ret)

    def read_array(self, **kwargs):
        data = []
        out = None
        read_header = kwargs.get('read_header', False)
        conversion_type = kwargs.get('conversion_type')
        # ic(f'File: {self.fn}, Header: {read_header}, Conversion: {conversion_type}')
        if not os.path.exists(self.fn):
            return data
        with open(self.fn, 'rb') as f:
            if read_header:
                f.read(self.std_header_size)
            data = msgpack.unpack(f)
            if conversion_type is not None:
                out = pa.array(np.array(data, dtype=conversion_type))
            else:
                out = pa.array(np.array(data))
        return out


def read_array(fn, read_header=True):
    bf = BinFile(fn)
    bf.read_array(read_header)


def read_enum(fn):
    bf = BinFile(fn)
    bf.read_enum()


def read_dictionary(fn):
    bf = BinFile(fn)
    bf.read_dictionary()


def read_arrays(dir_name, keys_and_files, read_header=True):
    data = {}
    for k, v in keys_and_files.items():
        fn = v[0]
        if not os.path.exists(os.path.join(dir_name, fn)):
            continue
        if dir_name is None:
            data[k] = BinFile(fn).read_array(read_header=read_header, conversion_type=v[1])
        else:
            data[k] = BinFile(os.path.join(dir_name, fn)).read_array(read_header=read_header, conversion_type=v[1])
    return pa.table(data)


def read_enums(dir_name, keys_and_files):
    if not keys_and_files:
        return None
    data = {}

    for k, v in keys_and_files.items():
        data[k] = BinFile(os.path.join(dir_name, v)).read_enum()
    return pa.table(data)
