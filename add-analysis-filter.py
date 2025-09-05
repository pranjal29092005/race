import os, sys, json, pathlib
import jmespath
import argparse
import click
from icecream import ic
import copy


class AnalysisFilter():
    def __init__(self, **kwargs):
        self.dir = kwargs.get('dir')
        self.attribute = kwargs.get('attribute')
        self.value = kwargs.get('value')
        self.analysis_files = self.dir.glob('*analysis*json')

    def get_exp_filter_path(self, data, *path):
        ret = None
        for k, v in data.items():
            if (isinstance(v, dict)):
                items = list(copy.deepcopy(path))
                items.append(k)
                ret = self.get_exp_filter_path(v, *items)
                if ret:
                    return ret
            elif (isinstance(v, list)):
                if k == 'FilterList':
                    items = list(copy.deepcopy(path))
                    items.append(k)
                    return items
        return None

    def add_filter(self, data):
        srch = jmespath.search('*.ExposureFilterSets.FilterList', data)
        if not srch:
            return data
        path = self.get_exp_filter_path(data)
        if not path:
            return data

        filter_list = data
        for p in path:
            filter_list = filter_list[p]

        filter_obj = {
            'AndOr':
                'AND',
            'FilterList':
                [
                    {
                        'AndOr': 'OR',
                        'AssetType': 'Site',
                        'Attribute': self.attribute,
                        'Operator': 'EQ',
                        'Value': self.value
                    }
                ]
        }
        filter_list.append(filter_obj)
        return data

    def execute(self):
        for fn in self.analysis_files:
            data = None
            with fn.open('r') as f:
                data = json.load(f)
            if data is not None:
                data = self.add_filter(data)
            with fn.open('w') as f:
                json.dump(data, f, indent=4)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='add-analysis-filter')
    parser.add_argument('--dir', dest='dir', required=True, type=str)
    parser.add_argument('--attribute', '-a', dest='attr', required=False, type=str)
    parser.add_argument('--value', '-v', dest='value', required=False, type=str)
    parser.add_argument('--string', '-s', dest='string', required=False, type=str)

    argv = parser.parse_args()
    if argv.string is None and (argv.attr is None or argv.value is None):
        click.secho('No valid filter provided', fg='red')
        sys.exit(-1)
    dir_name = pathlib.Path(argv.dir)
    if not dir_name.exists():
        click.secho('Test directory does not exist', fg='red')
        sys.exit(-1)
    if argv.string:
        (attribute, value) = argv.string.split('=')
    else:
        attribute = argv.attr
        value = argv.value
    attribute = attribute.strip()
    value = value.strip()
    engine = AnalysisFilter(dir=dir_name, attribute=attribute, value=value)
    engine.execute()
