import pandas as pd
import os, sys, json
import argparse
import er_etf

parser = argparse.ArgumentParser(prog='extract contract from etf')
parser.add_argument('--etf', dest='etf', required=True, type=str)
parser.add_argument('--action', choices=['stats'])

argv = parser.parse_args()

etf = er_etf.read_etf(argv.etf)


def get_stats(etf):
    num_contracts = len(etf['contract'].drop_duplicates(
        'Covered_Asset_Schedule_Name', keep='first').index)
    num_assets = len(etf['asset'].drop_duplicates('ASSET_NUMBER',
                                                  keep='first').index)
    num_layers = len(etf['layer'].drop_duplicates('layer_number',
                                                  keep='first').index)
    num_cvgs = len(etf['coverage'].index)
    cvg_col = etf['coverage']['cause_of_loss'].drop_duplicates()
    layer_col = etf['layer']['cause_of_loss'].drop_duplicates()

    out = []
    out.append({'key': 'Number of contracts', 'value': num_contracts})
    out.append({'key': 'Number of assets', 'value': num_assets})

    out.append({'key': 'Number of coverage terms', 'value': num_cvgs})
    out.append({'key': 'Cvg cause of loss', 'value': cvg_col.tolist()})

    out.append({'key': 'Number of layers', 'value': num_layers})
    out.append({'key': 'Layer cause of loss', 'value': layer_col.tolist()})

    print(pd.DataFrame(out))


if argv.action == 'stats':
    get_stats(etf)
