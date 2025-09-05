import pandas as pd
import argparse, os, sys
import er_etf
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse

parser = argparse.ArgumentParser(prog='cause-of-loss stuff')
parser.add_argument('--etf', dest='etf', required=True)
parser.add_argument('--date', dest='date')

argv = parser.parse_args()

etf = er_etf.read_etf(argv.etf)

if 'contract' not in etf.keys():
    print("not a contract portfolio")
    sys.exit(-1)

contract = etf['contract']

contract_cols = ['contract_number', 'inception_date', 'expiration_date']

if argv.date:
    analysis_date = date_parse(argv.date)
    mask = ((contract['inception_date'] <= analysis_date) &
            (contract['expiration_date'] >= analysis_date))
    contract = contract.loc[mask]

if 'coverage' not in etf.keys():
    print('no coverages file')
    sys.exit(-1)

combined_cols = ['program_name', 'cause_of_loss']

con_cvg = pd.merge(contract[contract_cols],
                   etf['coverage'],
                   left_on='contract_number',
                   right_on='program_name',
                   how='inner')

con_cvg = con_cvg.assign(col_split=con_cvg.cause_of_loss.str.split(
    '\\+')).explode('col_split').reset_index()
con_cvg[combined_cols + ['col_split']].sort_values(
    by=['program_name', 'col_split']).drop_duplicates(
        subset=['program_name', 'col_split'], keep='first').to_csv('abc.csv',
                                                                   index=False)
