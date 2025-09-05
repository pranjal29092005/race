import pandas as pd
import zipfile
import os, sys
from dateutil.parser import parse as date_parse


def read_etf(etf_file):
    with zipfile.ZipFile(etf_file) as zf:
        out = {}
        for f in zf.namelist():
            prefix, extn = os.path.splitext(f)
            df = pd.read_csv(zf.open(f), low_memory=False)
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            if 'inception_date' in df.columns:
                df['inception_date'] = df['inception_date'].apply(date_parse)
            if 'expiration_date' in df.columns:
                df['expiration_date'] = df['expiration_date'].apply(date_parse)
            out[prefix] = df
    return (out)
