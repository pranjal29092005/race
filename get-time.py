import os, sys
import er_utils
import pandas as pd
import pytz
from datetime import datetime

zones = {
    'IST': pytz.timezone('Asia/Calcutta'),
    'UTC': pytz.timezone('UTC'),
    'US Central': pytz.timezone('US/Central'),
    'US Eastern': pytz.timezone('US/Eastern'),
    'US Pacific': pytz.timezone('US/Pacific')
}


def time_now():
    times = {}
    times['Zone'] = []
    times['Time'] = []
    for k, v in zones.items():
        times['Zone'].append(k)
        times['Time'].append(datetime.now(tz=v).strftime('%Y-%b-%d %H:%M:%S'))
    er_utils.tabulate_df(pd.DataFrame.from_dict(times))


if __name__ == '__main__':
    time_now()
