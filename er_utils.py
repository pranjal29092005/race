import os, sys
import pytz
import pandas as pd
import click
from tabulate import tabulate
import libarchive.read as lar
import time

lm_utc = 'Last Modified (UTC)'
lm_ist = 'Last Modified (IST)'


def tabulate_df(df, **kwargs):
    options = {'headers': 'keys', 'tablefmt': 'fancy_grid', 'showindex': False, 'floatfmt': ',.2f', **kwargs}
    print(tabulate(df, **options))


def ask_choice(df, *items):
    tabulate_df(df, *items)
    valid_choices = click.IntRange(0, len(df))
    choice = click.prompt(f'Enter Choice (1 - {len(df)}, 0 to exit)', type=valid_choices)
    return choice


def pick_choices(min_val, max_val):
    valid_choices = click.IntRange(min_val, max_val)
    choice = click.prompt(f'Enter Choice ({min_val} - {max_val}), 0 to exit)', type=valid_choices)
    return choice


def extract_archive(archive_file, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    with lar.file_reader(archive_file) as reader:
        for entry in reader:
            if entry.isdir:
                os.makedirs(os.path.join(out_dir, entry.name), exist_ok=True)
                continue
            out_file = os.path.join(out_dir, entry.name)
            with open(out_file, 'wb') as f:
                for block in entry.get_blocks():
                    f.write(block)


def get_time_from_string(s):
    fmt_a = '%Y-%b-%d:%H%M%S'
    fmt_b = '%Y-%b-%d:%H%M'
    fmt_c = '%Y-%b-%d'

    ret = None

    if not s:
        return None

    try:
        return time.strptime(s, fmt_a)
    except ValueError:
        pass

    try:
        return time.strptime(s, fmt_b)
    except ValueError:
        pass

    try:
        return time.strptime(s, fmt_c)
    except ValueError:
        pass

    return None


def to_pd_timestamp(t, tz=pytz.utc):
    if not t:
        return None

    return pd.Timestamp(year=t[0], month=t[1], day=t[2], hour=t[3], minute=t[4], second=t[5], tzinfo=tz)


def to_pd_timestamp_from_string(s, tz=pytz.utc):
    t = get_time_from_string(s)
    return to_pd_timestamp(t, tz)
