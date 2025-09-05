import os, sys
import er_aws, er_utils
import argparse
import pytz
from datetime import datetime, timedelta
import pandas as pd
import libarchive.read as lar
from er_choice import Choice

log_bucket = 'er-race-log'

envs = ['prod', 'prod2', 'alerts', 'alerts2', 'uat', 'integration',
        'beta'].sort()
parser = argparse.ArgumentParser(prog='get-race-logs')
parser.add_argument('--env', dest='env', choices=envs, default='uat')
parser.add_argument('--full-logs', dest='full_logs', action='store_true')

subparsers = parser.add_subparsers(help='subparsers')

range_parser = subparsers.add_parser('range')
range_parser.add_argument('-from',
                          dest='from_date',
                          help='From date in yyyy-mmm-dd:hhmmss format',
                          required=True)
range_parser.add_argument('-to',
                          dest='to_date',
                          help='To date in yyyy-mmm-dd:hhmmss format',
                          required=False)
range_parser.add_argument('-ist',
                          action='store_true',
                          help='Use IST time zone instead of UTC. Default UTC',
                          required=False)

offset_parser = subparsers.add_parser('offset')
offset_parser.add_argument('-days',
                           dest='offset_days',
                           type=int,
                           help='Get race logs for days',
                           required=True)

env_prefix_map = {
    'prod': 'PROD_',
    'prod2': 'PROD2_',
    'alerts': 'ALERT_',
    'alerts2': 'ALERT2_',
    'uat': 'SCALE_',
    'integration': 'INTEGRATION_',
    'beta': 'BETA_'
}

args = parser.parse_args()

days_delta = getattr(args, 'offset_days', 1)

ist = getattr(args, 'ist', False)
tz = pytz.timezone('Asia/Calcutta') if ist else pytz.utc

default_end = pd.Timestamp.utcnow()
default_start = default_end - pd.Timedelta(days_delta, unit='D')

end_time = er_utils.to_pd_timestamp_from_string(getattr(args, 'to_date', None),
                                                tz)
start_time = er_utils.to_pd_timestamp_from_string(
    getattr(args, 'from_date', None), tz)

if not end_time:
    end_time = default_end

if not start_time:
    start_time = default_start

fmt = '%Y-%b-%d:%H:%M:%S'
print('Listing logs from {} to {}'.format(
    start_time.astimezone(tz).strftime(fmt),
    end_time.astimezone(tz).strftime(fmt)))

log_files = er_aws.list_files_from_s3(Bucket=log_bucket,
                                      Prefix=env_prefix_map[args.env]).copy()

mask = (log_files['LastModified'] > start_time) & (log_files['LastModified'] <
                                                   end_time)

filtered = log_files.loc[mask].copy().reset_index().drop('index', axis=1)
filtered['Item'] = filtered.index + 1
filtered = filtered.rename(columns={
    'Key': 'File',
})[['Item', 'File', er_utils.lm_utc, er_utils.lm_ist]]

choice = 1
choices = []

if len(filtered) == 1:
    choices.append(1)
else:
    er_utils.tabulate_df(filtered)
    skg = Choice(max=len(filtered))

    while choice:
        choice = skg.get_choice()
        if choice:
            if (isinstance(choice, list)):
                choices.extend(choice)
            else:
                choices.append(choice)


def extract_full(archive_file, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    with lar.file_reader(archive_file) as reader:
        for entry in reader:
            with open(os.path.join(out_dir, entry.name), 'wb') as out:
                for block in entry.get_blocks():
                    out.write(block)
    print(f'Extracted logs to {out_dir}')


def extract_only_race_log(archive_file, out_file):
    with lar.file_reader(archive_file) as reader:
        for entry in reader:
            if entry.name != 'race.log':
                continue
            with open(out_file, 'wb') as out:
                for block in entry.get_blocks():
                    out.write(block)
    print(f'Extracted log file to {out_file}')


def extract_race_log(archive_file):
    pfx = os.path.splitext(os.path.basename(archive_file))[0]
    out_dir = os.environ.get('RACE_LOGS_DIR', os.getcwd())
    if args.full_logs:
        extract_full(archive_file, os.path.join(out_dir, pfx))
    else:
        extract_only_race_log(archive_file,
                              os.path.join(out_dir, f'race-{pfx}.log'))


out_files = []

for idx, row in filtered[filtered['Item'].isin(choices)].iterrows():
    lf = row['File']
    tm = row[er_utils.lm_utc]
    tm = tm.replace(':', '-')
    tm = tm.replace(' ', '@')
    print(f'Fetching {lf}')
    out_file = f'{tm}.7z'
    out_files.append(out_file)
    er_aws.download_file(log_bucket, lf, out=out_file)
    # extract_race_log(out_file)

for f in out_files:
    extract_race_log(f)
    os.remove(f)
