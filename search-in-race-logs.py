import os, sys, re
import subprocess, glob
import libarchive.read as lar
import click
import argparse

parser = argparse.ArgumentParser(prog='search-in-race-logs')
parser.add_argument(
    '--logs-dir',
    dest='logs_dir',
    type=str,
    help=
    'Directory to look for log files. If not provided, this script will check for RACE_LOGS_DIR environment variable',
    required=False
)
parser.add_argument(
    '--search',
    '-s',
    dest='search',
    type=str,
    required=True,
    help='String to search (minimum 3 chars length) the logs for'
)

argv = parser.parse_args()
if argv.logs_dir is None:
    argv.logs_dir = os.environ.get('RACE_LOGS_DIR')

if argv.logs_dir is None:
    print("No logs directory specified. Exiting ...")
    sys.exit(-1)

if len(argv.search) < 3:
    print("Too small string to search for. Exiting ...;")
    sys.exit(-1)

files = glob.glob(os.path.join(argv.logs_dir, '*.7z'))

for f in files:
    found = False
    try:
        with lar.file_reader(f) as reader:
            for entry in reader:
                if (entry.pathname == 'race.log' or entry.pathname == 'reinsurance-accumulator.log'):
                    with open('race.log', 'wb') as op:
                        for block in entry.get_blocks():
                            op.write(block)
                    with open('race.log', 'r') as ip:
                        for l in ip.readlines():
                            if argv.search in l:
                                found = True
                                break
                    if found:
                        click.secho(f, fg='green', bold='True')
    except Exception as e:
        click.secho(f'{os.path.basename(f)}: {e}', fg='red', bold=True)
