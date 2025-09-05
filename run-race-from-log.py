import argparse
import json, os, re
import pandas as pd
import er_race_log
from collections import OrderedDict

parser = argparse.ArgumentParser(prog='Run Race')
parser.add_argument('--env',
                    dest='env',
                    choices=['uat', 'prod'],
                    default='uat')
parser.add_argument('--file',
                    dest='file',
                    type=str,
                    required=True,
                    help='log file to read')
parser.add_argument('--user', dest='user', type=str, required=False)
argv = parser.parse_args()

print(argv)

cmds = read_log()
cmds.to_csv('cmds.csv', index=False)
