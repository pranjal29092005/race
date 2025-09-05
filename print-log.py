import os, sys, json
import er_race_log
import argparse

parser = argparse.ArgumentParser(prog='Print log messages on console')
parser.add_argument('--file', dest='file', help='Log file to read', type=str)
argv = parser.parse_args()

df = er_race_log.read_log_2(argv.file)

mask = df['Analysis0'].notnull() & df['User'].str.contains('email.processing@')

for idx, row in df.loc[mask].tail(10).iterrows():
    print(row['User'])
    print(row['Analysis0'])
    print('--------------')

# for idx, row in df.loc[df.Analysis0.notnull():].tail(10):
# print(row['Analysis0'])
