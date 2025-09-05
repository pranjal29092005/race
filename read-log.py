import os, sys, argparse, re
import pandas as pd
import er_race_log

parser = argparse.ArgumentParser(prog='Read log file')
parser.add_argument('--file',
                    dest='file',
                    help='Log file to read',
                    default='race.log')

sub_parsers = parser.add_subparsers(help='sub commands',
                                    title='commands',
                                    dest='command')

list_parser = sub_parsers.add_parser('list')
list_parser.add_argument(
    '--type',
    dest='list_type',
    choices=['users', 'events', 'exposures', 'exp-events'],
    help='List type in log file')

message_reg = re.compile(
    'Message received.*User": *"([^"]*)", *"CommandID": *"([^"]*)"')
reply_reg = re.compile(
    '"Type": *"Reply",*"UserID": *"([^"]*)", *"CommandID": *"([^"]*)"')

argv = parser.parse_args()

cmds = er_race_log.read_log(argv.file)

list_type = getattr(argv, 'list_type', None)
# user_id_list = []
if list_type is None:
    pass
elif (list_type == 'events'):
    cols = 'exp_type,exp_id,event_id'.split(',')
    print(cmds.drop_duplicates(subset=['group'], keep='first').loc[:, cols])
# elif (list_type == 'events'):
#     cols = ['Name', 'Exp Type', 'Id', 'Event Id']
#     mask = (cmds['Event Id'] != 0)
#     # print(cmds.loc[mask, cols])
# elif (list_type == 'exposures'):
#     cols = ['Name', 'Exp Type', 'Id']
#     # print(cmds[cols].query('`Exp Type`.str.contains("Sov")', engine='python'))
