import os, sys, re
import argparse, json
import jmespath, psycopg2
from psycopg2.extras import DictCursor

parser = argparse.ArgumentParser(prog='get-bad-events-from-race-log')
parser.add_argument('--file',
                    dest='file',
                    type=str,
                    help='race log file',
                    required=True)
argv = parser.parse_args()

with open(argv.file, 'r') as f:
    lines = f.readlines()

exception_line_reg = re.compile(
    '\((email\.processing[12345]?@eigenrisk\.com)\).*Caught exception in spatial filtering'
)
command_line_reg = re.compile(
    '\((email\.processing[12345]?@eigenrisk\.com)\).*"Command":"Execute"')

cmd_reg = re.compile('Message received: (.*)')

line_num = 0
commands = {}
exception_lines = {}

for l in lines:
    line_num = line_num + 1
    m = command_line_reg.search(l)
    if m is not None:
        commands[line_num] = {'user': m.group(1), 'cmd': l}
    else:
        m = exception_line_reg.search(l)
        if m is not None:
            exception_lines[line_num] = {'user': m.group(1)}


def get_latest_cmd(line_num, user):
    lines = filter(
        lambda elem: (elem[0] < line_num) and elem[1]['user'] == user,
        commands.items())
    try:
        cmd_line = list(sorted(lines, key=lambda elem: elem[0],
                               reverse=True))[0]
        m = cmd_reg.search(cmd_line[1]['cmd'])
        if m is not None:
            cmd = json.loads(m.group(1))
            event = jmespath.search('Analysis0.Events[0].EventID', cmd)
            # sev_model_id = jmespath.search(
            # 'Analysis0.Events[0].SeverityModelID', cmd)
            return event

    except Exception as e:
        print('Exception: {}'.format(e))
        return None


def get_cmd(k):
    line_num = k[0]
    user = k[1]['user']
    return get_latest_cmd(line_num, user)


events = set()
for event in map(get_cmd, exception_lines.items()):
    if event is not None:
        events.add(event)

print(events)

# db_info = {
#     'host': os.environ['PG_HOST'],
#     'dbname': os.environ['PG_DB'],
#     'port': os.environ['PG_PORT'],
#     'user': os.environ['PG_USER'],
#     'password': os.environ['PG_PW']
# }

# conn_str = 'host={host} dbname={dbname} user={user} password={password} port={port}'.format_map(
#     db_info)

# with psycopg2.connect(conn_str) as conn:
# for event in events:
# cursor = conn.cursor(cursor_factory=DictCursor)
# stmt = f'select * from "logInvalidEvents"({event})'
# cursor.execute(stmt)
