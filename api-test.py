import os, sys, json, argparse
from race_server import RaceServer
import pandas as pd
import er_utils
import janitor

rs = RaceServer(user='raceclienttester@eigenrisk.com')
rs.create_session()

parser = argparse.ArgumentParser(prog='api-test')
parser.add_argument('--input', dest='input', type=str, required=False)
argv = parser.parse_args()

data = {
    "Exposure": {
        'ScheduleId': 4195384,
        # 'ScheduleId': 1347,
        'IsPortfolio': False,
        'SortBy': 'Valuation Date'
    },
    'Script':
        '''
string errorMessage;
if (!ExposureSummary('Exposure', errorMessage)) {
    Send('Error!', errorMessage);
    return;
}
    '''
}

cmd = rs.create_command()
if argv.input and os.path.exists(argv.input):
    with open(argv.input, 'r') as f:
        data = json.load(f)

cmd.update(data)
cmd['Script'] = cmd['Script'].replace('\n', '')

with open('input.json', 'w') as f:
    json.dump(cmd, f, indent=4)

res = rs.send_race(cmd)
val = res.get('Value')
if not val:
    sys.exit(0)

df = pd.DataFrame(val['ExposureSummary']).clean_names()
df = df[df.country == 'US']
er_utils.tabulate_df(df)
df.to_csv('/home/surya/exp-summary.csv', index=False)

# for c in cmds:
# cmd = rs.create_command()
# cmd.update(c)
# out = rs.send_race(cmd)
# print(json.dumps(out, indent=4))
