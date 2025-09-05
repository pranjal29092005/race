import os, sys, json
from race_server import RaceServer
from argparse import ArgumentParser
from colorama import init
from colorama import Fore, Back, Style
from icecream import ic
import jmespath
import click

init()

parser = ArgumentParser(prog='run-race-test-from-folder')
parser.add_argument('--dir', '-d', dest='directory', required=True, type=str)
parser.add_argument('--user-id', '-u', dest='user_id', required=False, type=int)

argv = parser.parse_args()

click.echo(click.style('Creating session...', fg='cyan', bg='black', bold=True))

user = 'raceclienttester@eigenrisk.com'
user_id = 0
if argv.user_id:
    user = f'user_{argv.user_id}@dummy.com'
    user_id = argv.user_id

rs = RaceServer(user=user)
rs.create_session(user_id=user_id)

input_json = os.path.join(argv.directory, 'input.json')
conf = None
with open(input_json, 'r') as f:
    conf = json.load(f)

if conf is None:
    print('Input json file not found. Exiting')
    sys.exit(-1)

click.echo(click.style(f'Running test for {conf["Description"]}', fg='cyan', bg='black', bold=True))


def read_step_json(step_file):
    fn = os.path.join(argv.directory, step_file)
    if not os.path.exists(fn):
        return {}
    with open(fn, 'r') as f:
        return json.load(f)


def read_step_ramp(ramp_file):
    fn = os.path.join(argv.directory, ramp_file)
    if not os.path.exists(fn):
        return ''
    with open(fn, 'r') as f:
        return ' '.join(f.readlines())


steps = conf['Steps']
for idx, step in enumerate(steps):
    click.echo(click.style(f'Step: {step["Step Name"]} ', fg='yellow'), nl=False)
    out_file = step['Step Name'].lower().replace(' ', '_').replace('.', '')
    out_file = f'results_{idx}_{out_file}.json'
    step_name = step['Step Name'].lower().replace(' ', '_')
    json_file = step.get('Input File')
    inp = {}
    if json_file is None:
        json_file = f'{step_name}.json'
    inp.update(read_step_json(json_file))
    ramp_file = step.get('Script File')
    if ramp_file is None:
        ramp_file = f'{step_name}.ramp'
    inp['Script'] = read_step_ramp(ramp_file)
    cmd = rs.create_command()
    cmd.update(inp)
    resp = rs.send_race(cmd)
    progress = resp.get('ResultName')
    while (progress is not None and 'progress' in progress.lower()):
        resp = rs.recv_race()
        progress = resp.get('ResultName')
    value = resp.get('Value')
    time_obj = resp.get('Time')
    cmd = resp.get('CommandID')
    # click.secho('Command: ', nl=False)
    # click.secho(cmd, fg='magenta', bold=True)
    if time_obj is not None:
        click.echo(click.style(f' => {time_obj["Duration"]}', fg='blue'))
    if value is not None:
        if isinstance(value, int):
            if value == 1:
                click.echo(click.style('Success', fg='green'))
            else:
                click.echo(click.style('Failed', fg='red'))
                break
        else:
            num_assets = jmespath.search('Value.Analysis0."Asset Count"', resp)
            if num_assets:
                click.secho('Num Assets: ', nl=False)
                click.secho(f'{num_assets}', fg='green', bold=True)
            with open(out_file, 'w') as f:
                json.dump(resp, f, indent=4)
