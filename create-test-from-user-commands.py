import os, sys, json, click
import argparse, re
from icecream import ic

parser = argparse.ArgumentParser(prog='create-test-from-user-commands')
parser.add_argument('--user', '-u', dest='user', required=True, type=str)
parser.add_argument('--log-file', '-l', dest='log_file', required=True, type=str)
parser.add_argument('--out-name', '-o', dest='out_name', required=True, type=str)
argv = parser.parse_args()

this_dir = os.path.dirname(os.path.abspath(__file__))
test_dir = os.path.join(this_dir, '../Tools/TestCases', argv.out_name)

reg = re.compile('.*Message received:(.*)$')
cmds = []

with open(argv.log_file, 'r') as f:
    for l in f.readlines():
        m = reg.match(l)
        if m is not None:
            item = json.loads(m[1])
            user = item.get('User')
            if (user == argv.user):
                ic(item)
                if click.confirm('Add'):
                    cmds.append(item)

if not cmds:
    sys.exit(-1)
os.makedirs(test_dir, exist_ok=True)
inp_json = os.path.join(test_dir, 'input.json')
test_conf = {'Description': argv.out_name, 'Test Name': argv.out_name}
steps = []
for idx, c in enumerate(cmds):
    for k in ['Command', 'CommandID', 'User']:
        c.pop(k)
    ic(c)
    ramp_file = os.path.join(test_dir, f'step_{idx}.ramp')
    json_file = os.path.join(test_dir, f'step_{idx}.json')
    step = {
        'Id': idx,
        'Step Name': f'Step {idx}',
        'Script File': os.path.basename(ramp_file),
        'Input File': os.path.basename(json_file)
    }
    if idx:
        step['Depends On'] = [idx - 1]
    steps.append(step)
    script = c.pop('Script')
    with open(ramp_file, 'w') as f:
        f.write(script)
    if not c:
        continue
    with open(json_file, 'w') as f:
        json.dump(c, f, indent=4)
test_conf['Steps'] = steps
with open(inp_json, 'w') as f:
    json.dump(test_conf, f, indent=4)
