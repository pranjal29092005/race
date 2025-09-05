import os, sys, json
import argparse, shutil, subprocess
import click
from icecream import ic

parser = argparse.ArgumentParser(prog='split-analysis-call')
parser.add_argument('--dir', required=True, dest='dir', type=str)
argv = parser.parse_args()

src_dir = subprocess.check_output(['git', 'rev-parse', '--show-toplevel']).decode('utf-8').split('\n')[0]


def read_input_json():
    fn = os.path.join(src_dir, f'Tools/TestCases/{argv.dir}/input.json')
    if not os.path.exists(fn):
        click.secho('No input.json file found', fg='red', bold=True)
        sys.exit(-1)
    with open(fn, 'r') as fp:
        return json.load(fp)


def write_input_json(input_json):
    fn = os.path.join(src_dir, f'Tools/TestCases/{argv.dir}/input.json')
    with open(fn, 'w') as fp:
        json.dump(input_json, fp, indent=4)


def create_analysis_ramp(analysis_idx):
    fn = os.path.join(src_dir, f'Tools/TestCases/{argv.dir}/analysis_{analysis_idx}.ramp')
    key = f'Analysis{analysis_idx}'
    lines = [
        'string errorMessage;', f"if (!runAnalysis('{key}', {analysis_idx}, errorMessage))",
        '{ SendErrorWithCode(errorMessage); return;}'
    ]
    with open(fn, 'w') as fp:
        fp.write('\n'.join(lines))
        fp.write('\n')
    return os.path.basename(fn)


def get_steps_list(input_json):
    steps = []
    for s in input_json['Steps']:
        step = s
        step.pop('Id')
        depends_key = step.get('Depends On')
        if depends_key is not None:
            step.pop('Depends On')
        if not 'analysis_(new)' in s['Script File']:
            steps.append(step)
        else:
            fn = create_analysis_ramp(0)
            step['Script File'] = fn
            cur_step = step.copy()
            cur_step['Step Name'] = f"{cur_step['Step Name']}_0"
            steps.append(cur_step)
            fn = create_analysis_ramp(1)
            step['Script File'] = fn
            cur_step = step.copy()
            cur_step['Step Name'] = f"{cur_step['Step Name']}_1"
            steps.append(cur_step)

    for idx, step in enumerate(steps):
        step['Id'] = idx
        if idx:
            step['Depends On'] = [idx - 1]

    return steps


input_json = read_input_json()
steps = get_steps_list(input_json)
input_json['Steps'] = steps
write_input_json(input_json)
