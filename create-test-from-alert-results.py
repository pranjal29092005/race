import os, sys, json
import argparse
from datetime import datetime, timedelta
from jinja2 import Environment as j2env, FileSystemLoader as j2fsl

parser = argparse.ArgumentParser(prog='Test from alert results')
parser.add_argument('--env', dest='env', choices=['alerts', 'alerts2'], default='alerts')
parser.add_argument('--file', dest='file', type=str, required=True)
parser.add_argument('--out', dest='out', type=str, required=True)

argv = parser.parse_args()

alert_res = None
with open(argv.file, 'r') as f:
    alert_res = json.load(f)

this_dir = os.path.dirname(os.path.abspath(__file__))
template_path = os.path.normpath(os.path.join(this_dir, '../Tools/Templates'))
test_dir = os.path.normpath(os.path.join(this_dir, '../Tools/TestCases', argv.out))

files = {
    'import': os.path.join(test_dir, 'import_exposure.ramp'),
    'analysis_r': os.path.join(test_dir, 'analysis.ramp'),
    'analysis_j': os.path.join(test_dir, 'analysis.json'),
    'summary_r': os.path.join(test_dir, 'summary.ramp'),
    'summary_j': os.path.join(test_dir, 'summary.json'),
    'topncontracts_r': os.path.join(test_dir, 'topn_contracts.ramp'),
    'topncontracts_j': os.path.join(test_dir, 'topn_contracts.json'),
    'topn_assets_r': os.path.join(test_dir, 'topn_assets.ramp'),
    'topn_assets_j': os.path.join(test_dir, 'topn_assets.json')
}

env = j2env(loader=j2fsl(template_path), trim_blocks=True)

os.makedirs(test_dir, exist_ok=True)
is_portfolio = (alert_res["Exposure Type"] == 'P')

steps = []
id = 0

with open(files['import'], 'w') as f:
    d = {'portfolio': is_portfolio, 'exp_id': alert_res["Exposure ID"]}
    f.write(env.get_template('import_exposure.tmpl').render(d))
    steps.append({'Id': id, 'Step Name': 'Import Exposure', 'Script File': os.path.basename(files['import'])})
    id = id + 1

with open(files['analysis_r'], 'w') as rf, open(files['analysis_j'], 'w') as jf:
    dt = datetime.strptime(alert_res['Analysis Date'], '%a %b %d %H:%M:%S %Y UTC')
    d = {
        'dmg_func_id': 195,
        'quantile': 50,
        'dmg_adjustment': 1,
        'dt': dt,
        'portfolio': is_portfolio,
        'event_name': alert_res['Event Name'],
        'event_id': alert_res['Event ID'],
        'sev_model_id': alert_res['Sev Model ID']
    }
    rf.write(env.get_template('analysis.ramp.tmpl').render(d))
    jf.write(env.get_template('analysis.json.tmpl').render(d))
    steps.append(
        {
            'Id': id,
            'Step Name': 'Analysis',
            'Script File': os.path.basename(files['analysis_r']),
            'Input File': os.path.basename(files['analysis_j']),
            'Depends On': [0]
        }
    )
    id = id + 1

with open(files['summary_r'], 'w') as rf, open(files['summary_j'], 'w') as jf:
    dt = datetime.strptime(alert_res['Analysis Date'], '%a %b %d %H:%M:%S %Y UTC')
    d = {'portfolio': is_portfolio}
    rf.write(env.get_template('summary.ramp.tmpl').render(d))
    jf.write(env.get_template('summary.json.tmpl').render(d))
    steps.append(
        {
            'Id': id,
            'Step Name': 'Summary',
            'Script File': os.path.basename(files['summary_r']),
            'Input File': os.path.basename(files['summary_j']),
            'Depends On': [1]
        }
    )
    id = id + 1

with open(files['topn_assets_r'], 'w') as rf, open(files['topn_assets_j'], 'w') as jf:
    d = {'var': 'Intensity', 'count': -1, 'additionalSortMeasure': 'TIV'}
    rf.write(env.get_template('topn_assets.ramp.tmpl').render(d))
    jf.write(env.get_template('topn_assets.json.tmpl').render(d))
    steps.append(
        {
            'Id': id,
            'Step Name': 'Topn Assets',
            'Script File': os.path.basename(files['topn_assets_r']),
            'Input File': os.path.basename(files['topn_assets_j']),
            'Depends On': [1]
        }
    )

db_env = 'Prod' if argv.env == 'alerts' else 'Prod2'
input_json = {
    'Description': argv.out,
    'Type': 'P' if is_portfolio else 'A',
    'Test Name': argv.out,
    'Environment': db_env,
    'Steps': steps
}

with open(os.path.join(test_dir, 'input.json'), 'w') as f:
    f.write(json.dumps(input_json, indent=4))
