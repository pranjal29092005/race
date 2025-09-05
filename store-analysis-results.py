import os, sys, json
import argparse, shutil, subprocess
import click, re, er_db
import jinja2, uuid
from icecream import ic
from jinja2 import Environment as j2env, FileSystemLoader as j2fsl

parser = argparse.ArgumentParser(prog='store-analysis-step')
parser.add_argument(
    '--dir', dest='dir', required=True, type=str, help='directory name with in the tests directory in current race dir'
)

argv = parser.parse_args()
clang_format = shutil.which('clang-format')

race_test_dir = os.environ.get('RACE_TESTS_DIR')

race_dir = subprocess.check_output(['git', 'rev-parse', '--show-toplevel']).decode('utf-8').split('\n')[0]
test_dir = os.path.join(race_dir, f'Tools/TestCases/{argv.dir}')
if race_test_dir is not None:
    if os.path.exists(os.path.join(race_test_dir, argv.dir)):
        test_dir = os.path.join(race_test_dir, argv.dir)
if test_dir is not None:
    test_dir = os.path.normpath(test_dir)
ic(test_dir)
sov_reg = re.compile('ImportExposureFromDB *\\(([0-9]+) *, *([0-9])\\)')
portfolio_reg = re.compile('ImportContractPortfolio *\\(([0-9]+) *, *([0-9])\\)')

if not os.path.exists(test_dir):
    sys.exit(-1)


def fatal_error(msg):
    click.secho(msg, fg='red', reverse=True, bold=True)
    sys.exit(-1)


if not os.path.exists(test_dir):
    fatal_error(f'Test dir {test_dir} does not exist')


def read_import_ramp(step):
    ramp_file = os.path.join(test_dir, step['Script File'])
    portfolio = True
    exp_id = None
    analysis_index = 0
    with open(ramp_file, 'r') as f:
        for l in f.readlines():
            m = sov_reg.search(l)
            if m is not None:
                portfolio = False
                exp_id = int(m[1])
                analysis_index = int(m[2])
                break
            m = portfolio_reg.search(l)
            if m is not None:
                portfolio = True
                exp_id = int(m[1])
                analysis_index = int(m[2])
                break
    return (exp_id, analysis_index, portfolio)


input_json = None
with open(os.path.join(test_dir, 'input.json'), 'r') as f:
    input_json = json.load(f)

steps = input_json['Steps']
env = input_json['Environment']
exp_id, analysis_index, portfolio = read_import_ramp(steps[0])
audit_id = None
exp_name = None

if exp_id is None:
    fatal_error("Exposure id not found")

with er_db.get_db_conn(env) as conn:
    audit_id, portfolio = er_db.get_audit_id_v2(conn, exp_id, portfolio)
    exp_name = er_db.get_exp_name(conn, exp_id, portfolio)

template_path = os.path.join(race_dir, 'Helpers', 'Templates')
template_env = j2env(loader=j2fsl(template_path), trim_blocks=True)

store_analysis_json = template_env.get_template('StoreAnalysisResults_json.j2')
store_analysis_ramp = template_env.get_template('StoreAnalysisResults_script.j2')

step_id = steps[-1]['Id'] + 1
store_analysis_json_file = os.path.join(test_dir, f'store_analysis_results_{step_id}.json')
store_analysis_ramp_file = os.path.join(test_dir, f'store_analysis_results_{step_id}.ramp')

step = {
    'Id': step_id,
    'Step Name': 'Store Analysis Results',
    'Group': 'Default',
    'Input File': os.path.basename(store_analysis_json_file),
    'Script File': os.path.basename(store_analysis_ramp_file),
    'Depends On': [step_id - 1]
}
steps.append(step)
input_json['Steps'] = steps
with open(os.path.join(test_dir, 'input.json'), 'w') as f:
    json.dump(input_json, f, indent=4)

data = {
    'analysisIndex': analysis_index,
    'exposureId': exp_id,
    'auditId': audit_id,
    'exposureName': exp_name,
    'attributes': ["Latitude", "Longitude", "TIV", "Asset Name"],
    'contributionAttributes': ['TIV'],
    'akey': str(uuid.uuid1())
}

with open(store_analysis_json_file, 'w') as f:
    s = store_analysis_json.render(data)
    s = json.loads(s)
    json.dump(s, f, indent=4)

with open(store_analysis_ramp_file, 'w') as f:
    f.write(store_analysis_ramp.render(data))
subprocess.call([clang_format, '-i', store_analysis_ramp_file])
