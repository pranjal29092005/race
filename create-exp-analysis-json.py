import os, sys, json
import jinja2, argparse
from datetime import datetime
from jinja2 import Environment as j2env, FileSystemLoader as j2fsl

parser = argparse.ArgumentParser('create-exp-analysis-json.py')
parser.add_argument('--id', dest='exp_id', type=int, required=True, help='Exposure Id')
parser.add_argument('--exp-type', dest='exp_type', type=str, required=True, choices=['p', 'a'], help='Exposure Type')
parser.add_argument('--date', dest='date', type=str, required=False, help='Analysis Date')
args = parser.parse_args()

cur_dir = os.path.dirname(os.path.abspath(__file__))
template_path = os.path.join(cur_dir, 'Templates')
env = j2env(loader=j2fsl(template_path), trim_blocks=True)
template = env.get_template('race_exposure_analysis.j2')

out_file_name = os.path.join(cur_dir, 'Generated', f'exposure_analysis_{args.exp_id}.json')
if args.date:
    analysis_date = datetime.strptime(args.date, '%d-%m-%Y')
else:
    analysis_date = datetime.today()

summary_measures = ['#Assets', 'TIV', "GroundUpLoss"]
if args.exp_type == 'p':
    summary_measures.append('ContractLoss_Value All Types_GL')

out_str = json.loads(
    template.render(
        {
            'isPortfolio': 'true' if args.exp_type == 'p' else 'false',
            'summaryMeasures': summary_measures,
            'exp_id': args.exp_id,
            'date': {
                "Year": analysis_date.year,
                "Month": analysis_date.month,
                "Day": analysis_date.day
            }
        }
    )
)

with open(out_file_name, 'w') as fout:
    json.dump(out_str, fout, indent=4)
