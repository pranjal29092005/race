import os, sys, json, re
import pandas as pd
import numpy as np
import er_db, click
import er_utils
import jinja2, argparse, uuid
from datetime import datetime
from jinja2 import Environment as j2env, FileSystemLoader as j2fsl
import psycopg2, psycopg2.extras
from psycopg2.extras import DictCursor, RealDictCursor
import shutil, subprocess

keys = [s.lower() for s in er_db.get_db_keys()]

parser = argparse.ArgumentParser(prog='create test')
parser.add_argument('--env', dest='env', choices=keys, default='uat')
parser.add_argument('--id', dest='id', required=True, type=int)
parser.add_argument('--name', dest='name', required=True, type=str)
parser.add_argument('--portfolio', '-p', dest='portfolio', action='store_true')
parser.add_argument('--event-id', dest='event_id', required=False, type=int)
parser.add_argument('--spider', '-s', dest='spider', required=False, action='store_true')
parser.add_argument('--program', dest='program', required=False, type=int)
parser.add_argument('--today', '-t', dest='today', action='store_true')
parser.add_argument('--cause-of-loss', '-c', dest='cause_of_loss', required=False, type=str)
parser.add_argument('--reinsurance', '-r', dest='reinsurance_vars', required=False, action='store_true')
parser.add_argument('-with-date', '-d', dest='with_date', required=False, action='store_true')
parser.add_argument('-v1', dest='use_api_v1', action='store_true')
parser.add_argument('-x', dest='external_dir', action='store_true', required=False)


class create_test(object):
    def __init__(self, argv, **kwargs):
        self.argv = argv
        module = kwargs.get('module')
        self.init_vars()
        self.init_exes()
        if module is None or module is False:
            self.init_env()
        self.event_info = None
        self.cause_of_loss = None
        self.sev_model_id = None
        self.event_id = None
        self.event_set_id = None
        self.reins_vars = [
            'Fac Exposed Limit', 'Net of Fac Exposed Limit', 'Treaty Exposed Limit', 'Net Pre Cat Exposed Limit',
            'Fac loss', 'Net of Fac Loss', 'Treaty Loss', 'Net Pre Cat Loss', 'Net Post Cat Loss',
            'Net Post Cat Exposed Limit', 'Reinsurance Gross Loss', 'Reinsurance Gross Exposed Limit'
        ]

    def get_event_info(self, cursor):
        if self.argv.event_id is not None:
            self.event_id = self.argv.event_id

            cursor.execute(f'select "EVENT_SEV_MODEL_ID" from race."B_EVENT_SEVERITY" where "EVENT_ID"={self.event_id}')
            row = cursor.fetchone()
            self.sev_model_id = row['EVENT_SEV_MODEL_ID']

            cursor.execute(f'select "EVENT_SET_ID","LABL" from race."M_EVENT" where "ID"={self.event_id}')
            row = cursor.fetchone()
            event_set_id = row['EVENT_SET_ID']
            event_name = row['LABL']

            cursor.execute(f'select "PERIL","SOURCE" from "GetEventSetDetails"({event_set_id})')
            row = cursor.fetchone()
            if row is not None:
                peril = row['PERIL']
                source = row['SOURCE']
            else:
                peril = ''
                source = ''

            cursor.execute(f'select "SHAPE_TYPE" from "GetEventSeverity"({self.sev_model_id},{self.event_id})')
            row = cursor.fetchone()
            shape_type = row['SHAPE_TYPE']

            cursor.execute(f'select "SUB_PERIL" from "GetEventSevModelDetails"({self.sev_model_id})')
            row = cursor.fetchone()
            sub_peril = row['SUB_PERIL']

            self.event_info = {
                'id': self.event_id,
                'sev_id': self.sev_model_id,
                'name': event_name,
                'peril': peril,
                'subPeril': sub_peril,
                'source': source,
                'type': shape_type,
                'delta': {
                    'lat': 0.0,
                    'lon': 0.0
                }
            }

    def render(self, template_name, data):
        tmpl = self.env.get_template(f'{template_name}.j2')
        return tmpl.render(data).replace('\n', '').replace("\u2013", " - ").replace("  ", " ").replace("'", '"')

    def write_json(self, d, prefix):
        out_file_name = os.path.join(self.out_dir, f'{prefix}.json')
        d = json.loads(d)
        with open(out_file_name, 'w') as f:
            f.write(json.dumps(d, indent=4))

    def write_ramp(self, d, prefix):
        out_file_name = os.path.join(self.out_dir, f'{prefix}.ramp')
        with open(out_file_name, 'w') as f:
            f.write(d)
        subprocess.call([self.exes['clang-format'], '-i', out_file_name])

    def import_exposure(self):
        data = {'exposures': [{'id': self.exp['id'], 'portfolio': self.argv.portfolio}]}
        idx = len(self.steps)
        prefix = f'import_exposure_{idx}'
        self.write_ramp(self.render('import', data), prefix)
        self.steps.append(prefix)

    def run_analysis(self):
        analysis = {'handDrawnEvent': False, 'events': False}
        if self.argv.with_date:
            analysis['date'] = self.exp['date']
        data = {'analysisList': [analysis], 'damageFunctionId': 100021, 'portfolio': self.argv.portfolio}
        if self.event_info is not None:
            data['events'] = [self.event_info]
        if self.argv.cause_of_loss is not None:
            filter = {'op': 'EQ', 'attr': 'Cause Of Loss', 'value': self.argv.cause_of_loss}
            data['filterType'] = 'AND'
            data['filters'] = [filter]
        if self.argv.program is not None:
            data['program'] = self.argv.program
        idx = len(self.steps)
        prefix = f'run_analysis_{idx}'
        self.write_json(self.render('analysis_json', data), prefix)
        self.write_ramp(self.render('analysis_script', data), prefix)
        self.steps.append(prefix)

    def get_summary(self):
        data = {'analysisCount': 1, 'vars': ['TIV', 'GroundUpLoss', '#Assets']}
        if (self.argv.portfolio):
            data['vars'].extend(['ContractLoss_Value All Types_EL', 'ContractLoss_Value All Types_EL'])
            if self.argv.reinsurance_vars:
                data['vars'].extend(self.reins_vars)
        idx = len(self.steps)
        prefix = f'get_summary_{idx}'
        self.write_json(self.render('summary_json', data), prefix)
        script_name = 'summary_script_old' if self.argv.use_api_v1 else 'summary_script'
        self.write_ramp(self.render(script_name, data), prefix)
        self.steps.append(prefix)

    def store_analysis(self):
        data = {
            'analysisIndex': 0,
            'attributes': ['Latitude', 'Longitude', 'Asset Number', 'Asset Name'],
            'contributionAttributes': ['TIV'],
            'auditId': self.exp['audit_id'],
            'exposureId': self.exp['id'],
            'exposureName': self.exp['name'],
            'fileName': str(uuid.uuid1()) + '.parquet'
        }
        if self.argv.portfolio or self.argv.program is not None:
            data['attributes'].extend(
                ['ContractLoss_Value All Types_EL', 'ContractLoss_Value All Types_GL', 'Contract Number']
            )
            data['contributionAttributes'].extend(
                ['ContractLoss_Value All Types_EL', 'ContractLoss_Value All Types_GL']
            )
        if self.argv.program is not None:
            data['attributes'].extend(['Retained Limit', 'Retained Loss'])
        if self.argv.reinsurance_vars:
            data['attributes'].extend(self.reins_vars)
        if self.event_info is not None:
            data['attributes'].extend(['Intensity'])
            data['contributionAttributes'].extend(['Intensity'])
        idx = len(self.steps)
        prefix = f'store_analysis_{idx}'
        self.write_json(self.render('StoreAnalysisResults_json', data), prefix)
        self.write_ramp(self.render('StoreAnalysisResults_script', data), prefix)
        self.steps.append(prefix)

    def topn_assets(self):
        data = {
            'analysisCount': 1,
            'count': 10,
            'sortMeasure': 'TIV',
            'additionalSortMeasure': 'TIV',
            'topnBy': 'assets',
            'attributes': ['Latitude', 'Longitude', 'ROW', 'Asset Name', 'Asset Number', 'GroundUpLoss']
        }
        if self.argv.portfolio:
            data['attributes'].extend([self.vars['gel'], self.vars['gl'], 'Contract Number'])
            data['additionalSortMeasure'] = self.vars['gel']
            if self.argv.reinsurance_vars:
                data['attributes'].extend(self.reins_vars)
        idx = len(self.steps)
        prefix = f'topn_assets_{idx}'
        self.write_json(self.render('topn_json', data), prefix)
        script_name = 'topn_script_old' if self.argv.use_api_v1 else 'topn_script'
        self.write_ramp(self.render(script_name, data), prefix)
        self.steps.append(prefix)

    def topn_contracts(self):
        if not self.argv.portfolio:
            return
        data = {
            'analysisCount': 1,
            'count': 10,
            'sortMeasure': self.vars['gel'],
            'additionalSortMeasure': 'TIV',
            'topnBy': 'contracts',
            'attributes': ['GroundUpLoss', self.vars['gel'], self.vars['gl'], 'Contract Number']
        }
        if (self.argv.reinsurance_vars):
            data['attributes'].extend(self.reins_vars)
        idx = len(self.steps)
        prefix = f'topn_contracts_{idx}'
        self.write_json(self.render('topn_json', data), prefix)
        script_name = 'topn_script_old' if self.argv.use_api_v1 else 'topn_script'
        self.write_ramp(self.render(script_name, data), prefix)
        self.steps.append(prefix)

    def init_vars(self):
        self.vars = {
            'gel': 'ContractLoss_Value All Types_EL',
            'gl': 'ContractLoss_Value All Types_GL',
            'gul': 'GroundUpLoss'
        }

    def init_exes(self):
        self.exes = {'clang-format': shutil.which('clang-format'), 'git': shutil.which('git')}

    def get_portfolio(self, cursor):
        stmt = f'select * from race.m_portfolio where portfolio_id={self.argv.id}'
        cursor.execute(stmt)
        row = cursor.fetchone()
        self.exp = {
            'audit_id': row['audit_id'],
            'schedule_id': 0,
            'id': self.argv.id,
            'name': row['portfolio_name'],
            'date': row['max_inception_date'],
            'bbox': [row['UL_LON'], row['UL_LAT'], row['LR_LON'], row['LR_LAT']]
        }

    def get_sov(self, cursor):
        stmt = f'select * from race."M_ASSET_SCHEDULE" where "ID"={self.argv.id}'
        cursor.execute(stmt)
        row = cursor.fetchone()
        self.exp = {
            'audit_id': row['AUDIT_ID'],
            'schedule_id': self.argv.id,
            'id': self.argv.id,
            'name': row['SCHEDULE_NAME'],
            'date': row['max_valuation_date'],
            'bbox': [row['UL_LON'], row['UL_LAT'], row['LR_LON'], row['LR_LAT']]
        }

    def cap_words(self, arg):
        if '_' in arg:
            return arg.title().replace('_', ' ')
        elif '-' in arg:
            return arg.title().replace('-', ' ')
        return arg.title()

    def write_input_json(self):
        out_file = os.path.join(self.out_dir, 'input.json')
        test_steps = []
        reg = re.compile('(.*)_([0-9]+)')
        for step in self.steps:
            m = reg.search(step)
            if m is None:
                raise NameError(f'Incorrect step name {step}')
            idx = int(m[2])
            name = self.cap_words(m[1])
            test_step = {
                'Id': idx,
                'Step Name': f'{idx+1}. {name}',
                'Script File': f'{step}.ramp',
                'Input File': f'{step}.json'
            }
            if idx > 0:
                test_step['Depends On'] = [idx - 1]
            test_steps.append(test_step)
        data = {
            'Type': 'P' if self.argv.portfolio else 'A',
            'Environment': self.argv.env.capitalize(),
            'Steps': test_steps,
            'Description': self.argv.name.capitalize(),
            'Test Name': self.cap_words(self.argv.name)
        }
        with open(out_file, 'w') as fout:
            fout.write(json.dumps(data, indent=4))

    def get_exposure_details(self, cursor):
        if self.argv.portfolio:
            self.get_portfolio(cursor)
        else:
            self.get_sov(cursor)
        if self.argv.today:
            self.exp['date'] = datetime.today()

    def init_env(self):
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        template_path = os.path.join(cur_dir, 'Templates')
        src_dir = subprocess.check_output([self.exes['git'], 'rev-parse',
                                           '--show-toplevel']).decode('utf-8').split('\n')[0]

        self.exp = None
        self.steps = []
        self.out_dir = os.path.join(src_dir, 'Tools', 'TestCases', self.argv.name)
        if self.argv.external_dir:
            test_dir = os.environ.get('RACE_TESTS_DIR')
            if test_dir is not None and os.path.exists(test_dir):
                self.out_dir = os.path.join(test_dir, self.argv.name)
        self.env = j2env(loader=j2fsl(template_path), trim_blocks=True)
        self.env.globals.update(zip=zip)

        if os.path.exists(self.out_dir):
            click.confirm(f'Okay to overwrite test folder {self.argv.name}', abort=True)
        else:
            os.makedirs(self.out_dir, exist_ok=True)

    def pick_selection(self, df):
        er_utils.tabulate_df(df, showindex=True)
        valid_choices = click.IntRange(0, len(df))
        choice = click.prompt(f'Enter Choice (1 - {len(df)}), 0 to exit', type=valid_choices)
        return df.iloc[[choice - 1]].iloc[0]

    def query_peril(self, conn):
        stmt = 'select peril_id,peril_name,peril_code from "GetPerilData"()'
        df = pd.read_sql(stmt, conn)
        df = df[~df.peril_code.isin(['ALLP', 'AOP'])][['peril_name', 'peril_code']]
        df.index = np.arange(1, len(df) + 1)
        return self.pick_selection(df).peril_name

    def query_sub_peril(self, conn, peril):
        stmt = f"select sub_peril_name from \"GetPerilHierarchy\"() where peril_name='{peril}'"
        df = pd.read_sql(stmt, conn)
        df.index = np.arange(1, len(df) + 1)
        return self.pick_selection(df).sub_peril_name

    def spider(self, conn):
        measure = 'TIV'
        if self.argv.portfolio:
            measure = 'ContractLoss_Value All Types_EL'
        peril = self.query_peril(conn)
        sub_peril = self.query_sub_peril(conn, peril)
        bbox = self.exp['bbox']
        data = {
            'portfolio': self.argv.portfolio,
            'peril': peril,
            'sub_peril': sub_peril,
            'lr_lat': bbox[3],
            'lr_lon': bbox[2],
            'ul_lat': bbox[1],
            'ul_lon': bbox[0]
        }
        idx = len(self.steps)
        prefix = f'spider_{idx}'
        self.write_ramp(self.render('spider_script', data), prefix)
        self.write_json(self.render('spider_json', data), prefix)
        self.steps.append(prefix)

    def contribution_by_attribute(self, measure, name):
        data = {'analysisCount': 1, 'measure': measure, 'attribute': 'Valuation Currency'}
        idx = len(self.steps)
        prefix = f'contribution_{idx}'

        self.write_ramp(self.render('contribution_script', data), prefix)
        self.write_json(self.render('contribution_json', data), prefix)
        self.steps.append(prefix)

    def intensity_contribution(self, measure, name, cursor):
        cursor.execute(f'select bin_range from "GetEventBinRangeV2"({self.event_id})')
        row = cursor.fetchone()
        bins = row['bin_range']
        data = {'analysisCount': 1, 'attribute': 'Intensity', 'measure': measure, 'bins': bins}
        idx = len(self.steps)
        prefix = f'contribution_{idx}'

        self.write_ramp(self.render('contribution_script', data), prefix)
        self.write_json(self.render('contribution_json', data), prefix)
        self.steps.append(prefix)

    def do_it(self):
        with er_db.get_db_conn(self.argv.env) as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            self.get_event_info(cursor)
            self.get_exposure_details(cursor)
            self.import_exposure()
            self.run_analysis()
            if self.argv.spider:
                self.spider(conn)
            else:
                self.get_summary()
                # self.topn_assets()
                self.store_analysis()
                # self.topn_contracts()
        self.write_input_json()


if __name__ == '__main__':
    argv = parser.parse_args()
    obj = create_test(argv, module=False)
    obj.do_it()
