import os, sys, json
from race_server import RaceServer as rs
import shutil, subprocess


class AlertJsonException(Exception):
    pass


class AlertJsonNotFoundException(AlertJsonException):
    pass


class AlertJson(object):
    def __init__(self, fn, **kwargs):
        self.alert = None
        self.race_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.is_portfolio = False
        self.clang_format = shutil.which('clang-format')
        self.steps = []
        if not os.path.exists(fn):
            raise AlertJsonNotFoundException()

        with open(fn, 'r') as f:
            self.alert = json.load(f)

        dry_run = kwargs.get('dry_run', False)
        self.race_server = rs(dry_run=dry_run)
        self.race_server.create_session()

    def __enter__(self):
        return self

    def import_exposure(self):
        obj = self.alert['schedule']
        self.is_portfolio = obj['isPortfolio']
        data = {'exposures': [{'portfolio': self.is_portfolio, 'id': obj['id']}]}
        ret = self.race_server.import_exposure(data)
        return ret

    def compute(self):
        obj = self.alert['analysis']
        a_date = obj['AnalysisDateBegin']
        data = {
            'damageFunctionId': obj['DamageFunctionID'],
            'portfolio': self.is_portfolio,
            'analysisList': [{
                'date': {
                    'day': a_date['Day'],
                    'month': a_date['Month'],
                    'year': a_date['Year']
                }
            }]
        }
        event_info = self.get_event_info()
        data['events'] = [event_info]
        ret = self.race_server.run_analysis(data)
        return ret

    def get_event_info(self):
        obj = self.alert['analysis']['Events'][0]
        peril, subperil = obj['peril'].split('-')
        return {
            'id': obj['EventID'],
            'name': obj['EventName'],
            'sev_id': obj['SeverityModelID'],
            'delta': {
                'lon': obj['deltaLon'],
                'lat': obj['deltaLat']
            },
            'type': obj['eventType'],
            'peril': peril,
            'subPeril': subperil,
            'source': obj['source']
        }
        return obj

    def get_summary(self):
        data = {'portfolio': self.is_portfolio, 'analysisCount': 1}
        data['vars'] = self.alert['summaryMeasures']
        ret = self.race_server.get_summary(data)
        return ret

    def get_topn(self):
        data = {
            'topnBy': 'assets',
            'analysisCount': 1,
            'sortMeasure': self.alert['topNprimary'],
            'additionalSortMeasure': self.alert['topNsecondary'],
            'attributes': self.alert['topNadditional'],
            'count': self.alert['topNcount'],
            'resultName': 'Topn Data'
        }
        ret = self.race_server.get_topn(data)

        if self.is_portfolio:
            data.update({'topnBy': 'contracts', 'resultName': 'Topn Contract Data'})
            ret = self.race_server.get_topn(data)
        return ret

    def get_step_files(self, step_name):
        this_step = {'Step Name': step_name}
        step_name = step_name.replace(' ', '_')
        step_name = step_name.lower()
        this_step['File Prefix'] = step_name
        self.steps.append(this_step)
        return (f'{step_name}.json', f'{step_name}.ramp')

    def format_script_file(self, script_file):
        if not os.path.exists(script_file):
            return
        if self.clang_format is None:
            return
        subprocess.call([self.clang_format, '--style=file', '-i', '--verbose', script_file])

    def get_test_name(self):
        env = self.alert['SubDomainName']
        exp_type = 'p' if self.is_portfolio else 'a'
        audit_id = self.alert['schedule']['audit_id']
        schedule_id = self.alert['schedule']['id']
        event = self.alert['analysis']['Events'][0]
        event_id = event['EventID']
        sev_id = event['SeverityModelID']
        return f'alert_{env}_{exp_type}_{audit_id}_{schedule_id}_{event_id}_{sev_id}'

    def write_input_json(self, input_json_file):
        contents = {
            'Environment': self.alert['SubDomainName'].capitalize(),
            'Description': self.alert['schedule']['importsetname'],
            'Test Name': self.get_test_name(),
            'Type': 'P' if self.is_portfolio else 'A'
        }
        this_steps = []
        for idx, step in enumerate(self.steps):
            this_step = {'Step Name': step['Step Name'], 'Id': idx, 'Script File': f"{step['File Prefix']}.ramp"}
            json_file = os.path.join(os.path.dirname(input_json_file), f"{step['File Prefix']}.json")
            if os.path.exists(json_file):
                this_step['Input File'] = os.path.basename('json_file')
            if idx != 0:
                this_step['Depends On'] = [idx - 1]
            this_steps.append(this_step)
        contents['Steps'] = this_steps

        with open(input_json_file, 'w') as fout:
            fout.write(json.dumps(contents, indent=4))

    def save(self, test_name):
        self.steps = []
        test_dir = os.path.join(self.race_root, 'Tools/TestCases', test_name)
        os.makedirs(test_dir, exist_ok=True)
        commands = self.race_server.get_command_list()
        for cmd in commands:
            json_file, script_file = self.get_step_files(cmd.pop('Name'))
            script_field = cmd.pop('Script')
            for cn in ['CommandID', 'User', 'From', 'Command']:
                cmd.pop(cn)

            with open(os.path.join(test_dir, script_file), 'w') as fout:
                fout.write(script_field)
            self.format_script_file(os.path.join(test_dir, script_file))

            if not cmd:
                continue
            with open(os.path.join(test_dir, json_file), 'w') as fout:
                fout.write(json.dumps(cmd, indent=4))
        self.write_input_json(os.path.join(test_dir, 'input.json'))

    def __exit__(self, type, value, traceback):
        pass
