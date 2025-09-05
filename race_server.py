import random
import os, sys, json
import zmq, uuid
from datetime import datetime
import time
from icecream import ic
from jinja2 import Environment as j2env, FileSystemLoader as j2fsl

class Endpoint(object):
    def __init__(self):
        self.socket = None
        self.address = None

    def set_socket(self, socket):
        self.socket = socket
        # print(f"Socket set to {self.socket}")

    def set_address(self, addr):
        self.address = addr

    def connect(self):
        self.socket.connect(self.address)
        # print(f"Connected to {self.address}")

class RaceServer(object):
    def __init__(self, **kwargs):
        self.context = zmq.Context()

        self.dry_run = kwargs.get('dry_run', False)
        self.user = kwargs.get('user', 'pythonuser@localhost')
        self.ip = kwargs.get('ip', '127.0.0.1')
        rcv_timeout = kwargs.get('rcv_timeout')
        if rcv_timeout is not None:
            self.context.setsockopt(zmq.RCVTIMEO, rcv_timeout)
        else:
            self.context.setsockopt(zmq.RCVTIMEO, -1)

        self.context.setsockopt(zmq.SNDTIMEO, 5000)
        self.race = Endpoint()
        self.router = Endpoint()
        self.results = Endpoint()
        self.env = self.setup_template_env()
        self.commands = []
        self.race_results = []

        self.init()

    def get_command_list(self):
        return self.commands

    def init(self):
        if self.dry_run:
            return
        print(f"Connecting to router at tcp://{self.ip}:5556")
        self.router.set_address(f'tcp://{self.ip}:5556')
        # print(f"connected...")
        self.router.set_socket(self.create_request_socket())
        # print(f"setting the socket.")
        self.router.connect()
        # print("connected to the router.")

    def create_request_socket(self):
        sock = self.context.socket(zmq.REQ)
        return sock

    def create_sub_socket(self):
        sock = self.context.socket(zmq.SUB)
        sock.setsockopt(zmq.SUBSCRIBE, b'')
        return sock

    def create_command(self, **kwargs):
        cmd = kwargs.get('cmd')
        if cmd is None:
            cmd = 'Execute'
        return {'Command': cmd, 'User': self.user, 'CommandID': str(uuid.uuid4()), 'From': 'PyScript'}

    def ping_router(self):
        cmd = self.create_command(cmd='Ping')
        # print(f"Pinging router with command: {cmd}")
        resp = self.send_router(cmd)
        # print(resp)

    def ping_race(self):
        cmd = self.create_command(cmd='Ping')
        self.race.socket.send_json(cmd)
        resp = self.race.socket.recv_json()
        # print(resp)

    def send_router(self, obj):
        try:
            self.router.socket.send_json(obj)
            # print(self.router.socket.recv_json())
            ack = self.router.socket.recv_json()
            # print(f"Received ack from router: {ack}")
            return ack
        except Exception as e:
            print(f'Exception .....: {e}')
            raise (e)

    def send_race(self, obj):
        if self.dry_run:
            self.commands.append(obj)
        else:
            self.race.socket.send_json(obj)
            ack = self.race.socket.recv_json()
            # print(f"Received ack from race: {ack}")
            result_ack = self.results.socket.recv_json()
            # print(f"Received ack from results: {result_ack}")
            return result_ack

    def recv_race(self):
        return self.results.socket.recv_json()

    def get_spider_status(self, geo_spider=False):
        cmd = self.create_command(cmd='GetGeoSpiderStatus' if geo_spider else 'GetSpiderStatus')
        return self.send_router(cmd)

    def get_pi_status(self):
        cmd = self.create_command(cmd='GetPortfolioImpactStatus')
        return self.send_router(cmd)

    def terminate_spider(self, geo_spider):
        if geo_spider is True:
            return self.terminate('TerminateGeoSpider')
        return self.terminate('TerminateProfile')

    def terminate_session(self):
        cmd = self.create_command()
        cmd['Script'] = 'prepareTerminatSession()'
        self.race.socket.send_json(cmd)
        ack = self.race.socket.recv_json()
        time.sleep(1)
        return self.terminate('TerminateSession')

    def terminate_portfolio_impact(self):
        return self.terminate('TerminatePortfolioImpact')

    def terminate(self, cmd_name):
        cmd = self.create_command(cmd=cmd_name)
        return self.send_router(cmd)

    def create_session(self, **kwargs):
        if self.dry_run:
            return
        # print("Creating session...")
        # self.ip = "10.0.8.56"
        user_id = kwargs.get('user_id')
        if user_id is None:
            user_id = 0
        else:
            self.user_id = f'user_{user_id}'
            print(f"User ID set to {self.user_id}")
        # print(f"user ip: {self.ip}")
        # print("cmd createSession...")
        obj = self.create_command(cmd='CreateSession')
        obj['UserId'] = user_id
        # print(f"cmd createSession... {obj}")
        ret = self.send_router(obj)
        # print(f"sending to router... {ret}")
        self.race.set_socket(self.create_request_socket())
        # print(f"request socket created... ")
        self.race.set_address(ret['ServerCommandAddr'])
        # print(f"request socket address set to...")
        self.race.connect()
        # print("race socket connected...")
        self.results.set_socket(self.create_sub_socket())
        self.results.set_address(ret['ServerResultsAddr'])
        self.results.connect()

    def render(self, template_name, data):
        tmpl = self.env.get_template(f'{template_name}.j2')
        # print(f"Rendering template {tmpl} with data: {data}")
        return tmpl.render(data).replace('\n', '')

    def run_analysis(self, data):
        cmd = self.create_command()
        # with open('ting.out', 'w') as f:
        # f.write(self.render('analysis_json', data))

        cmd.update(json.loads(self.render('analysis_json', data)))
        cmd['Script'] = self.render('analysis_script', data)
        cmd['Name'] = 'Analysis'
        # print(f"command for run analysis: {cmd}")
        return self.send_race(cmd)

    def get_summary(self, data):
        cmd = self.create_command()
        cmd.update(json.loads(self.render('summary_json', data)))
        cmd['Name'] = 'Summary'
        cmd['Script'] = self.render('summary_script', data)
        # print(f"command for summary: {cmd}")
        return self.send_race(cmd)

    def get_exposure_summary(self,additional_measures):
        cmd = self.create_command()
        cmd['Name'] = 'Exposure Summary'
        cmd["GetSummary"]={"additionalMeasures":additional_measures,"showXofY":False}
        # print(self.render('summary_script_old', cmd['GetSummary']))
        cmd['Script'] = "array<uint> indices;        indices.insertLast(0);                string errorMessage;        if (!GetSummaryV2('GetSummary', @indices, errorMessage)) {            Send('Error!', errorMessage); return;        }"
        # print(f"command for exposure summary: {cmd}")
        return self.send_race(cmd)

    def get_topn(self, data):
        cmd = self.create_command()
        out = self.render('topn_json', data)
        cmd.update(json.loads(self.render('topn_json', data)))
        cmd['Script'] = self.render('topn_script', data)
        cmd['Name'] = 'Topn Assets' if data['topnBy'] == 'assets' else 'Topn Contracts'
        print(f"command for topn: {cmd}")
        return self.send_race(cmd)

    def import_exposure(self, data):
        cmd = self.create_command()
        cmd['Script'] = self.render('import', data)
        cmd['Name'] = 'Import Exposure'
        return self.send_race(cmd)

    def contract_summary(self, portfolio_id):
        cmd = self.create_command()
        data = {'sch_id': portfolio_id}
        cmd.update(json.loads(self.render('contract_summary_json', data)))
        cmd['Script'] = self.render('contract_summary_script', data)
        cmd['Name'] = 'Contract Summary'
        return self.send_race(cmd)

    def import_sov(self, id):
        data = {'exposures': [{'portfolio': False, 'id': id}]}
        return self.import_exposure(data)

    def import_portfolio(self, id):
        data = {'exposures': [{'portfolio': True, 'id': id}]}
        return self.import_exposure(data)
    
    def load_portfolio(self,id):
        cmd = self.create_command()
        # cmd['script'] = "bool loadStatus = ImportContractPortfolio(43370,0); Send('ExposureLoadStatus', loadStatus);"
        data = id
        cmd['Script'] = self.render('import', data)
        # print(f"command for portfolio import: {cmd}")
        cmd['Name'] = 'Import Portfolio'
        return self.send_race(cmd)
    
    def pick_intensity(self, data):
        cmd = self.create_command()
        cmd.update(json.loads(self.render('intensity_picker_json', data)))
        cmd['Script'] = self.render('intensity_picker_script', data)
        cmd['Name'] = 'Intensity Picker'
        return self.send_race(cmd)

    def setup_template_env(self):
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        template_path = os.path.join(cur_dir, 'Templates')
        env = j2env(loader=j2fsl(template_path), trim_blocks=True)
        env.globals.update(zip=zip)
        return env

    def check_resource_usage(self):
        cmd = self.create_command()
        cmd['Command'] = 'CheckResourceUsage'
        return self.send_router(cmd)

    def get_topn_assets(self, data):
        """Get top N assets based on specified criteria"""
        cmd = self.create_command()
        try:
            # Render the JSON template first
            json_output = self.render('topn_assets_json', data)
            cmd.update(json.loads(json_output))
            # print(f"Command for top N assets: {cmd}")
            # Render the script template
            cmd['Script'] = "array<uint> indices;        indices.insertLast(0);                string errorMessage;        if (!TopnAssetsV2('TopNData', @indices, errorMessage)) {            SendError(errorMessage); return;        }"
            cmd['Name'] = 'Top N Assets'
            return self.send_race(cmd)
        except Exception as e:
            print(f"Error in get_topn_assets: {str(e)}")
            print(f"Data being rendered: {data}")
            raise e
