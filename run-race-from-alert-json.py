import os, sys, json, argparse
from jinja2 import Environment as j2env, FileSystemLoader as j2fsl
from datetime import datetime
import pandas as pd
import tempfile
import zmq, uuid
from race_server import RaceServer as rs
from alert_json import AlertJson as aj, AlertJsonException as aje

parser = argparse.ArgumentParser(prog='race-from-alert-json')
parser.add_argument('--alert-json', type=str, required=True, dest='alert_json')

argv = parser.parse_args()


def read_alert_json():
    if not os.path.exists(argv.alert_json):
        print(f'Alert json file {alert_json} does not exist')
        sys.exit(-1)
    alert = None
    with open(argv.alert_json, 'r') as f:
        alert = json.load(f)
    return alert


if __name__ == '__main__':
    dry_run = False
    if argv.save_test:
        dry_run = True
    with aj(argv.alert_json, dry_run=dry_run) as alert_json:
        ret = alert_json.import_exposure()
        ret = alert_json.compute()
        ret = alert_json.get_summary()
        ret = alert_json.get_topn()
        alert_json.save(argv.save_test)
