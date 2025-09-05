import os, sys, json
import argparse
import subprocess
from icecream import ic
import er_aws

parser = argparse.ArgumentParser(prog='get-ss-conf')
parser.add_argument('--results-key', '-r', dest='results_key', required=True, type=str)

argv = parser.parse_args()

bucket = 'er-race-results'
key = f'shape_spider/{argv.results_key}/server_info.json'

akey = os.environ.get('S3_ACCESS_KEY')
skey = os.environ.get('S3_SECRET_KEY')
session = er_aws.AwsSession(akey, skey)
conf = session.read_json(bucket, key)
if conf is not None:
    ic(conf)
