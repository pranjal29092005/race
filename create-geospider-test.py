import os, sys, argparse, json
from pathlib import Path
import click, uuid
import er_aws
from icecream import ic

parser = argparse.ArgumentParser(prog='create-geospider-test')
parser.add_argument('--results-key', '-r', required=True, dest='results_key', type=str)
parser.add_argument('-x', dest='external_test_dir', action='store_true')
parser.add_argument('--name', '-n', dest='name', type=str, required=True)
parser.add_argument('--ignore-results-key', '-ir', dest='ignore_results_key', action='store_true')

gs_key = "ca3dca33-cbbd-4fd7-8d35-05f383f10f80"


class CreateGeoSpider:
    def __init__(self, **kwargs):
        p = Path(__file__)
        self.test_dir = p.parent.parent.joinpath('Tools', 'TestCases')
        if kwargs.get('external_test_dir', False):
            self.test_dir = Path(os.environ.get('RACE_TESTS_DIR'))
        self.rkey = kwargs.get('rkey')
        self.name = kwargs.get('name')
        self.ignore_rkey_in_output = kwargs.get('ignore_results_key', False)

    def create_test_json(self, conf):
        conf.pop('resultsKey', None)
        if self.ignore_rkey_in_output:
            conf['AccumulateByAttribute']['Results Key'] = gs_key
        out = {
            'Script':
                "string errorMessage; uint index= 0;if (!RunGeoSpider('GeoSpider', index, errorMessage)) {Send('Error!', errorMessage); }",
            'GeoSpider':
                conf
        }
        ic(self.test_dir)
        with self.test_dir.joinpath(f'{self.name}.json').open('w') as f:
            json.dump(out, f, indent=4)

    def execute(self):
        session = er_aws.AwsSession(os.environ['S3_ACCESS_KEY'], os.environ['S3_SECRET_KEY'])
        key = f'geo-spider/{self.rkey}/geo_spider.json'
        conf = session.read_json('er-race-results', key)
        if conf is None:
            click.secho('Unable to create geospider test', fg='red', bold=True)
            return
        self.create_test_json(conf)


if __name__ == '__main__':
    argv = parser.parse_args()

    engine = CreateGeoSpider(
        name=argv.name,
        rkey=argv.results_key,
        external_test_dir=argv.external_test_dir,
        ignore_results_key=argv.ignore_results_key
    )
    engine.execute()
