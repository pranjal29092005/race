import os, sys, argparse, json
from pathlib import Path
import click, uuid
import er_aws
from icecream import ic

uuid_key = 'c9590987-12b9-4c6a-a3d7-48524664db7a'

parser = argparse.ArgumentParser(prog='create-shape-spider-test')
parser.add_argument('--results-key', '-r', required=True, dest='results_key', type=str)
parser.add_argument('-x', dest='external_test_dir', action='store_true')
parser.add_argument('--name', '-n', dest='name', type=str, required=True)
parser.add_argument('--ignore-results-key', '-ir', dest='ignore_results_key', action='store_true')
parser.add_argument('--with-heatmap', '-m', dest='with_heatmap', action='store_true')


class CreateShapeSpider:
    def __init__(self, **kwargs):
        p = Path(__file__)
        self.test_dir = p.parent.parent.joinpath('Tools', 'TestCases')
        if kwargs.get('external_test_dir', False):
            self.test_dir = Path(os.environ.get('RACE_TESTS_DIR'))
        self.rkey = kwargs.get('rkey')
        self.name = kwargs.get('name')
        self.with_heatmap = kwargs.get('with_heatmap', False)
        self.ignore_rkey_in_output = kwargs.get('ignore_results_key', False)

    def create_test_json(self, conf):
        if self.ignore_rkey_in_output:
            for key in ['resultsKey', 'Results Key', 'ResultsKey']:
                conf.pop(key, None)
            conf['Results Key'] = uuid_key
        if self.with_heatmap:
            conf['s3Bucket'] = 'er-race-results'
            conf['s3Folder'] = f'shape_spider_heatmap/{uuid_key}'
        else:
            conf.pop('s3Bucket', None)
            conf.pop('s3Folder', None)
        out = {
            'Script':
                "string errorMessage; uint index= 0;if (!AsyncSpiderAnalysis('AsyncSpiderAnalysis', errorMessage)) {SendErrorWithCode(errorMessage); }",
            'AsyncSpiderAnalysis':
                conf
        }
        ic(self.test_dir)
        with self.test_dir.joinpath(f'{self.name}.json').open('w') as f:
            json.dump(out, f, indent=4)

    def execute(self):
        session = er_aws.AwsSession(os.environ['S3_ACCESS_KEY'], os.environ['S3_SECRET_KEY'])
        key = f'shape_spider/{self.rkey}/shape_spider.json'
        conf = session.read_json('er-race-results', key)
        if conf is None:
            click.secho('Unable to create shape_spider test', fg='red', bold=True)
            return
        self.create_test_json(conf)


if __name__ == '__main__':
    argv = parser.parse_args()

    engine = CreateShapeSpider(
        name=argv.name,
        rkey=argv.results_key,
        external_test_dir=argv.external_test_dir,
        ignore_results_key=argv.ignore_results_key,
        with_heatmap=argv.with_heatmap
    )
    engine.execute()
