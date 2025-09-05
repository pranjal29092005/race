import os, sys, json, uuid
import er_db
import pandas as pd
import click
from icecream import ic
from jinja2 import Environment as j2env, FileSystemLoader as j2fsl


class NoPerilException(Exception):
    pass


class InvalidPerilException(Exception):
    pass


class GeoSpider:
    def __init__(self):
        super().__init__()
        self.peril = None
        self.results_key = None
        self.resolutions = []
        self.exp_filters = []
        self.exposure_id = None
        self.event_id = None
        self.threshold = 10000
        self.measure = None
        this_dir = os.path.dirname(os.path.abspath(__file__))
        template_path = os.path.normpath(os.path.join(this_dir, 'Templates'))
        self.env = j2env(loader=j2fsl(template_path), trim_blocks=True)
        self.script = '''
string errorMessage;
if (!RunGeoSpider("GeoSpider", 0, errorMessage)){
    Send("Error!", errorMessage);
}
        '''.replace('\n', ' ')

    def add_filter(self, attr, op, val, andor='AND'):
        tmpl = self.env.get_template('filter.j2').render(
            {
                'attribute': attr,
                'operator': op,
                'value': val,
                'andor': andor
            }
        )
        self.exp_filters.append(json.loads(tmpl))

    def populate_perils(self, db_conn):
        if self.peril is None:
            raise NoPerilException()
        df = er_db.get_peril_table(db_conn)
        df = df[df.sub_peril_code == self.peril]
        if len(df) != 1:
            raise InvalidPerilException()
        return (df.iloc[0]['peril_code'], self.peril)

    def to_json(self, db_conn):
        peril, sub_peril = self.populate_perils(db_conn)
        self.add_filter('Cause Of Loss', 'EQ', sub_peril)
        if self.results_key is None:
            self.results_key = str(uuid.uuid4())
        data = {
            'exp_id': self.exposure_id,
            'measure': self.measure,
            'attributes': ["Country", "State"],
            'results_key': self.results_key,
            'threshold': self.threshold,
            'measures': ["TIV", "#Assets", "#Contracts"]
        }
        tmpl = self.env.get_template('geospider.j2').render(data).replace('\n', '')
        out = json.loads(tmpl)
        if (self.exp_filters):
            out['ExposureFilterSets'] = {'AssetModel': 'ERBASICS', 'FilterList': self.exp_filters}
        out = {'Script': self.script, 'GeoSpider': out}
        ic(out)


if __name__ == '__main__':
    this_dir = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.normpath(os.path.join(this_dir, 'Templates'))
    env = j2env(loader=j2fsl(template_path), trim_blocks=True)
    val = env.get_template('filter.j2').render(
        {
            'andor': 'AND',
            'attribute': 'Cause Of Loss',
            'operator': 'EQ',
            'value': 'EQSH',
        }
    )
    val = json.loads(val)
    ic(val)
