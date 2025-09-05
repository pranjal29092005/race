import os, sys, json
import argparse
import er_db, er_utils
from icecream import ic
import click
import pandas as pd


class GetProgram(object):
    def __init__(self, **kwargs):
        super(GetProgram, self).__init__()
        self.env = kwargs.get('env')
        self.id = kwargs.get('id')

    def header(self, text):
        click.secho(f'\n\n{text}', fg='green', bold=True)

    def get_contract_info(self, conn):
        query = f'select * from "GetPBContractData"({self.id})'
        out = er_db.exec_stmt(conn, query, fetch_all=True)
        df = pd.DataFrame(out)
        self.header('Contract')
        er_utils.tabulate_df(df)

    def get_coverage_info(self, conn):
        query = f'select * from "GetPBCoverageData"({self.id})'
        out = er_db.exec_stmt(conn, query, fetch_all=True)
        df = pd.DataFrame(out)
        self.header('Coverages')
        er_utils.tabulate_df(df)

    def get_layer_info(self, conn):
        query = f'select * from "GetPBLayerData"({self.id})'
        out = er_db.exec_stmt(conn, query, fetch_all=True)
        df = pd.DataFrame(out)
        self.header('Layers')
        er_utils.tabulate_df(df)

    def execute(self):
        with er_db.get_db_conn(argv.env) as conn:
            self.get_contract_info(conn)
            self.get_coverage_info(conn)
            self.get_layer_info(conn)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='get-program')
    parser.add_argument('--env', dest='env', choices=['prod', 'integration', 'alpha'], required=True)
    parser.add_argument('--id', type=int, dest='id', required=True)
    argv = parser.parse_args()
    prog = GetProgram(env=argv.env, id=argv.id)
    prog.execute()
