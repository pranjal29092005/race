import os, sys, subprocess, json
import er_db, er_aws
import click
from icecream import ic
import argparse


class BinCreate():
    def __init__(self, **kwargs):
        self.portfolio = False
        self.env = None
        self.exp_id = None
        self.audit_id = None
        self.src_dir = os.environ.get('RACE_BIN_SOURCES_DIR')
        self.exp_name = None
        self.types = kwargs.get('types')
        self.get_bin_create_exe()

        self.portfolio = kwargs.get('portfolio', False)
        self.env = kwargs.get('env')

        if self.env is None:
            self.error('No environment specified')

        self.audit_id = kwargs.get('audit_id')
        self.exp_id = kwargs.get('exp_id')
        self.upload_binaries = kwargs.get('upload_binaries', False)
        self.reinsurance_binaries_only = kwargs.get('reinsurance_binaries_only', False)

        with er_db.get_db_conn(self.env) as conn:
            if self.audit_id is not None:
                self.exp_id = er_db.get_exposure_id(conn, self.audit_id, self.portfolio)
            if self.exp_id is None:
                self.error('No exposure id provided')
            if self.audit_id is None:
                self.audit_id = er_db.get_audit_id_v2(conn, self.exp_id, self.portfolio)[0]
            self.exp_name = er_db.get_exp_name(conn, self.exp_id, self.portfolio)

    def get_bin_create_exe(self):
        x = subprocess.check_output(['git', 'rev-parse', '--show-toplevel']).decode('utf-8').split('\n')[0]
        self.exe = os.path.join(x, 'build/Tools/BinaryCreator/Cli/BinaryCreatorCli')
        self.cwd = os.path.join(x, 'build')

    def error(self, msg):
        click.secho(msg, fg='red')
        sys.exit(-1)

    def setup_env(self):
        conf = er_db.conf
        rv = er_db.get_db_info(self.env)
        for k, v in rv.items():
            os.environ[k] = v
        for k in ['PG_PW', 'PG_PORT', 'AWS_DEFAULT_REGION', 'PG_USER']:
            os.environ[k] = conf[k]

    def get_command(self):
        sid = self.exp_id
        if self.portfolio:
            sid = 0
        input_json = os.path.join(self.src_dir, f'{self.audit_id}_{sid}', 'input.json')
        if not os.path.exists(input_json):
            self.error('No input json exists')
        cmd = [self.exe, '--input', input_json]
        if self.reinsurance_binaries_only:
            cmd.append('--reinsurance-binaries-only')
        if not self.upload_binaries:
            cmd.append('--no-upload')
        if self.types:
            for item in enumerate(self.types):
                cmd.extend(['-t', item[1]])
        return cmd

    def execute(self):
        cmd = self.get_command()
        self.setup_env()
        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as handle:
            click.secho('Creating binaries for ...', underline=True, bold=True, fg='magenta', italic=True)
            click.secho('Audit Id: ', fg='yellow', nl=False)
            click.secho(f'{self.audit_id}', fg='blue')
            click.secho('Exposure Id: ', fg='yellow', nl=False)
            click.secho(f'{self.exp_id}', fg='blue')
            click.secho('Name: ', fg='yellow', nl=False)
            click.secho(f'{self.exp_name}', fg='blue')
            click.secho('Command: ', fg='yellow', nl=False)
            click.secho(' '.join(cmd), fg='green', bold=True)
            out, err = handle.communicate()
            if (err):
                self.error(err.decode('utf-8').strip())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='bin-create')
    parser.add_argument('--env', '-e', dest='env', required=True, type=str)
    parser.add_argument('--id', '-i', dest='exp_id', required=False, type=int)
    parser.add_argument('--audit-id', '-a', dest='audit_id', required=False, type=int)
    parser.add_argument('--portfolio', '-p', dest='portfolio', action='store_true')
    parser.add_argument('--reins', dest='reinsurance_binaries_only', action='store_true')
    parser.add_argument('--upload', dest='upload_binaries', action='store_true')
    parser.add_argument('--from-parquet', '-fp', action='store_true', dest='from_parquet')
    parser.add_argument('--type', '-t', nargs='*', required=False, dest='types')

    args = parser.parse_args()
    if args.from_parquet:
        os.environ['RACE_BC_CREATE_BINARIES_FROM_PARQUET'] = 'on'
        click.secho('Creating binaries from source parquet files', bg='magenta', bold=True, fg='white')

    bc = BinCreate(
        env=args.env,
        exp_id=args.exp_id,
        types=args.types,
        audit_id=args.audit_id,
        portfolio=args.portfolio,
        reinsurance_binaries_only=args.reinsurance_binaries_only,
        upload_binaries=args.upload_binaries
    )
    bc.execute()
