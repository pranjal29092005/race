import os, sys, json
import er_db, er_aws, er_utils
import argparse, hashlib
import glob

parser = argparse.ArgumentParser(prog="get-bin-sources")
parser.add_argument('-e', '--env', dest='env', type=str, required=True, choices=['uat', 'alpha', 'integration', 'prod'])
parser.add_argument('-a', '--audit-id', dest='audit_id', type=int, required=False)
parser.add_argument('-s', '--schedule-id', dest='schedule_id', type=int, required=False)
parser.add_argument('-i', '--id', dest='exposure_id', type=int, required=False)
parser.add_argument('-p', '--portfolio', dest='portfolio', action='store_true', required=False)
argv = parser.parse_args()

if not argv.schedule_id:
    argv.schedule_id = 0


def get_audit_and_schedule_id_from_exp_id(argv):
    exp_id = argv.exposure_id
    res = None
    with er_db.get_db_conn(argv.env.capitalize()) as conn:
        res = er_db.get_audit_id_v2(conn, exp_id, argv.portfolio)
    if res and res[0]:
        if argv.portfolio:
            return (int(res[0]), 0)
        else:
            return (int(res[0]), exp_id)


def get_exp_id_from_audit_id(argv):
    query = ''
    if argv.portfolio:
        query = f'select portfolio_id from race.m_portfolio where audit_id={argv.audit_id}'
    else:
        query = f'select "ID" from race."M_ASSET_SCHEDULE" where "AUDIT_ID"={argv.audit_id}'
    with er_db.get_db_conn(argv.env.capitalize()) as conn:
        res = er_db.exec_stmt(conn, query)
    return res[0]


def create_input_json(dest_dir, aid, sid):
    aid_sid = f'{aid}_{sid}'
    src_dir = os.path.join(dest_dir, aid_sid)
    txt_files = glob.glob(os.path.join(src_dir, '*.txt'))
    input_json = os.path.join(src_dir, 'input.json')
    config = {
        'Exposure': {
            'Audit Id': aid,
            'Is Portfolio': argv.portfolio,
            'Schedule Id': sid
        },
        'Config': {
            'Output Directory': os.path.join(src_dir, 'output')
        },
        'Valuation Binaries':
            {
                'F Asset':
                    {
                        'Input File': os.path.join(src_dir, f'f_asset_{aid_sid}.txt'),
                        'Order File': os.path.join(src_dir, 'f_asset.txt')
                    },
                'M Asset':
                    {
                        'Input File': os.path.join(src_dir, f'm_asset_{aid_sid}.txt'),
                        'Order File': os.path.join(src_dir, 'm_asset.txt')
                    },
            }
    }
    if argv.portfolio:
        pc_config = {
            'Coverage Binaries':
                {
                    'Column Order File': os.path.join(src_dir, 'f_coverage.txt'),
                    'Input File': os.path.join(src_dir, f'data_f_coverage_{aid_sid}.txt')
                },
            'Contract Binaries':
                {
                    'Input File': os.path.join(src_dir, f'data_f_contract_{aid_sid}.txt'),
                    'Mapped Columns File': os.path.join(src_dir, 'f_contract.txt')
                },
            'Asset Schedule Binaries': {
                'Input File': os.path.join(src_dir, f'data_m_asset_schedule_{aid_sid}.txt')
            }
        }
        layer_files = set(filter(lambda s: '_layer_' in s, txt_files))
        if len(layer_files):
            pc_config.update(
                {
                    'Layer Binaries':
                        {
                            'Input File': os.path.join(src_dir, f'data_f_layer_{aid_sid}.txt'),
                            'Mapped Columns File': os.path.join(src_dir, 'f_layer.txt')
                        }
                }
            )
        reins_files = set(filter(lambda s: 'r_info' in s, txt_files))
        if len(reins_files):
            pc_config.update(
                {
                    'Reinsurance Binaries':
                        {
                            'Info':
                                {
                                    'Input File': os.path.join(src_dir, f'data_r_info_{aid_sid}.txt'),
                                    'Order File': os.path.join(src_dir, 'r_info.txt')
                                },
                            'Scope':
                                {
                                    'Input File': os.path.join(src_dir, f'data_r_scope_{aid_sid}.txt'),
                                    'Order File': os.path.join(src_dir, 'r_scope.txt')
                                }
                        }
                }
            )
        config.update(pc_config)
    with open(input_json, 'w') as f:
        json.dump(config, f, indent=4)
    print(f'Input json {input_json} created')


if argv.audit_id:
    argv.exposure_id = get_exp_id_from_audit_id(argv)

audit_id, schedule_id = get_audit_and_schedule_id_from_exp_id(argv)
argv.audit_id = audit_id
argv.schedule_id = schedule_id

if argv.audit_id is None:
    print('Audit id not provided')
    sys.exit(-1)

this_dir = os.path.dirname(os.path.abspath(__file__))
config_file = os.path.join(os.path.dirname(this_dir), 'Aws.cfg')
with open(config_file, 'r') as f:
    config = json.load(f)[argv.env.title()]

bucket = config['ASSET_S3_BUCKET']
aid_sid = f'{argv.audit_id}_{argv.schedule_id}'
hashed_key = hashlib.md5(aid_sid.encode()).hexdigest().upper()
key = f'{hashed_key}/{aid_sid}_src.7z'

session = er_aws.AwsSession(os.environ['S3_ACCESS_KEY'], os.environ['S3_SECRET_KEY'])
out = f'{aid_sid}_src.7z'
print(out)
session.download_file(bucket, key, out=out)

dest_dir = os.environ.get('RACE_BIN_SOURCES_DIR')
if dest_dir is None:
    sys.exit(0)

er_utils.extract_archive(out, os.path.join(dest_dir, f'{aid_sid}'))
os.remove(out)

create_input_json(dest_dir, argv.audit_id, argv.schedule_id)
