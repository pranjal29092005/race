import os, sys, json
import psycopg2 as pg
import argparse
import er_db
from event_set import EventSet, EventSetException


class NoResult(Exception):
    pass


parser = argparse.ArgumentParser(prog='Create event set')
parser.add_argument('--env',
                    dest='env',
                    help='Db environment to create the event set',
                    choices=['uat', 'prod'],
                    default='uat')
parser.add_argument('--config',
                    dest='config',
                    help='Config file',
                    required=True)
argv = parser.parse_args()

if not os.path.exists(argv.config):
    print('Config file does not exist')
    sys.exit(-1)

with open(argv.config, 'r') as f:
    conf = json.load(f)


def get_xdef_id(conn, xdef):
    pass


def get_units_id(conn, units):
    pass


def get_source_id(conn, source):
    stmt = f'select "ID" from race."M_SOURCE" where "CODE"=\'{source}\''
    ret = None
    try:
        row = er_db.fetch_one(conn, stmt)
        ret = row['ID']
    except exceptions.DbNoResultException:
        raise NoResult
    return ret


def get_sev_model_id(conn, sev_model):
    pass


def get_event_set_id(conn, name):
    pass


if __name__ == '__main__':
    # new_event_set = EventSet(**conf)
    # event_set_id, sev_model_id = new_event_set.create()

    with er_db.get_db_conn(argv.env) as conn:
        code = 'IHME'
        stmt = f"""select "ID" from race."M_SOURCE" where "CODE"='{code}'"""
        res = er_db.fetch_one(conn, stmt)
        print(res['ID'])
    #     # try:
    #     #     event_set_id = get_event_set_id(conn, conf['name'])
    #     #     if event_set_id:
    #     #         print(f'Event set already exists: {event_set_id}')
    #     #         sys.exit(0)
    #     #     source_id = get_source_id(conn, conf['source'])
    #     #     xdef_id = get_xdef_id(conn, conf['source'])
    #     #     units_id = get_units_id(conn, conf['units'])
    #     #     sev_model_id = get_sev_model_id(conn, conf['sev_model'])
    #     # except NoResult:
    #     #     print("No results")
    #     #     sys.exit(-1)
