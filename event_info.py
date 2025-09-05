import os, sys
import psycopg2, psycopg2.extras
import argparse, json
import er_db
import event_shape
from helpers_conf import *

parser = argparse.ArgumentParser(prog='get-event-info')
parser.add_argument('--env', dest='env', choices=keys, default='uat')
parser.add_argument('--event-id', dest='event_id', required=True, type=int)
parser.add_argument('--sev-id', dest='sev_id', required=True, type=int)
parser.add_argument('--thumbnail', dest='thumbnail', action='store_true', required=False)


def get_event_shape(conn, sev_id, event_id, id):
    stmt = f'select * from "GetEventSeverity"({sev_id}, {event_id})'
    event_details = er_db.fetch_one(conn, stmt)
    event_shape.get_event_shape(event_details, id, name=event_details['EVENT_NAME'])


def get_event_thumbnail(conn, sev_id, event_id):
    out_file = f'{event_id}_thumbnail.png'
    out = er_db.fetch_one(conn, f'select "SHAPE_THUMBNAIL" from "GetEventSeverity"({sev_id}, {event_id})')
    if out:
        with open(out_file, 'wb') as f:
            f.write(out['SHAPE_THUMBNAIL'])


if __name__ == '__main__':
    args = parser.parse_args()
    db_info = er_db.get_db_info(args.env)
    with er_db.get_db_conn(args.env) as conn:
        if args.thumbnail:
            get_event_thumbnail(conn, args.sev_id, args.event_id)
        else:
            get_event_shape(conn, args.sev_id, args.event_id, args.event_id)
