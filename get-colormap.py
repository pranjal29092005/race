import os, sys, json
import pandas as pd
import argparse
import er_db
from icecream import ic
import click
from psycopg2.extras import RealDictCursor

parser = argparse.ArgumentParser(prog='get-colormap')
parser.add_argument('--env', dest='env', required=True, type=str, choices=['prod', 'alpha', 'integration'])
parser.add_argument('--id', dest='event_id', required=True, type=int)
argv = parser.parse_args()


def _modify(jitem):
    ret = {}
    for k, v in jitem.items():
        if k in ['r', 'g', 'b']:
            ret[k] = int(v)
        elif k in ['minValue', 'maxValue']:
            ret[k] = float(v)
        else:
            ret[k] = v
    return ret


with er_db.get_db_conn(argv.env) as conn:
    try:
        query = f'select "EVENT_SEV_MODEL_ID" from race."B_EVENT_SEVERITY" where "EVENT_ID"={argv.event_id}'
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        row = cursor.fetchone()
        sid = row['EVENT_SEV_MODEL_ID']
        query = f'select color_map from get_event_details_4_0({sid}, {argv.event_id})'
        cursor.execute(query)
        res = cursor.fetchone()
        cmap = res['color_map']
        items = []
        for item in cmap:
            items.append(
                {
                    'r': int(item['r']),
                    'g': int(item['g']),
                    'b': int(item['b']),
                    'minValue': float(item['minValue']),
                    'maxValue': float(item['maxValue']),
                    'description': item['description']
                }
            )
        df = pd.DataFrame(items).sort_values(by='minValue')
        for _, row in df.iterrows():
            click.secho(
                f"min: {row['minValue']}, max: {row['maxValue']}, desc: {row['description']}",
                fg='black',
                bg=(row['r'], row['g'], row['b'])
            )
    except Exception as e:
        print(e)
