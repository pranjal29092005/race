import er_utils, er_db
import geopandas as gpd
import shapely
import pandas as pd
import json, os, argparse, tempfile
import subprocess, shutil

rscript = shutil.which('Rscript')

parser = argparse.ArgumentParser(prog='bbox check')
parser.add_argument('--env',
                    dest='env',
                    choices=['uat', 'prod'],
                    default='prod')
parser.add_argument('--event-id', dest='event_id', required=True, type=int)
parser.add_argument('--sev-id', dest='sev_id', required=True, type=int)
parser.add_argument('--exp-id', dest='exp_id', required=True, type=int)
parser.add_argument('--portfolio', dest='portfolio', action='store_true')
argv = parser.parse_args()

with er_db.get_db_conn('prod') as conn:
    if argv.portfolio:
        stmt = f'select "UL_LON","UL_LAT","LR_LON","LR_LAT" from race.m_portfolio where portfolio_id={argv.exp_id}'
    else:
        stmt = f'select "UL_LON","UL_LAT","LR_LON","LR_LAT" from race."M_ASSET_SCHEDULE" where "ID"={argv.exp_id}'
    res = er_db.fetch_one(conn, stmt)
    bbox = shapely.geometry.box(res['UL_LON'], res['UL_LAT'], res['LR_LON'],
                                res['LR_LAT'])
    exp = gpd.GeoDataFrame(geometry=gpd.GeoSeries([bbox]))
    exp_json_file = os.path.join(tempfile.gettempdir(), f'{argv.exp_id}.json')
    event_json_file = os.path.join(tempfile.gettempdir(),
                                   f'{argv.event_id}_{argv.sev_id}.json')
    with open(exp_json_file, 'w') as f:
        obj = json.loads(gpd.GeoSeries([bbox]).to_json())
        f.write(json.dumps(obj, indent=4))
        print(f'Exp json written to {exp_json_file}')

    stmt = f'select * from "GetEventBondingBox"({argv.sev_id}, {argv.event_id})'
    res = er_db.fetch_one(conn, stmt)
    with open(event_json_file, 'w') as f:
        bbox = shapely.geometry.box(res['UL_LON'], res['UL_LAT'],
                                    res['LR_LON'], res['LR_LAT'])
        evt = gpd.GeoDataFrame(geometry=gpd.GeoSeries([bbox]))
        obj = json.loads(gpd.GeoSeries([bbox]).to_json())
        f.write(json.dumps(obj, indent=4))
        print(f'Event json written to {event_json_file}')

    out = gpd.overlay(exp, evt, how='intersection')
    print(out.empty)

    if os.path.exists(event_json_file) and os.path.exists(exp_json_file):
        out = os.path.join(os.getcwd(),
                           f'{argv.exp_id}_{argv.event_id}_{argv.sev_id}.png')
        subprocess.call(
            f'{rscript} exp_event.R --exp {exp_json_file} --evt {event_json_file} --out {out}'
            .split(),
            cwd=os.getcwd())
