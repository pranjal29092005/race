import er_db
import geopandas as gpd
import shapely
import json, os
import psycopg2, psycopg2.extras
from psycopg2.extras import DictCursor


def bb_overlap(conn, exp_id, event_id, sev_id, is_portfolio):
    if is_portfolio:
        stmt = f'select "UL_LON","UL_LAT","LR_LON","LR_LAT" from race.m_portfolio where portfolio_id={exp_id}'
    else:
        stmt = f'select "UL_LON","UL_LAT","LR_LON","LR_LAT" from race."M_ASSET_SCHEDULE" where "ID"={exp_id}'

    res = er_db.fetch_one(conn, stmt)
    # print(res)
    bbox = shapely.geometry.box(res['UL_LON'], res['UL_LAT'], res['LR_LON'],
                                res['LR_LAT'])
    exp = gpd.GeoDataFrame(geometry=gpd.GeoSeries([bbox]))
    # print(f'Sev id: {sev_id}, Event id: {event_id}')
    stmt = f'select * from "GetEventBondingBox"({sev_id}, {event_id})'
    res = er_db.fetch_one(conn, stmt)
    bbox = shapely.geometry.box(res['UL_LON'], res['UL_LAT'], res['LR_LON'],
                                res['LR_LAT'])
    evt = gpd.GeoDataFrame(geometry=gpd.GeoSeries([bbox]))

    out = gpd.overlay(exp, evt, how='intersection')
    if out.empty:  # no intersection
        return False
    return True


def bb_geoms_overlap(alert_data):
    with er_db.get_db_conn(alert_data['SubDomainName']) as conn:
        exp_id = alert_data['schedule']['id']
        event_info = alert_data['analysis']['Events'][0]
        is_portfolio = alert_data['schedule']['isPortfolio']
        return bb_overlap(conn, exp_id, event_info['EventID'],
                          event_info['SeverityModelID'], is_portfolio)


if __name__ == '__main__':
    try:
        with open('alert.json', 'r') as f:
            data = json.load(f)
        if bb_geoms_overlap(data):
            print('yes')
        else:
            print('no')
    except Exception as e:
        print(f'Exception: {e}')
        pass
