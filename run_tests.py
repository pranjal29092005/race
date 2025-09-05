import os, sys, json
from race_server import RaceServer
import psycopg2, psycopg2.extras
from psycopg2.extras import DictCursor
import pandas as pd

cur_dir = os.path.dirname(os.path.abspath(__file__))
config_json = os.path.join(cur_dir, 'run_tests_config.json')

with open(config_json, 'r') as f:
    config = json.load(f)

race_ip = config['Server']['Race']
gel = 'ContractLoss_Value All Types_EL'
gl = 'ContractLoss_Value All Types_GL'


def get_db_conn():
    db_info = config['Server']['Db']
    conn_str = 'host={} dbname={} user={} password={} port={}'.format(
        db_info['PG_HOST'], db_info['PG_DB'], db_info['PG_USER'], db_info['PG_PW'], db_info['PG_PORT']
    )
    return psycopg2.connect(conn_str)


def get_event_info(event_id, cursor):
    cursor.execute(f'select "EVENT_SEV_MODEL_ID" from race."B_EVENT_SEVERITY" where "EVENT_ID"={event_id}')
    row = cursor.fetchone()
    sev_model_id = row['EVENT_SEV_MODEL_ID']

    cursor.execute(f'select "EVENT_SET_ID","LABL" from race."M_EVENT" where "ID"={event_id}')
    row = cursor.fetchone()
    event_set_id = row['EVENT_SET_ID']
    event_name = row['LABL']

    cursor.execute(f'select "PERIL","SOURCE" from "GetEventSetDetails"({event_set_id})')
    row = cursor.fetchone()
    peril = row['PERIL']
    source = row['SOURCE']

    cursor.execute(f'select "SHAPE_TYPE" from "GetEventSeverity"({sev_model_id},{event_id})')
    row = cursor.fetchone()
    shape_type = row['SHAPE_TYPE']

    cursor.execute(f'select "SUB_PERIL" from "GetEventSevModelDetails"({sev_model_id})')
    row = cursor.fetchone()
    sub_peril = row['SUB_PERIL']

    return {
        'id': event_id,
        'sev_id': sev_model_id,
        'name': event_name,
        'peril': peril,
        'subPeril': sub_peril,
        'source': source,
        'type': shape_type,
        'delta': {
            'lat': 0.0,
            'lon': 0.0
        }
    }


def get_filter_info():
    filters = config.get('Filters')
    if filters is None:
        return (None, None)
    filter_type = filters.get('type', 'AND')
    filter_type = filter_type.upper()
    conditions = filters.get('conditions', [])

    ret = []
    for c in conditions:
        cur = c.split(',')
        ret.append({'attr': cur[0], 'op': cur[1], 'value': cur[2]})
    return filter_type, ret


event_applied = True

eventId = config.get('EventId')
if not eventId:
    event_applied = False

event_info = None

if event_applied:
    with get_db_conn() as conn:
        cursor = conn.cursor(cursor_factory=DictCursor)
        event_info = get_event_info(eventId, cursor)
        event_info.update({'delta': {'lon': 0.0, 'lat': 0.0}})
        print(event_info)

race_server = RaceServer(user='tmh.test@eigenrisk.com', config['Server']['Race'])
race_server.create_session()

exposures = config['Exposures']

for exp in exposures:
    print(f"Running exposure {exp} ...")
    exp_data = {'exposures': [{'id': exp, 'portfolio': True}]}
    out = race_server.import_exposure(exp_data)
    value = out.get('Value', 0)
    if not value:
        print(f'load exposure failed for {exp}')
        continue

    filter_type, objs = get_filter_info()
    data = {'damageFunctionId': 100021, 'analysisList': [{}]}
    if filter_type is not None:
        data['filters'] = objs
        data['filterType'] = filter_type
    if event_info is not None:
        data['events'] = [event_info]

    out = race_server.run_analysis(data)
    value = out.get('Value', 0)
    if not value:
        print(f'analysis failed for {exp}')
        continue

    data = {'analysisCount': 1, 'vars': config['Summary']}
    out = race_server.get_summary(data)
    summary_out = []
    for k, v in out['Value']['Analysis0'].items():
        summary_out.append({'Name': k, 'Value': v})

    data = {
        'analysisCount': 1,
        'topnBy': 'assets',
        'vars': config['Topn Assets'],
        'count': -1,
        'sortMeasure': gel,
        'additionalSortMeasure': 'TIV'
    }
    topn_assets_out = race_server.get_topn(data)

    data = {
        'analysisCount': 1,
        'topnBy': 'contracts',
        'vars': config['Topn Contracts'],
        'count': -1,
        'sortMeasure': gel,
        'additionalSortMeasure': 'TIV'
    }
    topn_contracts_out = race_server.get_topn(data)

    writer = pd.ExcelWriter(f'results-{exp}.xlsx', engine='xlsxwriter')
    try:
        pd.DataFrame(summary_out).to_excel(writer, sheet_name='summary')
    except:
        pass

    try:
        pd.DataFrame(topn_assets_out['Value']['Analysis0']).to_excel(writer, sheet_name='topn_assets')
    except:
        pass

    try:
        pd.DataFrame(topn_contracts_out['Value']['Analysis0']).to_excel(writer, sheet_name='topn_contracts')
    except:
        pass

    writer.close()
