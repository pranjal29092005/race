import os, json
from race_server import RaceServer
from portfolio_analysis import run_pipeline
import psycopg2
from template_utils import template_creation_for_analysis
from pi_utils import stream_parquet_rows

cur_dir = os.path.dirname(os.path.abspath(__file__))

portfolio_id_from_user_for_exposure_A = 43370 #41366
portfolio_id_from_user_for_exposure_B = 43370
output_parquet_expB_path = run_pipeline(portfolio_id=portfolio_id_from_user_for_exposure_B, working_env="prod")
print(f"Pipeline completed. Output path: {output_parquet_expB_path}")
lat_long_pq_table = stream_parquet_rows(
        output_parquet_expB_path,
        columns=["asset_name","asset_number", "latitude","longitude"],
        rename=None,
        as_dict=True
    )
print(lat_long_pq_table)

import time
config_json = os.path.join(cur_dir, 'run_tests_config.json')

with open(config_json, 'r') as f:
    config = json.load(f)

race_ip = config['Server']['Race']
## EXPosure-A== 22068,41366
## Exposure-B = 43370
def get_db_conn():
    db_info = config['Server']['Db']
    conn_str = 'host={} dbname={} user={} password={} port={}'.format(
        db_info['PG_HOST'], db_info['PG_DB'], db_info['PG_USER'], db_info['PG_PW'], db_info['PG_PORT']
    )
    return psycopg2.connect(conn_str)

race_server = RaceServer(user='tmh.test@eigenrisk.com', ip=race_ip)
print("command for the session creation started...")
race_server.create_session()
print("session got created...")
portfolio_data = {'exposures': [{'id': portfolio_id_from_user_for_exposure_A, 'portfolio': True}]}
print(f"Portfolio data: {portfolio_data}")
portfolio_loading_message = race_server.load_portfolio(portfolio_data)
print(f"Exposure import response: {portfolio_loading_message}")
portfolio_value = portfolio_loading_message.get("Value", 0)
print(f"Portfolio value: {portfolio_value}")

saved_responses = []
for lat_long_json_obj in lat_long_pq_table:
    print(f"Processing lat_long_json_obj: {lat_long_json_obj}")
    start_time = time.time()
    lat = lat_long_json_obj['latitude']
    long = lat_long_json_obj['longitude']
    asset_name = lat_long_json_obj['asset_name']
    print(f"Processing for the: Lat: {lat}, Long: {long}")

    data_for_analysis = template_creation_for_analysis(lat,long,radius=5)
    set_analysis_call = race_server.run_analysis(data_for_analysis)

    # print(f"Set analysis call response: {set_analysis_call}")
    set_analysis_value = set_analysis_call.get("Value", 0)
    # print(f"Set analysis value: {set_analysis_value}")

    exposures_summary_call = race_server.get_exposure_summary()
    # print(f"Exposures summary call response: {exposures_summary_call}")
    exposures_summary_value = exposures_summary_call.get("Value", 0)
    # print(f"Exposures summary value: {exposures_summary_value}")
    saved_responses.append({
        'exposure-summary': exposures_summary_value['Analysis0'],
        'latitude': lat,
        'longitude': long
    })
    print(f"Time taken for analysis with asset:{asset_name} is: {time.time() - start_time} seconds")
print(f"Saved responses: {saved_responses}")