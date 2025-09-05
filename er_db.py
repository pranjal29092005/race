import os, sys
import psycopg2, psycopg2.extras
import pandas as pd
import sqlalchemy
from psycopg2.extras import RealDictCursor
import json
import exceptions
from icecream import ic

cur_dir = os.path.dirname(os.path.abspath(__file__))
db_config_file = os.path.join(os.path.dirname(cur_dir), 'Aws.cfg')
conf = None

with open(db_config_file, 'r') as f:
    conf = json.load(f)

if conf is None:
    print("Unable to read config file")
    sys.exit(-1)


def get_db_keys():
    return conf.keys()


def get_db_info(key):
    rv = conf[key.capitalize()]
    for k, v in rv.items():
        if k.endswith('BUCKET'):
            os.environ[k] = v
    return rv


def get_sql_alchemy_engine(key):
    db_info = get_db_info(key)
    url = f'postgresql+psycopg2://{conf["PG_USER"]}:{conf["PG_PW"]}@{db_info["PG_HOST"]}:{conf["PG_PORT"]}/{db_info["PG_DB"]}'
    engine = sqlalchemy.create_engine(url, execution_options={'isolation_level': 'AUTOCOMMIT'})
    return engine


def get_audit_ids_for_exp_group(conn, eg_id):
    audit_ids = []
    query = f"select portfolio_ids from get_portfolios_exposure_group({eg_id})"
    query = f"select audit_id from race.m_portfolio where portfolio_id in ({query}) order by portfolio_id"
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query)
    res = cursor.fetchall()
    for r in res:
        audit_ids.append(r['audit_id'])
    return audit_ids


def get_db_conn(key):
    db_info = get_db_info(key)
    conn_str = 'host={} dbname={} user={} password={} port={}'.format(
        db_info['PG_HOST'], db_info['PG_DB'], conf['PG_USER'], conf['PG_PW'], conf['PG_PORT']
    )
    conn = psycopg2.connect(conn_str)
    conn.set_session(readonly=True, autocommit=True)
    return conn


def get_audit_id(conn, sch_id):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(f'select audit_id from race.m_portfolio where portfolio_id={sch_id}')
    row = cursor.fetchone()
    if row:
        return (row['audit_id'], True)

    cursor.execute(f'select "AUDIT_ID" from race."M_ASSET_SCHEDULE" where "ID"={sch_id}')
    row = cursor.fetchone()
    if row:
        return (row['AUDIT_ID'], False)
    return (None, None)


def get_audit_id_v2(conn, sch_id, portfolio):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    query = None
    if portfolio:
        query = f'select audit_id from race.m_portfolio where portfolio_id={sch_id}'
    else:
        query = f'select "AUDIT_ID" as audit_id from race."M_ASSET_SCHEDULE" where "ID"={sch_id}'
    cursor.execute(query)
    row = cursor.fetchone()
    if row:
        return (row['audit_id'], portfolio)
    return (None, None)


def get_exposure_id(conn, audit_id, portfolio):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    query = None
    if portfolio:
        query = f'select portfolio_id as exp_id from race.m_portfolio where audit_id={audit_id}'
    else:
        query = f'select "ID" as exp_id from race."M_ASSET_SCHEDULE" where "AUDIT_ID"={audit_id}'
    cursor.execute(query)
    row = cursor.fetchone()
    if row:
        return (row['exp_id'])
    return None


def get_exp_name(conn, sch_id, portfolio):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    query = None
    if portfolio:
        query = f'select portfolio_name as name from race.m_portfolio where portfolio_id={sch_id}'
    else:
        query = f'select "SCHEDULE_NAME" as name from race."M_ASSET_SCHEDULE" where "ID"={sch_id}'
    cursor.execute(query)
    row = cursor.fetchone()
    if row:
        return (row['name'])
    return None


def get_audit_trail(conn, audit_id):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    ret = []

    if isinstance(audit_id, int):
        aid = audit_id
    else:
        aid = audit_id[0]
    field = 'previous_audit_id'
    while aid:
        stmt = f'select {field} from race.c_audit where "ID" = {aid}'
        ic(stmt)
        cursor.execute(stmt)
        row = cursor.fetchone()
        if row:
            aid = row[field]
            if aid:
                ret.append(aid)
        else:
            break

    return ret


def get_file_path(conn, audit_id):
    if audit_id is None:
        return None
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(f'select * from race.c_audit where "ID" = {audit_id}')
    row = cursor.fetchone()
    if row:
        return row['Filepath']
    return None


def fetch_one(conn, stmt):
    print(f'Executing: {stmt}')
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(stmt)
    row = cursor.fetchone()
    if row:
        return row
    else:
        raise exceptions.DbNoResultException


def exec_stmt(conn, stmt, **kwargs):
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    dry_run = kwargs.get('dry_run', False)
    if dry_run:
        return {}
    cursor.execute(stmt)
    fetchall = kwargs.get('fetch_all', False)
    if fetchall:
        return cursor.fetchall()
    else:
        return cursor.fetchone()


def get_sev_model_id(conn, id):
    query = f'select "EVENT_SEV_MODEL_ID" from race."B_EVENT_SEVERITY" where "EVENT_ID"={id}'
    row = fetch_one(conn, query)
    return row['EVENT_SEV_MODEL_ID']


def get_peril_table(conn):
    query = '''
select ph.peril_id, ph.sub_peril_id, ph.peril_name, ph.sub_peril_name, pp.peril_code peril_code, sp.peril_code sub_peril_code
FROM
(select peril_id,peril_name,sub_peril_id,sub_peril_name from "GetPerilHierarchy"()) ph
INNER JOIN
(select peril_id,peril_name, peril_code from "GetSubPerilData"()) sp
ON
ph.sub_peril_id = sp.peril_id
INNER JOIN
(select peril_id, peril_name, peril_code from "GetPerilData"()) pp
ON
ph.peril_id = pp.peril_id
    '''
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query)
    res = cursor.fetchall()
    return pd.DataFrame(res)
