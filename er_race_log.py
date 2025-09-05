import json, os, sys, re, uuid
import pandas as pd
import numpy as np
import jmespath as jp
from datetime import datetime
from collections import OrderedDict

reg_create_session = re.compile(
    '\{"Command":"CreateSession","CommandID":"CreateSession","User":"([^"]*)"')
reg_terminate_session = re.compile(
    '\{"Command":"TerminateSession","User":"([^"]*)"')
reg_message = re.compile('\{"Command":"Execute", "User":"([^"]*)"')
reg_reply = re.compile(
    '\{"Type":"Reply","UserID":"([^"]*)","CommandID":"([^"]*)"')
reg_time = re.compile(
    '[0-9]{4}-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-[0-9]{2} [012][0-9]:[0-5][0-9]:[0-5][0-9]\.[0-9]{5}'
)

reg_sov = re.compile('ImportExposureFromDB\(([0-9]+)')
reg_portfolio = re.compile('ImportContractPortfolio\(([0-9]+)')

cmd_name_map = {
    'Analysis0': 'Analysis',
    'GetSummary': 'Summary',
    'OverlayParams': 'HeatMap',
    'TopNData': 'TopN Assets',
    'TopNContracts': 'TopN Contracts',
    'GroupBy': 'Contribution',
    'GroupByMulti': 'Contribution Multi'
}

# messages = OrderedDict()
# replies = OrderedDict()

groups = {}


def add_new_group(cmd_json):
    user = cmd_json['User']
    group_id = uuid.uuid4()
    groups[user] = str(group_id)
    cmd_json['group'] = str(group_id)


def get_group_id(**kwargs):
    user = kwargs.get('user')
    cmd = kwargs.get('cmd')
    if user is not None:
        return groups.get(user)
    elif cmd is not None:
        user = cmd.get('User')
        if user is not None:
            return groups.get(user)
    return None


def remove_group(cmd_json):
    cmd_json['group'] = get_group_id(cmd=cmd_json)
    groups.pop(cmd_json['User'])


def add_exp_id(row):
    m = reg_sov.search(row['Script'])
    if m is None:
        return 0
    return int(m.group(1))


def add_portfolio_id(row):
    m = reg_portfolio.search(row['Script'])
    if m is None:
        return 0
    return int(m.group(1))


# def add_exposure_for_user(inp):
#     script = inp['Script']
#     m = reg_sov.search(script)
#     if m is None:
#         m = reg_portfolio.search(script)
#     id = int(m.group(1))


def add_group_id_for_cmd(cmd):
    cmd['group'] = get_group_id(cmd=cmd)


def get_cmd_details(cmd):
    keys = cmd.keys()
    val = None
    cmd_name = cmd.get('Command')
    if cmd_name == 'CreateSession':
        add_new_group(cmd)
    elif cmd_name == 'TerminateSession':
        cmd['CommandID'] = 'TerminateSession'
        remove_group(cmd)
    else:
        add_group_id_for_cmd(cmd)

    if 'Script' in keys:
        if 'loadStatus' in cmd['Script']:
            cmd['Name'] = 'Import Exposure'
        elif 'prepareTerminat' in cmd['Script']:
            cmd['Name'] = 'Prepare Terminate Session'
    else:
        cmd['Script'] = '-'

    for k, v in cmd_name_map.items():
        if k in keys:
            val = cmd.pop(k)
            cmd['Name'] = v
            break

    if not 'Name' in cmd.keys():
        cmd['Name'] = cmd['Command']
    cmd['Input'] = val


def parse_reply(line):
    pass


def add_event_ids(df):
    def _add_event_id_(row):
        event_id = jp.search('Events[0].EventID', row['Input'])
        if event_id is not None:
            event_id = int(event_id)
        else:
            event_id = 0
        return pd.Series({'group': row['group'], 'event_id': event_id})

    mask = (df['Name'] == 'Analysis')
    out = df[mask].apply(_add_event_id_, axis=1)
    merged = pd.merge(df, out, how='inner', on='group').drop_duplicates(
        subset=['group', 'Name', 'event_id'], keep='first')
    return merged


def add_exposure_types(df):
    sov_mask = (df['Script'].notnull()
                & df['Script'].str.contains('ImportExposure'))
    portfolio_mask = (df['Script'].notnull()
                      & df['Script'].str.contains('ImportContractPortfolio'))

    conditions = [sov_mask, portfolio_mask]

    exp_type = pd.DataFrame(np.select(conditions, ['Sov', 'Portfolio'],
                                      default='-'),
                            columns=['exp_type'])
    exp_type['group'] = df['group']
    exp_type = exp_type.loc[exp_type['exp_type'] != '-']

    df.loc[sov_mask, 'exp_id'] = df[sov_mask].apply(add_exp_id, axis=1)
    df.loc[portfolio_mask,
           'exp_id'] = df[portfolio_mask].apply(add_portfolio_id, axis=1)

    exp_ids = df.loc[df['exp_id'].notnull(), ['group', 'exp_id']]
    df = pd.merge(df, exp_ids, how='inner', on='group',
                  suffixes=['_x', None]).drop(columns='exp_id_x')
    df['exp_id'] = df['exp_id'].astype(np.int64)
    df = pd.merge(df, exp_type, how='inner', on='group')
    return df


def get_time(line):
    m = reg_time.search(line)
    if m is not None:
        return datetime.strptime(m.group(0), '%Y-%b-%d %H:%M:%S.%f')
    return datetime.now()


def read_log(fn):
    with open(fn, 'r') as f:
        lines = f.readlines()
    cmds = []
    for l in lines:
        m = None
        if m is None:
            m = reg_create_session.search(l)
        if m is None:
            m = reg_terminate_session.search(l)
        if m is None:
            m = reg_message.search(l)
        if m is None:
            reply_match = reg_reply.search(l)
            if reply_match is not None:
                parse_reply(l)
            continue
        cmd_dict = json.loads(l[m.start():])
        get_cmd_details(cmd_dict)
        cmd_dict['time'] = get_time(l)
        cmds.append(cmd_dict)
    df = pd.DataFrame(cmds)

    df = add_exposure_types(df)
    df = add_event_ids(df)

    return df.sort_values(by='time').copy()


def add_message(l, m):
    cmd = {'msg_time': get_time(l), 'type': 'message'}
    try:
        cmd_dict = json.loads(l[m.start():])

        cmd.update(cmd_dict)
    except Exception:
        pass
    return cmd


def add_reply(l, m):
    cmd = {'rep_time': get_time(l)}
    try:
        cmd_dict = json.loads(l[m.start():])
        cmd_dict['truncated'] = False
    except Exception:
        cmd_dict = {
            'Type': 'Reply',
            'UserID': m.group(1),
            'CommandID': m.group(2),
            'truncated': True
        }

    cmd.update(cmd_dict)
    return cmd


def read_log_2(fn):
    with open(fn, 'r') as f:
        lines = f.readlines()
    cmds = []
    replies = []
    for l in lines:
        m = reg_message.search(l)
        if m is not None:
            cmds.append(add_message(l, m))
            continue
        m = reg_reply.search(l)
        if m is not None:
            replies.append(add_reply(l, m))
            continue
    df = pd.DataFrame(cmds)
    df.drop(columns=['Command'], inplace=True)

    rep_df = pd.DataFrame(replies).query('Type.notnull()', engine='python')
    rep_df.drop(columns=['Seq', 'ResultName', 'Type', 'UserID'], inplace=True)

    for col in [
            'OverlayParams', 'TopNData', 'GroupByMulti', 'GroupBy',
            'Analysis0', 'GetSummary'
    ]:
        if not col in df.columns:
            continue
        mask = df[col].notnull()
        df.loc[mask, 'cmd_type'] = col
        # df.drop(columns=[col], inplace=True)

    merged_with_rep = pd.merge(df, rep_df, how='left', on='CommandID')
    return merged_with_rep
