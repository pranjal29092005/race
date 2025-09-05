import os, sys
import pyperclip
import pandas as pd
import er_utils

pw_file = os.path.join(os.environ['HOME'], '.passwords.xlsx')

passwords = pd.read_excel(
    pw_file, usecols='B:J', skiprows=[0, 1, 2], header=None).rename(
        columns={
            1: 'ec2_name',
            2: 'pvt_ip',
            3: 'pub_ip',
            6: 'user_Administrator',
            7: 'user_eigenriskprod',
            8: 'user_eigenrisk01'
        }).drop([4, 5, 9], axis=1)

passwords = passwords[passwords.pub_ip.notnull()].copy()
passwords['Item'] = passwords.index + 1

er_utils.tabulate_df(
    passwords[['Item', 'ec2_name', 'pvt_ip', 'pub_ip', 'user_Administrator']])

choice = er_utils.pick_choices(0, len(passwords) + 1)

if choice:
    item = passwords[passwords['Item'] == choice]
    ip = item['pvt_ip'].iloc[0]
    passwd = item['user_Administrator'].iloc[0]
    print(f'Copying password for {ip} to clipboard')
    pyperclip.copy(passwd)
