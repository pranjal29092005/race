import os, sys, json
from bs4 import BeautifulSoup
from rich.console import Console
from rich.table import Table
from rich import print_json
from rich.style import Style
import rich
import argparse
import pandas as pd

parser = argparse.ArgumentParser(prog='read-newman-report')
parser.add_argument('-f', dest='file', required=True, type=str)

success_style = Style(color='green', bold=False)
fail_style = Style(color='red', bold=True)
header = Style(color='cyan')
desc = Style(color='yellow', bold=True)


def display_report(**kwargs):
    console = kwargs.get('console')
    df = kwargs.get('report')
    table = Table(title='Test Results')
    table.add_column('section name')
    table.add_column('test name')
    table.add_column('error')
    for idx, row in df.iterrows():
        style = success_style
        if row['test_fail'] == 1:
            style = fail_style
        table.add_row(row['name'], row['test_name'], row['error'], style=style)
    console.print(table)


def read_report(**kwargs):
    console = kwargs.get('console')
    fn = kwargs.get('file')
    soup = None
    with open(fn, 'r') as f:
        soup = BeautifulSoup(f, 'html.parser')
    elems = soup.find_all('table', attrs={'class': 'table-condensed'})
    tests = []
    for table in elems:
        section_name = ''
        for p in table.find_parents('div', attrs={'class': 'panel-default'}):
            x = p.find('div', attrs={'class': 'panel-heading'})
            section_name = x.text.strip()
        for row in table.find_all('tr'):
            cols = [elem.text.strip() for elem in row.find_all('td')]
            if cols:
                test = {
                    'name': section_name,
                    'test_name': cols[0],
                    'test_pass': int(cols[1]),
                    'test_fail': int(cols[2])
                }
                tests.append(test)
    failures_start = False
    errors = []
    for elem in soup.find_all('h4'):
        if elem.text.strip().lower() == 'failures':
            for s in elem.find_next_siblings('div'):
                x = s.find('div', attrs={'class': 'panel-heading'})
                x = s.find('div', attrs={'class': 'panel-body'})
                desc_text = list(filter(lambda s: 'description' in s.lower(),
                                        x.text.strip().split('\n')))[0].replace('Description', '')
                errors.append(desc_text)

    df = pd.DataFrame(tests)
    df = df[df.test_fail == 1]
    df['error'] = errors
    return df


if __name__ == '__main__':
    argv = parser.parse_args()
    console = Console()
    df = read_report(console=console, file=argv.file)
    display_report(console=console, report=df)
