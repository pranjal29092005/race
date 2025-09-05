import os, sys, pathlib
import pandas as pd, numpy as np
import argparse
import er_db
from psycopg2.extras import RealDictCursor as rdc
from importlib.util import spec_from_file_location, module_from_spec
from importlib import import_module
import click

from PySide6.QtCore import (QObject, Signal, Slot, Qt)
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (QApplication, QMainWindow)
from PySide6.QtWidgets import (
    QVBoxLayout, QHBoxLayout, QSpacerItem, QWidget, QTreeWidget, QTreeWidgetItem, QLabel, QComboBox, QPushButton,
    QSizePolicy
)
from icecream import ic

parser = argparse.ArgumentParser(prog='get-ws-info')
parser.add_argument('--env', '-e', dest='env', type=str, required=True, choices=['alpha', 'prod', 'integration'])
parser.add_argument('--name', '-n', dest='ws_name', type=str)
parser.add_argument('--id', '-i', dest='ws_id', type=int)

this_dir = pathlib.Path(__file__).parent
root_dir = this_dir.parent

sys.path.insert(0, str(root_dir.joinpath('Tools', 'PyGui')))
import gui.TreeUtils as tu
from gui.WaitCursor import WaitCursor
from gui.Stylesheet import set_stylesheet


class InvalidInfo(Exception):
    pass


def to_qcolor(item):
    return QColor(item)


class Gui(QMainWindow):
    def __init__(self, **kwargs):
        super().__init__()
        self.get_exp = getattr(import_module('get-exp'), 'Exposure')
        self.env = kwargs.get('env')
        self.ws_name = kwargs.get('ws_name')
        self.ws_id = kwargs.get('ws_id')
        self.setWindowTitle("Get Workspace Info")
        self.col_names = [
            'Audit Id', 'Schedule Id', 'Exposure Name', 'Exposure Type', 'Get Etf', 'Get Binaries', 'Get Sources'
        ]
        self.column_indices = {
            'aid': self.col_names.index('Audit Id'),
            'sid': self.col_names.index('Schedule Id'),
            'name': self.col_names.index('Exposure Name'),
            'portfolio': self.col_names.index('Exposure Type'),
            'etf': self.col_names.index('Get Etf'),
            'bin': self.col_names.index('Get Binaries'),
            'src': self.col_names.index('Get Sources')
        }
        self.sov_color = '#0000aa'
        self.portfolio_color = '#00aa00'
        self.filters = ['No Filter', 'Portfolio', 'Sov']
        self.setup_gui()
        self.resize(1000, 300)
        self.df = None
        try:
            self.df = self.get_ws_info()
            self.populate_items(self.df)
        except InvalidInfo:
            pass
        self.connect_slots()

    def populate_items(self, df):
        if df is None:
            return
        tu.populateTreeWidget(self.ws_items, df, columns=self.col_names)
        tu.resizeColumns(self.ws_items, padding=5)
        for i in range(self.ws_items.columnCount()):
            tu.setColumnData(self.ws_items, i, Qt.ForegroundRole, df['color'].apply(QColor))

    def filter_changed(self, current):
        col_name = self.col_names[3]
        if current == self.filters[0]:
            self.populate_items(self.df)
        elif current == self.filters[1]:
            self.populate_items(self.df[self.df[col_name] == 'P'])
        else:
            self.populate_items(self.df[self.df[col_name] == 'S'])

    def connect_slots(self):
        self.filter_combo.currentTextChanged.connect(self.filter_changed)
        self.fetch_selected.clicked.connect(self.fetch_selected_exposures)

    def fetch_selected_exposures(self):
        with WaitCursor(qApp.activeWindow()) as wc, er_db.get_db_conn(self.env) as conn:
            for i in range(self.ws_items.topLevelItemCount()):
                item = self.ws_items.topLevelItem(i)
                etf = item.checkState(self.column_indices['etf']) == Qt.Checked
                bin = item.checkState(self.column_indices['bin']) == Qt.Checked
                src = item.checkState(self.column_indices['src']) == Qt.Checked
                if (not etf) and (not bin) and (not src):
                    continue
                audit_id = item.text(self.column_indices['aid'])
                portfolio = item.text(self.column_indices['portfolio']) == 'P'
                exp_id = item.text(self.column_indices['sid'])
                exp_name = item.text(self.column_indices['name'])
                exp = self.get_exp(
                    exp_id=int(exp_id),
                    exp_name=exp_name,
                    env=self.env,
                    audit_id=int(audit_id),
                    portfolio=portfolio,
                    bin=bin,
                    src=src,
                    etf=etf,
                    db_conn=conn
                )
                exp.execute()

    def get_ws_id(self, conn):
        if self.ws_id:
            return self.ws_id
        cursor = conn.cursor(cursor_factory=rdc)
        query = f"select id from race.m_account where name='{self.ws_name}'"
        cursor.execute(query)
        res = cursor.fetchone()
        if res is None:
            raise InvalidInfo()
        return res['id']

    def get_portfolios(self, conn, acc_id):
        cursor = conn.cursor(cursor_factory=rdc)
        query = f'''
with exp_table as (select audit_id aid, portfolio_id sid, portfolio_name name from race.m_portfolio),
     aud_table as (select "ID" aid from race.c_audit where account_id={acc_id})
     select exp_table.aid, exp_table.sid, exp_table.name from exp_table
     INNER JOIN aud_table
     ON aud_table.aid = exp_table.aid
        '''
        cursor.execute(query)
        res = cursor.fetchall()
        return res

    def get_sovs(self, conn, acc_id):
        cursor = conn.cursor(cursor_factory=rdc)
        query = f'''
with exp_table as (select "AUDIT_ID" aid, "ID" sid, "SCHEDULE_NAME" name from race."M_ASSET_SCHEDULE"),
     aud_table as (select "ID" aid from race.c_audit where account_id={acc_id})
     select exp_table.aid, exp_table.sid, exp_table.name from exp_table
     INNER JOIN aud_table
     ON aud_table.aid = exp_table.aid
        '''
        cursor.execute(query)
        res = cursor.fetchall()
        return res

    def get_ws_info(self):
        with er_db.get_db_conn(self.env) as conn:
            acc_id = self.get_ws_id(conn)
            ic(acc_id)
            sovs = self.get_sovs(conn, acc_id)
            portfolios = self.get_portfolios(conn, acc_id)
            items = []
            for s in sovs:
                items.append(
                    {
                        self.col_names[0]: s['aid'],
                        self.col_names[1]: s['sid'],
                        self.col_names[2]: s['name'],
                        self.col_names[3]: 'S',
                        'color': self.sov_color
                    }
                )
            for p in portfolios:
                items.append(
                    {
                        'color': self.portfolio_color,
                        self.col_names[0]: p['aid'],
                        self.col_names[1]: p['sid'],
                        self.col_names[2]: p['name'],
                        self.col_names[3]: 'P'
                    }
                )
            df = pd.DataFrame(items)
            df[self.col_names[4]] = False
            df[self.col_names[5]] = False
            df[self.col_names[6]] = False

            return df

    def vspacer(self, layout):
        spacer = QSpacerItem(20, 40, QSizePolicy.Minimum, QSizePolicy.Expanding)
        layout.addItem(spacer)

    def hspacer(self, layout):
        spacer = QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum)
        layout.addItem(spacer)

    def setup_gui(self):
        self.central_widget = QWidget(self)

        self.ws_items = QTreeWidget(self.central_widget)
        self.ws_items.setAlternatingRowColors(True)

        self.exp_label = QLabel(self.central_widget)
        self.exp_label.setText("Filter exposure type")

        self.filter_combo = QComboBox(self.central_widget)
        self.filter_combo.addItems(self.filters)
        self.filter_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        self.filter_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        self.fetch_selected = QPushButton('Fetch Selected', self.central_widget)

        self.central_layout = QVBoxLayout(self.central_widget)
        self.central_layout.setSpacing(5)

        self.central_layout.addWidget(self.ws_items)

        self.ops_layout = QHBoxLayout()
        self.ops_layout.setSpacing(10)
        self.ops_layout.addWidget(self.exp_label)
        self.ops_layout.addWidget(self.filter_combo)
        self.hspacer(self.ops_layout)
        self.ops_layout.addWidget(self.fetch_selected)

        self.central_layout.addLayout(self.ops_layout)

        self.setCentralWidget(self.central_widget)


if __name__ == '__main__':
    argv = parser.parse_args()
    if argv.ws_id is None and argv.ws_name is None:
        click.secho('Workspace name or id is not specified', fg='red', bold=True)
        sys.exit(-1)

    app = QApplication(sys.argv)
    set_stylesheet(app)

    gui = Gui(env=argv.env, ws_id=argv.ws_id, ws_name=argv.ws_name, this_file=__file__)
    gui.show()

    sys.exit(app.exec())
