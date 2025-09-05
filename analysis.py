import os, sys, json, subprocess, uuid
from jinja2 import Environment as j2env, FileSystemLoader as j2fsl
import er_db
from psycopg2.extras import RealDictCursor
import pandas as pd
from datetime import datetime
from icecream import ic


class NoPerilException(Exception):
    pass


class InvalidPerilException(Exception):
    pass


class Analysis:
    def __init__(self):
        super().__init__()

        self.exp_filters = []
        self.results_key = None
        self.concentric_circles = None
        self.date = None
        self.threshold = 0
        self.exposure_id = None
        self.event_id = None
        self.damage_function = None
        self.measure = None
        self.portfolio = None
        self.currency = "USD"
        self.sev_model_id = None
        self.delta_lat = None
        self.delta_lon = None
        self.program_id = None
        self.analysis_key = None
        self.index = 0
        self.script = None
        this_dir = os.path.dirname(os.path.abspath(__file__))
        template_path = os.path.normpath(os.path.join(this_dir, "Templates"))
        self.env = j2env(loader=j2fsl(template_path), trim_blocks=True)

    def add_attr_filter(self, attr, value):
        if value is None:
            return
        filter_op = 'IN' if isinstance(value, list) else 'EQ'
        self.add_filter(attr, filter_op, value)

    def move_footprint(self, move_conf):
        if move_conf is None:
            return
        self.delta_lat = move_conf['delta_lat']
        self.delta_lon = move_conf['delta_lon']

    def set_cause_of_loss(self, peril_code, db_conn):
        peril, sub_peril = self.get_perils(peril_code, db_conn)
        self.add_filter("Cause Of Loss", "EQ", sub_peril)

    def get_results_key(self):
        if self.results_key is None:
            return str(uuid.uuid4())
        return self.results_key

    def add_filter(self, attr, op, val, andor="AND"):
        tmpl = self.env.get_template("filter.j2").render(
            {
                "attribute": attr,
                "operator": op,
                "value": val,
                "andor": andor
            }
        )
        self.exp_filters.append(json.loads(tmpl))

    def add_exposure_filters(self, obj):
        if self.exp_filters:
            obj["ExposureFilterSets"] = {
                "AssetModel": "ERBASICS",
                "FilterList": self.exp_filters,
            }
        return obj

    def get_perils(self, peril, db_conn):
        df = er_db.get_peril_table(db_conn)
        df = df[df.sub_peril_code == peril]
        if len(df) != 1:
            raise InvalidPerilException()
        return (df.iloc[0]["peril_code"], self.peril)

    def get_peril_info(self, peril, db_conn):
        df = er_db.get_peril_table(db_conn)
        df = df[df.sub_peril_code == peril]
        if len(df) != 1:
            raise InvalidPerilException()
        return {
            "peril_code": df.iloc[0].peril_code,
            "sub_peril_code": df.iloc[0].sub_peril_code,
            "peril_name": df.iloc[0].peril_name,
            "sub_peril_name": df.iloc[0].sub_peril_name,
        }

    def get_event_info(self, conn):
        if not self.event_id:
            return {}
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        ids = None
        if isinstance(self.event_id, list):
            ids = ','.join([str(i) for i in self.event_id])
        else:
            ids = str(self.event_id)

        events = {}
        event_sets = set()
        sev_ids = set()

        cursor.execute(
            f'select "EVENT_ID","EVENT_SEV_MODEL_ID" from race."B_EVENT_SEVERITY" where "EVENT_ID" in ({ids})'
        )
        for r in cursor.fetchall():
            eid = r['EVENT_ID']
            sid = r['EVENT_SEV_MODEL_ID']
            sev_ids.add(sid)
            events[eid] = {'id': eid, 'sev_id': sid}

        cursor.execute(f'select "ID","EVENT_SET_ID","LABL" from race."M_EVENT" where "ID" in ({ids})')
        for r in cursor.fetchall():
            eid = r['ID']
            events[eid]['name'] = r['LABL']
            events[eid]['event_set_id'] = r['EVENT_SET_ID']
            event_sets.add(r['EVENT_SET_ID'])

        peril_info = {}
        sub_peril_info = {}
        for esid in event_sets:
            cursor.execute(f'select "PERIL","SOURCE" from "GetEventSetDetails"({esid})')
            row = cursor.fetchone()
            if row is not None:
                peril_info[esid] = {'peril': row['PERIL'], 'source': row['SOURCE']}
            else:
                peril_info[esid] = {'peril': '', 'source': ''}

        for sid in sev_ids:
            cursor.execute(f'select "SUB_PERIL" from "GetEventSevModelDetails"({sid})')
            row = cursor.fetchone()
            sub_peril_info[sid] = row["SUB_PERIL"]
        ret = []
        for id, evt in events.items():
            obj = {
                'id': id,
                'sev_id': evt['sev_id'],
                'name': evt['name'],
                'peril': peril_info[evt['event_set_id']]['peril'],
                'source': peril_info[evt['event_set_id']]['source'],
                'subPeril': sub_peril_info[evt['sev_id']]
            }
            ret.append(obj)
        return ret

    def get_concentrics_obj(self, conn):
        this_obj = self.concentric_circles
        pinfo = self.get_peril_info(this_obj['peril'], conn)
        data = {
            'peril': pinfo['peril_code'],
            'subPeril': pinfo['sub_peril_code'],
            'lat': this_obj['center_lat'],
            'lon': this_obj['center_lon'],
            'feature': 'concentrics',
            'damage': this_obj['damages']
        }
        tmpl = self.env.get_template("handdrawn.j2").render(data).replace('\n', '')
        return json.loads(tmpl)

    def to_json(self, conn=None):
        obj = {
            "ExposureFilterSets": {
                "AssetModel": "ERBASICS",
                "FilterList": self.exp_filters,
            },
            "BaseAssetTypeCodes": ["Site"],
            "CurrencyCode": self.currency,
            "IgnoreOutOfBoundAssets": True,
            "ValuationTypeCode": 1,
            "damageAdjustment": 1,
            "quantile": 50,
        }
        if self.date:
            dt = datetime.strptime(self.date, '%d-%m-%Y')
            dt_obj = {'Day': dt.day, 'Month': dt.month, 'Year': dt.year}
            obj['AnalysisDateBegin'] = dt_obj
            obj['AnalysisDateEnd'] = dt_obj
        if self.analysis_key:
            obj["Analysis Key"] = self.analysis_key
        if self.damage_function:
            obj["DamageFunctionID"] = self.damage_function
        if self.program_id:
            obj["ProgramId"] = self.program_id
        if self.event_id and conn is not None:
            events = self.get_event_info(conn)
            obj["Events"] = []
            for event_obj in events:
                self.sev_model_id = event_obj['sev_id']
                obj["Events"].append(
                    {
                        "EventName": event_obj["name"],
                        "peril": event_obj["peril"],
                        "subperil": event_obj["subPeril"],
                        "EventID": event_obj["id"],
                        "SeverityModelID": event_obj["sev_id"],
                        'deltaLat': self.delta_lat if self.delta_lat else 0.0,
                        'deltaLon': self.delta_lon if self.delta_lon else 0.0
                    }
                )
        if self.concentric_circles:
            conc = self.get_concentrics_obj(conn)
            obj['HandDrawnEvent'] = conc['HandDrawnEvent']
        return obj

    def get_script(self):
        return " ".join(
            [
                "string errorMessage;",
                f'if(!runAnalysis("Analysis{self.index}",{self.index}, errorMessage))',
                "{SendErrorWithCode(errorMessage);return;}",
            ]
        )
