import os, sys
import er_db


class EventSetException(Exception):
    pass


class EventSetInsufficientArgs(Exception):
    pass


class EventSetInvalidArgs(Exception):
    pass


class EventSet(object):
    def __init__(self, **kwargs):
        super().__init()
        self._populate_info(**kwargs)
        self.conn = None

    def _populate_info(self, **kwargs):
        try:
            for k in [
                    'event_set_name', 'sev_model_name', 'source', 'threshold',
                    'units', 'user_id', 'xdef', 'scaling_factor', 'env',
                    'event_type'
            ]:
                setattr(self, k, kwargs[k])
        except KeyError:
            raise EventSetInsufficientArgs

    def _event_set_exists(self, event_name):
        try:
            stmt = f"""select "ID" from race."B_EVENT_SET" where "LABL"='{event_name}'"""
            res = er_db.fetch_one(self.conn, stmt)
            self.event_set_id = res['ID']
            stmt = f"""select distinct "EVENT_SEV_MODEL_ID" from race."B_EVENT_SEVERITY" where "EVENT_ID" in (select "ID" in race."M_EVENT" where "EVENT_SET_ID"='{self.event_set_id}')"""
            res = er_db.fetch_one(self.conn, stmt)
            self.sev_model_id = res['EVENT_SEV_MODEL_ID']
        except exceptions.DbNoResultException:
            return False
        return True

    def _create_source(self, src):
        stmt = f'select "ID" from race."M_SOURCE" order by 1 desc limit 1'
        res = er_db.fetch_one(self.conn, stmt)
        max_source_id = res['ID']
        new_source_id = max_source_id + 1
        stmt = f"""insert into race."M_SOURCE" ("ID", "CODE", "DESCR") values({new_source_id},'{self.source}','{self.source}') returning "ID" """
        res = er_db.exec_stmt(self.conn, stmt)
        return res['ID']

    def _get_source_id(self):
        if len(self.source) == 0:
            raise EventSetInvalidArgs

        try:
            stmt = f"""select "ID" from race."M_SOURCE" where "CODE"='{self.source}'"""
            res = er_db.fetch_one(self.conn, stmt)
            return res['ID']
        except exceptions.DbNoResultException:
            return self._create_source(src)

    def _get_peril_id(self):
        peril_code = getattr(self, 'peril_code')
        peril_name = getattr(self, 'peril_name')
        if peril_code is None and peril_name is None:
            raise EventSetInsufficientArgs

        if peril_code is None:
            stmt = f"""select "ID" from race."M_PERIL" where "LABL"='{peril_name}'"""
        else:
            stmt = f"""select "ID" from race."M_PERIL" where "PERIL_CODE"='{peril_code}'"""
        res = erdb.fetch_one(self.conn, stmt)
        return res['ID']

    def _get_version_id(self):
        return 1

    def _get_event_type(self):
        try:
            stmt = f"""select "ID" from race."M_EVENT_TYPE" where "LABL"='{self.event_type}'"""
            res = er_db.fetch_one(self.conn, stmt)
            return res['ID']
        except exceptions.DbNoResultException:
            print(f'Invalid event type {self.event_type}')
            raise EventSetException

    def _get_unit_id(self):
        try:
            stmt = f"""select id from race.m_unit where labl='{self.units}'"""
            res = er_db.fetch_one(stmt)
            return res['id']
        except exceptions.DbNoResultException:
            print("Entry for unit not found in db")
            raise EventSetException

    def _get_xdef_id(self):
        try:
            stmt = f"""select id from race.m_xdefinition where labl='{self.xdef}'"""
            res = er_db.fetch_one(stmt)
            return res['id']
        except exceptions.DbNoResultException:
            stmt = f"""insert into race.m_xdefinition (labl,descr,perilid,threshold) values ('{self.xdef}','{self.xdef}',{self.perilid}, {self.threshold}) returning id"""
            res = er_db.exec_stmt(stmt)
            return res[0]

    def _map_xdef_and_unit(self):
        try:
            stmt = f"""select * from race.b_xdefunit where xdefid='{self.xdef}' and unitid='{self.unit}'"""
            res = er_db.fetch_one(stmt)
            print(f'{self.xdef} mapped to {self.unit}')
        except exceptions.DbNoResultException:
            stmt = f"""insert into race.b_xdefunit (xdefid, unitid, intensity_value) values ({self.xdef}, {self.unit}, {self.intensity})"""
            res = er_db.exec_stmt(stmt)
            self._map_xdef_and_unit()

    def _create_event_set(self):
        stmt = f"""
        insert into race."B_EVENT_SET"
        ("LABL", "DESCR", "SOURCE_ID", "PERIL_ID", "VERSION_ID", "EVENT_TYPE_ID", scaling_factor, user_id)
        values
        ('{self.event_set_name}','{self.event_set_name}',{self.source_id},{self.peril_id},{self.version_id},{self.event_type_id},{self.scaling_factor},{self.user_id})
        returning "ID"
        """
        res = er_db.exec_stmt(self.conn, stmt)
        return res['ID']

    def _create_event_sev_model(self):
        stmt = f"""
        insert into race."M_EVENT_SEV_MODEL" ("LABL","DESCR","PERIL_ID","EVENT_SEV_TYPE_ID","SOURCE_ID")
        values
        ('{self.sev_model_name}','{self.sev_model_name}',{self.peril_id},{self.type_id},{self.source_id})
        returning "ID"
        """
        res = er_db.exec_stmt(self.conn, stmt)
        return res[0]

    def create(self):
        with er_db.get_db_conn(self.env) as self.conn:
            if self._event_set_exists():
                return (self.event_set_id, self.sev_model_id)
            self.event_type_id = self._get_event_type()
            self.peril_id = self._get_peril_id()
            self.source_id = self._get_source_id()
            self.version_id = self._get_version_id()

            self.unit_id = self._get_unit_id()
            self._get_xdef_id()
            self._map_xdef_and_unit()

            self.event_set_id = self._create_event_set()
            self.sev_model_id = self._create_event_sev_model()
            return (self.event_set_id, self.sev_model_id)
