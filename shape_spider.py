import os, sys, json, uuid
import er_db
import pandas as pd
import click
from icecream import ic
from analysis import Analysis


class ShapeSpider(Analysis):
    def __init__(self):
        super().__init__()
        self.script = """
string errorMessage;
if (!AsyncSpiderAnalysis("AsyncSpiderAnalysis", errorMessage)){
    SendErrorWithCode(errorMessage);
}
        """.replace("\n", " ")
        self.damages = None
        self.sep_radius = 1500
        self.peril = None
        self.peril_name = None
        self.sub_peril_name = None
        self.threshold_op = "GT"
        self.heat_map = False

    def get_resolution(self, db_conn):
        xres = 2048
        query = f'select "UL_LON","UL_LAT","LR_LON","LR_LAT" from race.m_portfolio where "ID"={self.exposure_id}'
        if self.portfolio:
            query = f'select "UL_LON","UL_LAT","LR_LON","LR_LAT" from race.m_portfolio where portfolio_id={self.exposure_id}'
        resp = er_db.exec_stmt(db_conn, query)
        ullat = resp["UL_LAT"]
        ullon = resp["UL_LON"]
        lrlat = resp["LR_LAT"]
        lrlon = resp["LR_LON"]
        yres = (((2048 * 360) / (lrlon - ullon)) * (ullat - lrlat)) / 180
        return (xres, yres)

    def to_json(self, db_conn):
        xres, yres = self.get_resolution(db_conn)
        self.add_filter("Cause Of Loss", "EQ", peril_info["sub_peril_code"])
        data = {
            "exp_id": self.exposure_id,
            "measure": self.measure,
            "results_key": self.get_results_key(),
            "threshold": self.threshold,
            "threshold_op": self.threshold_op,
            "sov_or_portfolio": "portfolio" if self.portfolio else "sov",
            "xres": xres,
            "yres": yres,
            "measures": ["TIV", "#Assets", "#Contracts"],
        }

        if self.peril_name is None:
            peril_info = self.get_peril_info(self.peril, db_conn)
            data["peril"]: peril_info["peril_name"],
            data["sub_peril"]: peril_info["sub_peril_name"],
        else:
            data['peril'] = self.peril_name
            data['sub_peril'] = self.sub_peril_name
        if self.damages is not None:
            dmg = []
            radii = []
            for r, d in sorted(self.damages.items()):
                dmg.append(d)
                radii.append(r)
            data["damages"] = dmg
            data["radii"] = radii

        if self.sep_radius:
            data["sep_radius"] = self.sep_radius

        if self.heat_map:
            data["heatmap_bucket"] = "er-race-results"
            data["heatmap_prefix"] = f"{self.results_key}/spider-heatmap"

        tmpl = self.env.get_template("shape_spider.j2").render(data).replace("\n", "")
        out = json.loads(tmpl)
        out = self.add_exposure_filters(out)
        out = {"Script": self.script, "AsyncSpiderAnalysis": out}
        return out
