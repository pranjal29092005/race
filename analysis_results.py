import os, sys, json
import uuid
from psycopg2.extras import RealDictCursor
import er_db


class AnalysisResults:
    def __init__(self, **kwargs):
        super().__init__()
        self.portfolio = True
        self.reins_measures = False
        akey = kwargs.get('analysis_key')
        if akey is None:
            self.analysis_key = str(uuid.uuid4())
        else:
            self.analysis_key = akey

    def get_measures(self, **kwargs):
        measures = ["TIV", "GroundUpLoss", "Latitude", "Longitude", "Asset Name", "Asset Number", "Country", "State"]
        with_event = kwargs.get("with_event")
        with_program = kwargs.get("with_program")
        if with_event:
            measures.extend(["Intensity", "DamageFactor"])
            if with_program is not None:
                measures.append("Retained Loss")
        if with_program is not None:
            measures.append("Retained Limit")
        if not self.portfolio:
            return measures
        measures.extend(["Contract Number", "ContractLoss_Value All Types_EL", "Retained Limit"])
        if with_event:
            measures.extend(["Retained Loss", "ContractLoss_Value All Types_GL"])
        if self.reins_measures:
            measures.extend(
                [
                    "Fac Exposed Limit",
                    "Net of Fac Exposed Limit",
                    "Treaty Exposed Limit",
                    "Net Pre Cat Exposed Limit",
                    "Net Post Cat Exposed Limit",
                    "Reinsurance Gross Exposed Limit",
                ]
            )
            if with_event:
                measures.extend(
                    [
                        "Fac Loss",
                        "Net of Fac Loss",
                        "Treaty Loss",
                        "Net Pre Cat Loss",
                        "Net Post Cat Loss",
                        "Reinsurance Gross Loss",
                    ]
                )
        return measures

    def get_contribution_attributes(self, with_event):
        attrs = ["TIV", "GroundUpLoss"]
        if with_event:
            attrs.append("Intensity")
        if not self.portfolio:
            return attrs
        attrs.append("ContractLoss_Value All Types_EL")
        if with_event:
            attrs.append("ContractLoss_Value All Types_GL")
        return attrs

    def script(self):
        return """
string errorMessage;
if (!StoreAnalysisResults("StoreAnalysisResults", 0, errorMessage)){
        SendErrorWithCode(errorMessage);
}
Send("Analysis Results Saved", 1);
        """.replace("\n", " ")

    def to_json(self, db_conn, **kwargs):
        exp_id = kwargs.get("exp_id")
        with_event = kwargs.get("has_event")
        with_program = kwargs.get("has_program")
        exp_name = None
        audit_id = None
        if exp_id is not None:
            cursor = db_conn.cursor(cursor_factory=RealDictCursor)
            if self.portfolio:
                query = f"select audit_id, portfolio_name as exp_name from race.m_portfolio where portfolio_id={exp_id}"
            else:
                query = f'select "AUDIT_ID" as audit_id, "SCHEDULE_NAME" as exp_name from race."M_ASSET_SCHEDULE" where "ID"={exp_id}'
            cursor.execute(query)
            row = cursor.fetchone()
            audit_id = row["audit_id"]
            exp_name = row["exp_name"]
        return {
            "Audit Id": audit_id,
            "Exposure Name": exp_name,
            "MeasuresAndAttributes": self.get_measures(with_event=with_event, with_program=with_program),
            "ContributionAttributes": self.get_contribution_attributes(with_event),
            "Analysis Key": self.analysis_key,
        }
