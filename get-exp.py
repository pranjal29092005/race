import os, sys, argparse, glob, json, subprocess
import er_db, er_utils, er_aws
from icecream import ic
import click
import hashlib
import pandas as pd
from psycopg2.extras import RealDictCursor
import shutil


class Exposure:
    def __init__(self, **kwargs):
        self.audit_id = kwargs.get("audit_id")
        self.exp_id = kwargs.get("exp_id")
        self.exp_name = kwargs.get("exp_name")
        self.portfolio = kwargs.get("portfolio")
        self.get_binaries = kwargs.get("bin")
        self.get_etf = kwargs.get("etf")
        self.get_src = kwargs.get("src")
        self.db_conn = kwargs.get("db_conn")
        self.sch_id = 0 if self.portfolio else self.exp_id
        self.s3_session = er_aws.AwsSession(os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"])
        self.env = kwargs.get("env")
        self.contract_count = 0
        self.valuation_count = 0
        self.asset_count = 0
        self.user_id = None
        self.user_details = None
        self.coverage_config_exists = False
        self.set_asset_and_contract_count()
        self.get_user_details()
        self.get_coverage_info()

    def get_coverage_info(self):
        bucket = os.environ["ASSET_S3_BUCKET"]
        prefix = (hashlib.md5(bytes(f"{self.audit_id}_{self.sch_id}", "utf-8")).hexdigest().upper())
        key = f"{prefix}/coverages/coverage_config.json"
        session = er_aws.AwsSession(os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"])
        self.coverage_config_exists = session.key_exists(bucket, key)

    def get_user_details(self):
        query = f"select tenant_id, email from v_m_user where user_id={self.user_id}"
        cursor = self.db_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        row = cursor.fetchone()
        self.user_details = {"Tenant Id": row["tenant_id"], "Email": row["email"]}

    def set_asset_and_contract_count(self):
        if self.portfolio:
            query = f"select contract_count, user_id, asset_count, valuation_count from race.m_portfolio where audit_id={self.audit_id}"
        else:
            query = f'select 0 as contract_count, "USER_ID" as user_id, "ASSET_COUNT" as asset_count, "VALUATION_COUNT" as valuation_count from race."M_ASSET_SCHEDULE" where "AUDIT_ID"={self.audit_id}'
        cursor = self.db_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        row = cursor.fetchone()
        if row:
            self.contract_count = row["contract_count"]
            self.asset_count = row["asset_count"]
            self.valuation_count = row["valuation_count"]
            self.user_id = row["user_id"]

    def get_s3_path(self):
        bucket = os.environ.get("ASSET_S3_BUCKET")
        prefix = (hashlib.md5(bytes(f"{self.audit_id}_{self.sch_id}", "utf-8")).hexdigest().upper())
        return f"s3://{bucket}/{prefix}/"

    def create_input_json(self, dest_dir):
        aid_sid = f"{self.audit_id}_{self.sch_id}"
        src_dir = os.path.join(dest_dir, aid_sid)
        txt_files = glob.glob(os.path.join(src_dir, "*.txt"))
        input_json = os.path.join(src_dir, "input.json")
        config = {
            "Exposure": {
                "Audit Id": self.audit_id,
                "Is Portfolio": self.portfolio,
                "Schedule Id": self.sch_id,
            },
            "Config": {
                "Output Directory": os.path.join(src_dir, "output")
            },
            "Valuation Binaries":
                {
                    "F Asset":
                        {
                            "Input File": os.path.join(src_dir, f"f_asset_{aid_sid}.txt"),
                            "Order File": os.path.join(src_dir, "f_asset.txt"),
                        },
                    "M Asset":
                        {
                            "Input File": os.path.join(src_dir, f"m_asset_{aid_sid}.txt"),
                            "Order File": os.path.join(src_dir, "m_asset.txt"),
                        },
                },
        }
        if self.portfolio:
            pc_config = {
                "Coverage Binaries":
                    {
                        "Column Order File": os.path.join(src_dir, "f_coverage.txt"),
                        "Input File": os.path.join(src_dir, f"data_f_coverage_{aid_sid}.txt"),
                    },
                "Contract Binaries":
                    {
                        "Input File": os.path.join(src_dir, f"data_f_contract_{aid_sid}.txt"),
                        "Mapped Columns File": os.path.join(src_dir, "f_contract.txt"),
                    },
                "Asset Schedule Binaries": {
                    "Input File": os.path.join(src_dir, f"data_m_asset_schedule_{aid_sid}.txt")
                },
            }
            layer_files = set(filter(lambda s: "_layer_" in s, txt_files))
            if layer_files:
                pc_config.update(
                    {
                        "Layer Binaries":
                            {
                                "Input File": os.path.join(src_dir, f"data_f_layer_{aid_sid}.txt"),
                                "Mapped Columns File": os.path.join(src_dir, "f_layer.txt"),
                            }
                    }
                )
            reins_files = set(filter(lambda s: "r_info" in s, txt_files))
            if reins_files:
                pc_config.update(
                    {
                        "Reinsurance Binaries":
                            {
                                "Info":
                                    {
                                        "Input File": os.path.join(src_dir, f"data_r_info_{aid_sid}.txt"),
                                        "Order File": os.path.join(src_dir, "r_info.txt"),
                                    },
                                "Scope":
                                    {
                                        "Input File": os.path.join(src_dir, f"data_r_scope_{aid_sid}.txt"),
                                        "Order File": os.path.join(src_dir, "r_scope.txt"),
                                    },
                            }
                    }
                )
            config.update(pc_config)
        with open(input_json, "w") as f:
            json.dump(config, f, indent=4)
        print(f"Input json {input_json} created")

    def print_details(self):
        self.counts = f"Assets: {self.asset_count}, Contracts: {self.contract_count}, Valuations: {self.valuation_count}"
        self.ids = f"Exposure Id: {self.exp_id}, Audit Id: {self.audit_id}"
        self.user = f'Tenant Id: {self.user_details["Tenant Id"]}, Email: {self.user_details["Email"]}'
        keys = ["exp_name", "ids", "user", "counts"]
        out = []
        for k in keys:
            out.append({"Key": k, "Value": str(getattr(self, k))})
        out.append({"Key": "S3Path", "Value": self.get_s3_path()})
        out.append({"Key": "New coverage configuration", "Value": self.coverage_config_exists})
        df = pd.DataFrame(out)
        er_utils.tabulate_df(df)

    def get_bin_sources(self):
        out = f"{self.audit_id}_{self.sch_id}_src.7z"
        s3_path = f"{self.get_s3_path()}{out}"
        session = er_aws.AwsSession(os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"])
        path_components = s3_path.replace("s3://", "").split("/")
        status = session.download_file(path_components[0], "/".join(path_components[1:]), out=out)
        if not status:
            click.secho("Unable to fetch sources", fg="red")
            return
        dest_dir = os.environ.get("RACE_BIN_SOURCES_DIR")
        if dest_dir is None:
            click.secho("Not extracting src file", fg="red")
            return

        er_utils.extract_archive(out, os.path.join(dest_dir, f"{self.audit_id}_{self.sch_id}"))
        os.remove(out)

        self.create_input_json(dest_dir)

    def get_etf_file_list(self, bucket):
        prefix = er_db.get_file_path(self.db_conn, self.audit_id)
        audit_trail = er_db.get_audit_trail(self.db_conn, self.audit_id)
        if not prefix:
            return
        file_list = self.s3_session.list_files_from_s3(Bucket=bucket, Prefix=prefix)
        if file_list is None:
            for ad in audit_trail:
                prefix = er_db.get_file_path(self.db_conn, ad)
                if not prefix:
                    return None
                file_list = self.s3_session.list_files_from_s3(prefix)
                if file_list is not None:
                    break
        return file_list

    def get_etf_file(self):
        bucket = os.environ.get("IMPORT_S3_BUCKET")
        file_list = self.get_etf_file_list(bucket)
        etfs = (file_list[file_list["Key"].str.contains("\\.etf$")].copy().reset_index().drop("index", axis=1))
        etfs["Item"] = etfs.index + 1
        cols = ["Item", "Key", er_utils.lm_utc, er_utils.lm_ist]
        file = None
        if len(etfs) == 1:
            er_utils.tabulate_df(etfs[cols])
            file = etfs.iloc[0]["Key"]
        else:
            er_utils.tabulate_df(etfs[cols])
            item_num = input(
                f"\nThere are multiple items matching your query. Please specify the file you want to download. (1 - {len(etfs)})"
            )
            item_num = int(item_num)
            file = etfs.loc[etfs.Item == item_num]["Key"].iloc[0]
        out_file = f"{self.audit_id}_{self.sch_id}_{self.env}.etf"
        ic(out_file)
        self.s3_session.download_file(bucket, file, out=out_file)

    def get_bin_dir(self):
        parent = self.get_bin_dest_dir()
        return os.path.join(parent, f'{self.audit_id}_{self.sch_id}')

    def get_bin_dest_dir(self):
        master_folder = os.environ.get("RACE_MASTER_FOLDER")
        if master_folder is None:
            return os.getcwd()
        cur_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        race_cfg_file = os.path.join(cur_dir, "Race.cfg")
        with open(race_cfg_file, "r") as f:
            cfg = json.load(f)
            return os.path.join(master_folder, cfg["AssetScheduleDataFolder"], self.env)
        return os.getcwd()

    def change_bin_file_case(self, out_dir):
        files = glob.glob(os.path.join(out_dir, "*"))
        for f in files:
            base = os.path.basename(f)
            if base.endswith("parquet"):
                continue
            os.rename(f, os.path.join(out_dir, base.lower()))
        with open(os.path.join(out_dir, "done.txt"), "w") as f:
            pass

    def fetch_valuation_parquet_files(self, bucket, prefix, out_dir):
        key = f"{prefix}/valuation.json"
        valuation_config = self.s3_session.read_json(bucket, key)
        if valuation_config is None:
            return
        perils = valuation_config["Cause Of Loss"]
        for p in perils:
            key = f"{prefix}/valuation_{p}.parquet"
            print(f"Fetching {os.path.basename(key)}")
            self.s3_session.download_file(bucket, key, out=os.path.join(out_dir, os.path.basename(key)))

    def fetch_reinsurance_parquet_files(self, bucket, prefix, out_dir):
        key = f"{prefix}/reinsurance.json"
        reins_config = self.s3_session.read_json(bucket, key)
        if reins_config is None:
            return
        for k, _ in reins_config["Checksum"].items():
            key = f"{prefix}/{k}"
            print(f"Fetching {os.path.basename(key)}")
            self.s3_session.download_file(bucket, key, out=os.path.join(out_dir, os.path.basename(key)))

    def fetch_coverage_parquet_files(self, bucket, prefix, out_dir):
        key = f"{prefix}/coverages/coverage_config.json"
        cvg_config = self.s3_session.read_json(bucket, key)
        if cvg_config is None:
            return
        coverages_dir = os.path.join(out_dir, "coverages")
        os.makedirs(coverages_dir, exist_ok=True)
        try:
            for k in cvg_config["Terms"].keys():
                key = f"{prefix}/coverages/{k}"
                print(f"Fetching {k}")
                self.s3_session.download_file(bucket, key, out=os.path.join(coverages_dir, k))
        except KeyError:
            pass

        try:
            cvg_config["Conditions"]["coverage_conditions.parquet"]
            key = f"{prefix}/coverages/coverage_conditions.parquet"
            print(f"Fetching coverage_conditions.parquet")
            self.s3_session.download_file(bucket, key, out=os.path.join(coverages_dir, k))
        except (KeyError, TypeError):
            pass

    def fetch_layer_parquet_files(self, bucket, prefix, out_dir):
        key = f"{prefix}/layers/layer_config.json"
        lyr_config = self.s3_session.read_json(bucket, key)
        if lyr_config is None:
            return
        layers_dir = os.path.join(out_dir, "layers")
        os.makedirs(layers_dir, exist_ok=True)
        try:
            for k in lyr_config["Layers"].keys():
                key = f"{prefix}/layers/{k}"
                print(f"Fetching {k}")
                self.s3_session.download_file(bucket, key, out=os.path.join(layers_dir, k))
        except KeyError:
            pass

    def get_exp_binaries(self):
        bucket = os.environ.get("ASSET_S3_BUCKET")
        dest_folder = self.get_bin_dest_dir()
        s3_path = self.get_s3_path()
        components = (f"{s3_path}/{self.audit_id}_{self.sch_id}.7z".replace("s3://", "").replace("//", "/").split("/"))
        bucket = components[0]
        prefix = components[1]
        ic(prefix)
        key = "/".join(components[1:])
        out_dir = os.path.join(dest_folder, f"{self.audit_id}_{self.sch_id}")
        out_file = os.path.join(dest_folder, components[-1])
        os.makedirs(out_dir, exist_ok=True)
        aws_sync_cmd = (f"aws s3 sync --only-show-errors s3://{bucket}/{prefix} {out_dir}")
        ic(aws_sync_cmd)
        subprocess.call(aws_sync_cmd.split())
        # self.s3_session.download_file(bucket, key, out=out_file)
        bin_zip_file = os.path.join(out_dir, f"{self.audit_id}_{self.sch_id}.7z")
        shutil.copy(bin_zip_file, dest_folder)

        click.secho(f"Extracting archive to {out_dir} ...", fg="blue")
        er_utils.extract_archive(out_file, out_dir)
        self.change_bin_file_case(out_dir)
        return

        # aws_sync_cmd = f"aws s3 sync {s3_path} {out_dir}".split()
        # aws_sync_cmd.extend(["--include", "*.parquet", "--only-show-errors"])
        # subprocess.call(aws_sync_cmd)

        # click.secho('Fetching valuation parquet files', fg='blue')
        # self.fetch_valuation_parquet_files(bucket, prefix, out_dir)

        # click.secho('Fetching reinsurance parquet files', fg='blue')
        # self.fetch_reinsurance_parquet_files(bucket, prefix, out_dir)

        # click.secho('Fetching coverage parquet files', fg='blue')
        # self.fetch_coverage_parquet_files(bucket, prefix, out_dir)

        # click.secho('Fetching layer parquet files', fg='blue')
        # self.fetch_layer_parquet_files(bucket, prefix, out_dir)

    def execute(self):
        self.print_details()
        if self.get_src:
            self.get_bin_sources()
        if self.get_binaries:
            self.get_exp_binaries()
        if self.get_etf:
            self.get_etf_file()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="get-exp")
    parser.add_argument("--env", "-e", dest="env", required=True, choices=["prod", "integration", "alpha"])
    parser.add_argument("--audit-id", "-a", dest="audit_id", type=int, required=False)
    parser.add_argument("--id", "-i", dest="id", type=int, required=False)
    parser.add_argument("--portfolio", "-p", dest="portfolio", action="store_true", required=False)
    parser.add_argument("-etf", dest="get_etf", action="store_true", required=False)
    parser.add_argument("-bin", dest="get_binaries", action="store_true", required=False)
    parser.add_argument("-src", dest="get_bin_src", action="store_true", required=False)

    argv = parser.parse_args()
    audit_id = None
    exp_id = None
    exp_name = None
    db_info = er_db.get_db_info(argv.env)
    with er_db.get_db_conn(argv.env) as conn:
        if argv.audit_id:
            audit_id = argv.audit_id
            exp_id = er_db.get_exposure_id(conn, argv.audit_id, argv.portfolio)
        elif argv.id:
            audit_id, _ = er_db.get_audit_id_v2(conn, argv.id, argv.portfolio)
            exp_id = argv.id

        if not audit_id or not exp_id:
            click.secho("No audit or exposure id specified", bold=True, fg="red")
            sys.exit(-1)

        exp_name = er_db.get_exp_name(conn, exp_id, argv.portfolio)
        exp = Exposure(
            audit_id=audit_id,
            exp_id=exp_id,
            exp_name=exp_name,
            db_conn=conn,
            portfolio=argv.portfolio,
            etf=argv.get_etf,
            bin=argv.get_binaries,
            env=argv.env,
            src=argv.get_bin_src,
        )
        exp.execute()
