import os
import subprocess
import boto3,time
import msgpack
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2.extras import RealDictCursor
from db import get_db_connection, run_query 

from binfile import read_arrays

# AWS Credentials via ENV
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION,
)

s3 = session.client("s3")

def get_portfolio_md5(portfolio_id: int, env="integration"):
    conn = get_db_connection(env)
    query = f"""
        SELECT * 
        FROM race.m_portfolio 
        WHERE portfolio_id = {portfolio_id}
    """
    result = run_query(conn, query, cursor_factory=RealDictCursor)
    # print(f"Query Result: {result}")
    conn.close()
    if result:
        # print(f"result: {result}")
        s3_folder_md5 = result['s3_folder_md5']
        audit_id = result['audit_id']
    return s3_folder_md5,audit_id

def get_contract_details(audit_id: str, env="integration"):
    conn = get_db_connection(env)
    query = f"""
        SELECT * 
        FROM f_contract_{audit_id}
    """
    result = run_query(conn, query, cursor_factory=RealDictCursor)
    # print(f"Results contract: {result}")
    conn.close()
    if result:
        contract_number = result['contract_number']
    return contract_number

def construct_s3_path(md5: str, audit_id: str, env="integration"):
    bucket_lookup = {
        "prod": "prod-p-b-er",
        "alpha": "alpha-p-b-er",
        "uat": "uat-p-b-er",
        "integration": "nonprod-u-b-er",
    }
    print(f"Using environment: {env}")
    bucket = bucket_lookup.get(env, "nonprod-u-b-er")
    # bucket = "nonprod-u-b-er" 
    path_of_file = f"s3://{bucket}/{md5}/{audit_id}_0.7z"
    print(f"Constructed S3 path: {path_of_file}")
    return path_of_file

def download_from_s3(s3_path: str, local_path: str):
    parts = s3_path.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    key = parts[1]

    print(f" Downloading from S3: {s3_path}")
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(bucket, key, local_path)
        print(f" Downloaded to {local_path}")
    except Exception as e:
        print(f" Failed to download: {e}")
        raise

def extract_7z_file(file_path: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    print(f" Extracting {file_path} to {output_dir}")
    subprocess.run(
        ["7z", "x", file_path, f"-o{output_dir}", '-y'],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    print(f" Extracted to {output_dir}")

def timed(label: str):
    def decorator(func):
        def wrapper(*args, **kwargs):
            print(f"\n Starting: {label}")
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            print(f" Completed: {label} in {end - start:.2f} sec")
            return result
        return wrapper
    return decorator

@timed("Reading .bin column as PyArrow array")
def read_bin_column(file_path) -> pa.Array:
    with open(file_path, 'rb') as f:
        f.read(32)
        try:
            data = msgpack.unpack(f, raw=False)
            return pa.array(data)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return pa.nulls(0)

def read_parquet_file(file_path: str) -> pa.Table:
    print(f"📖 Reading Parquet file: {file_path}")
    schema = pq.read_schema(file_path)
    print(f"Schema: {schema}")
    table = pq.read_table(file_path)
    print(table.slice(0, 5))
    return table

@timed("Merging and writing Parquet")
def merge_bin_columns_to_parquet_arrow(folder_path: str, output_parquet_path: str):
    # Support both uppercase and lowercase file names
    base_names = [("m", "asset_name"), ("m", "asset_number"), ("m", "latitude"), ("m", "longitude"),("m","asset_schedule_id")]
    file_map = {}
    for name in base_names:
        if isinstance(name, tuple):
            prefix, column_name = name
            # print(f"prefix: {prefix} | column name: {column_name}")
        else:
            prefix = "m"
            column_name = name

        if column_name == "AssetLevelContractRowNum":
            file_map[column_name] = f"{prefix.upper()}_AssetLevelContractRowNum.bin"
        else:
            # print(f"column name: {column_name}")
            lower = f"{prefix}_{column_name}.bin"
            upper = f"{str(prefix).upper()}_{str(column_name).upper()}.bin"
            # print(f"upper: {upper} | lower: {lower}")
            lower_path = os.path.join(folder_path, lower)
            upper_path = os.path.join(folder_path, upper)
            if os.path.exists(lower_path):
                file_map[column_name] = lower
            elif os.path.exists(upper_path):
                file_map[column_name] = upper
            else:
                raise FileNotFoundError(f"Neither {lower} nor {upper} found in {folder_path}")

    columns = {}
    for column_name, file_name in file_map.items():
        path = os.path.join(folder_path, file_name)
        # print(f"Reading {column_name} from {file_name}")
        col = read_bin_column(path)
        # print(f"  ➕ Length: {len(col)} | Type: {col.type}")
        columns[column_name] = col

    table = pa.table(columns)

    print(f"\n Writing to Parquet: {output_parquet_path}")
    pq.write_table(table, output_parquet_path)


@timed("Merging and writing Parquet")
def merge_bin_columns_to_parquet_arrow_old_version(folder_path: str, output_parquet_path: str):
    # file_map = {
    #     "asset_name": "M_ASSET_NAME.bin",
    #     "asset_number": "M_ASSET_NUMBER.bin",
    #     "assetLevelContractRowNum": "M_AssetLevelContractRowNum.bin",
    #     "latitude": "M_LATITUDE.bin",
    #     "longitude": "M_LONGITUDE.bin"
    # }

    # columns = {}
    # for column_name, file_name in file_map.items():
    #     path = os.path.join(folder_path, file_name)
    #     print(f"Reading {column_name} from {file_name} and path {path}")

    #     # Using BinFile to read the bin file as pyarrow array
    #     bin_file = read_dictionary(path)

    #     print(f" Length: {len(bin_file)} | Type: {bin_file.type}")
    #     columns[column_name] = bin_file

    # # Creating pyarrow table with the collected columns
    # table = pa.table(columns)

    asset_key_files = {
    "asset_name":  "M_ASSET_NAME.bin",
    "asset_number": "M_ASSET_NUMBER.bin",
    "latitude": "M_LATITUDE.bin",
    "longitude": "M_LONGITUDE.bin",
    # "AssetLevelContractRowNum": "M_AssetLevelContractRowNum.bin"
    }
    contract_key_files = {
        "contractRowNum": "data_f_contract_40051_0_ContractRowNum.bin",
        "contractNumber": "data_f_contract_40051_0_Contract_Number.bin"
    }
    # Now use read_arrays to read the .bin files
    assest_table = read_arrays(folder_path, asset_key_files)
    print(f"Length of asset_table: {len(assest_table)}")
    print(f"Table schema: {assest_table.schema}")
    print(f"\n Writing to Parquet: {output_parquet_path}")
    pq.write_table(assest_table, output_parquet_path)
    # contract_table = read_arrays(folder_path, contract_key_files,read_header=False)
    # print(f"Length for contract: {len(contract_table)}")
    
    # print(f"Table schema: {contract_table.schema}")

    # import pyarrow.parquet as pq
    # pq.write_table(contract_table, "contract_data.parquet")

    # read_parquet_file("contract_data.parquet")

    # Create the lookup mapping
    # result_table = assest_table.join(
    #     contract_table,
    #     keys='AssetLevelContractRowNum',
    #     right_keys='contractRowNum', 
    #     join_type='left outer' 
    # )

    # # Check the mapping results
    # print(f"Assets processed: {len(result_table)}")
    # print(f"Merged Table Schema: {result_table.schema}")

    # pq.write_table(result_table, "merged_assets_and_contracts.parquet")

    # read_parquet_file("merged_assets_and_contracts.parquet")

def run_pipeline(portfolio_id: int, working_env="prod"):
    print(f"\n Starting pipeline for portfolio_id={portfolio_id}, env={working_env}")

    md5, audit_id = get_portfolio_md5(portfolio_id, working_env)
    contract_number = get_contract_details(audit_id, working_env)
    print(f" Portfolio MD5: {md5}, Audit ID: {audit_id}, Contract Number: {contract_number}")
    s3_path = construct_s3_path(md5, audit_id, working_env)

    local_file = f"./downloads/{audit_id}_0.7z"
    extract_dir = f"./extracted/{portfolio_id}"
    output_parquet_path = os.path.abspath(f"./output/{portfolio_id}_merged.parquet")

    os.makedirs("./downloads", exist_ok=True)
    os.makedirs("./extracted", exist_ok=True)
    os.makedirs("./output", exist_ok=True)

    download_from_s3(s3_path, local_file)
    extract_7z_file(local_file, extract_dir)

    merge_bin_columns_to_parquet_arrow(extract_dir, output_parquet_path)

    return output_parquet_path,contract_number

# if __name__ == "__main__":
#     path = run_pipeline(portfolio_id=2875, env="integration")
#     print(f"\n Final Parquet File Path:\n{path}")
