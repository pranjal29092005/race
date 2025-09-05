import os, sys, json
import boto3, botocore
import pandas as pd
import er_utils
import click
from icecream import ic


class AwsSession(object):
    def __init__(self, access_key, secret_key):
        self.session = boto3.session.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    def list_files_from_s3(self, **kwargs):
        s3 = self.session.client('s3')
        r = s3.list_objects_v2(**kwargs)
        return_code = r['ResponseMetadata']['HTTPStatusCode']
        files = []
        try:
            while return_code == 200:
                files.extend(r['Contents'])
                cont_token = r['NextContinuationToken']
                r = s3.list_objects_v2(**kwargs, ContinuationToken=cont_token)
                return_code = r['ResponseMetadata']['HTTPStatusCode']
        except KeyError as e:
            pass

        if not files:
            return None

        dt_format = '%Y-%b-%d %H:%M:%S'
        df = pd.DataFrame(files)
        df['LastModified'] = pd.to_datetime(df['LastModified'])
        df = df.sort_values(by='LastModified', ascending=False)

        lm = pd.to_datetime(df['LastModified'])
        df[er_utils.lm_utc] = lm.dt.strftime(dt_format)
        df[er_utils.lm_ist] = lm.dt.tz_convert(tz='Asia/Calcutta').dt.strftime(dt_format)

        return df[['Key', 'LastModified', er_utils.lm_utc, er_utils.lm_ist, 'Size']]

    def download_file(self, bucket, key, **kwargs):
        out_name = kwargs.get('out')
        out_name = out_name if out_name else key
        s3 = self.session.client('s3')
        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            ic(e)
            return False

        with open(out_name, 'wb') as f:
            s3.download_fileobj(bucket, key, f)
        return True

    def read_json(self, bucket, key):
        s3 = self.session.client('s3')
        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
            if (resp['ResponseMetadata']['HTTPStatusCode'] != 200):
                return None
            return json.load(resp['Body'])
        except Exception as e:
            click.secho(f'Exception: {e}', fg='red', bold=True)
            return None

    def upload_file(self, bucket, key, **kwargs):
        s3 = self.session.client('s3')
        file = kwargs.get('file')
        if file is None:
            return
        if not os.path.exists(file):
            return
        metadata = kwargs.get('metadata')
        if (isinstance(metadata, dict)):
            s3.upload_file(file, bucket, key, ExtraArgs={'Metadata': metadata})
        else:
            s3.upload_file(file, bucket, key)

    def key_exists(self, bucket, key):
        s3 = self.session.client('s3')
        try:
            resp = s3.head_object(Bucket=bucket, Key=key)
            return ic(resp['ResponseMetadata']['HTTPStatusCode']) == 200
        except Exception:
            return False
