
"""
Connector and methods accessing S3
"""

import os
import boto3
import logging
import pandas as pd
from io import BytesIO, StringIO

from xetra.common.constraints import S3FileTypes

class S3BucketConnector():
    """
    Class for interacting with S3 buckets
    """

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """"
        Constructor for s3 bucket connector

        :param access key: access key for accessing s3
        :param secret_key: secret key for accessing s3
        :param endpoint_url: endpoing url to s3
        :param bucket: s3 bucket name
        """


        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id = os.environ[access_key],
                                     aws_secret_access_key = os.environ[secret_key])
        self._s3 = self.session.resource(service_name = 's3', endpoint_url = endpoint_url)
        self._bucket = self._s3.Bucket(bucket)


    def list_files_in_prefix(self, prefix: str):
        """
        listing all files matching prefix in s3 bucket

        :param prefix: prefix on s3 bucket that should be filtered

        returns: 
            files: list of all file names containing prefix in key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix = prefix)]
        return files

    def read_csv_to_df(self, key: str, decoding: str = 'utf-8', sep: str =','):
        """
        :param key: file name/key for converting file to csv
        :param decoding: decoding method of file
        :param delimiter: delimiter of csv file

        returns:
            df: returns converted dataframe of the 
        
        """
        self._logger("Reading file %s/%s/%s", self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key = key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df

    def write_df_to_s3(self, df: pd.DataFrame, key: str, file_format: str):
        """
        :param df: dataframe you want written to s3 bucket
        :param key: target key of the saved file
        :param file_format: format of the saved file
        
        """
        if df.empty:
            self._logger.info('The dataframe is empty! No file will be written.')
            return None
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index = False)
            # write to bucket you created in AWS)
            return self._put_object(out_buffer, key)
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            df.to_csv(out_buffer, index = False)
            return self._put_object(out_buffer, key)
        self._logger.info("File type %s is not supported to be written to s3", file_format)
        
    def _put_object(self, out_buffer: BytesIO or StringIO, key: str):
        self._logger.info("Writing file to %s/%s/%s", self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Boyd = out_buffer.getvalue(), Key = key)
        return True
            
    