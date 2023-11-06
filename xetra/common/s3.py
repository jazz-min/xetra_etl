"""Connector and Methods to access AWS S3"""

import os
import logging
from io import StringIO
import pandas as pd

import boto3




class S3BucketConnector():
    """Class for interating with AWS S3"""

    def __init__(self,access_key: str, secret_key: str, endpoint_url: str,bucket: str):
        """
        Constructor for S3BucketConnector
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id = os.environ[access_key],
                                     aws_secret_access_key = os.environ[secret_key])
        self._s3 = self.session.resource(service_name='s3',endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """
        Get the list of files with the given prefix from the S3 
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files


    def read_csv_as_df(self,key: str, encoding: str = 'utf-8', sep: str = ','):
        """ Read CSV file from S3 and return a dataframe"""

        self._logger.info('Reading file %s/%s/%s',self.endpoint_url,self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        dataframe = pd.read_csv(data,sep=sep)
        return dataframe




    def write_df_to_s3(self):
        pass
