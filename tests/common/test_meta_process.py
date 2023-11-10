"""Test Meta Process Methods"""

from io import StringIO
import os
import unittest
from datetime import  datetime, timedelta
import pandas as pd
import boto3
from moto import mock_s3

from xetra.common.meta_process import MetaProcess
from xetra.common.s3 import S3BucketConnector
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFIleException

class TestMetaProcessMethods(unittest.TestCase):
    """Tests for the Meta process class"""

    def setUp(self):
        """ Environment set up"""
        # mocking S3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        # Defining the class arguments for the S3Bucket COnnector
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_KEY_ID'
        self.s3_endpoint_url = 'https://s3.us-west-2.amazonaws.com'
        self.s3_bucket_name = 'test_bucket'


        # Creating S3 access keys as environment variables

        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'

        #creating  Bucket instance on mocked S3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                                CreateBucketConfiguration={
                                    'LocationConstraint' : 'us-west-2'
                              })
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

        #Creating a tetsing instance
        self.s3_bucket_meta = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)
        self.dates = [(datetime.today()-timedelta(days = day)) \
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(8)]

    def tearDown(self):
        # mocking S3 connection stop
        self.mock_s3.stop()


    def test_update_meta_file_no_meta_file(self):
        """ Test update_meta_file when there is no meta file"""
        #Expected Results
        date_list_exp = ['2023-11-08','2023-11-09']
        proc_date_list_exp = [datetime.today().date()] * 2

        #Test Init
        meta_key='meta.csv'

        #Method Execution
        MetaProcess.update_meta_file(date_list_exp,meta_key,self.s3_bucket_meta)
        #Read Meta File
        data=self.s3_bucket.Object(key=meta_key).get().getBody().read.decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]) \
                            .dt.date)

        #Tests after execution
        self.assertEqual(date_list_exp,date_list_result)
        self.assertEqual(proc_date_list_exp,proc_date_list_result)
        #Cleanup
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_empty_date_list(self):
        """Test update_meta_file method when date_list argument is empty"""
        #Expected Results
        return_exp = 'True'
        log_exp = 'The dataframe is empty. No file will be written to S3'
        #Test Init
        date_list_exp = []
        meta_key='meta.csv'
        #Method Execution
        with self.assertLogs() as logm:
            result = MetaProcess.update_meta_file(date_list_exp,meta_key,self.s3_bucket_meta)
            #Test log message after execution
            self.assertIn(log_exp,logm.output[1])
        #Test after method execution
        self.assertEqual(return_exp,result)

    def test_update_meta_file_ok(self):
        """Test update_meta_file method when there is a meta file already in the S3"""
        #Expected results
        date_list_old = ['2023-11-05','2023-11-06']
        date_list_new = ['2023-11-07','2023-11-08']
        date_list_exp = date_list_old + date_list_new
        proc_date_list_exp = [datetime.today().date()] * 4

        #Test Init
        meta_key = 'meta.csv'
        meta_content =  (
          f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
          f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
          f'{date_list_old[0]},'
          f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
          f'{date_list_old[1]},'
          f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
        )
        self.s3_bucket.put_object(Body = meta_content, Key=meta_key)

        #Method Execution
        MetaProcess.update_meta_file(date_list_new,meta_key,self.s3_bucket_meta)

        #Read Meta File
        data=self.s3_bucket.Object(key=meta_key).get().getBody().read.decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]) \
                            .dt.date)
        #Tests after execution
        self.assertEqual(date_list_exp,date_list_result)
        self.assertEqual(proc_date_list_exp,proc_date_list_result)
        #Cleanup
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_wrong_format(self):
        """Test update_meta_file method when meta file is in wrong format"""
        #Expected results
        date_list_old = ['2023-11-05','2023-11-06']
        date_list_new = ['2023-11-07','2023-11-08']

        #Test Init
        meta_key = 'meta.csv'
        meta_content =  (
          f'wrong_column,{MetaProcessFormat.META_PROCESS_COL.value}\n'
          f'{date_list_old[0]},'
          f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
          f'{date_list_old[1]},'
          f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
        )
        self.s3_bucket.put_object(Body = meta_content, Key=meta_key)

        #Method Execution
        with self.assertRaises(WrongMetaFIleException):
            MetaProcess.update_meta_file(date_list_new, meta_key, self.s3_bucket_meta)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )


    



if __name__ == "__main__":
    unittest.main()