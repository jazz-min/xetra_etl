""" Test S3BucketConnector Methods"""

import os

import unittest
from io import StringIO,BytesIO
import pandas as pd



import boto3

from moto import mock_s3


from xetra.common.s3 import S3BucketConnector

class TestS3BucketConnector(unittest.TestCase):
    """ Testing S3BucketConnector class methods"""

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
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)


    def tearDown(self):
        """ Run after the tests"""
        # mocking S3 connection stop
        self.mock_s3.stop()



    def test_list_files_in_prefix_all_ok(self):
        """ Test list_files_in_prefix method  - get file keys listed on the mocked S3 bucket"""        
        # Expected Results
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'

        #Test init
        csv_content = """col1,col2
        val1,val2"""
        self.s3_bucket.put_object(Body=csv_content,Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content,Key=key2_exp)
        #Method Execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)

        #Tests after method execution
        self.assertEqual(len(list_result),2)
        self.assertIn(key1_exp,list_result)
        self.assertIn(key2_exp,list_result)

        #Cleanup
        self.s3_bucket.delete_objects(
            Delete = {
                'Objects': [
                    {
                        'Key' : key1_exp
                    },
                    {
                        'Key' : key2_exp
                    }
                ]
            }
        )



    def test_list_files_in_prefix_wrong_prefix(self):
        """Test list_files_in_prefix method  -  wrong or not existing prefix"""
        # Expected Results
        prefix_exp = 'no-prefix/'
        #Method Execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        #Tests after method execution
        self.assertTrue(not list_result)

    def test_read_csv_as_df_ok(self):
        """ Test read_csv_as_df method to read one csv file from mocked S3 bucket"""

        #Expected Results
        key_exp = 'test.csv'
        col1_exp = 'col1'
        col2_exp = 'col2'
        val1_exp = 'val1'
        val2_exp = 'val2'
        log_exp =f'Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'

        #Test init
        csv_content = f'{col1_exp},{col2_exp}\n{val1_exp},{val2_exp}'
        self.s3_bucket.put_object(Body=csv_content, Key=key_exp)

        #Method Execution
        with self.assertLogs() as logm:
            df_result = self.s3_bucket_conn.read_csv_as_df(key_exp)
            self.assertIn(log_exp,logm.output[0])
        #Test after method execution
        self.assertEqual(df_result.shape[0], 1)
        self.assertEqual(df_result.shape[1], 2)
        self.assertEqual(val1_exp, df_result[col1_exp][0])
        self.assertEqual(val2_exp, df_result[col2_exp][0])
        #Cleanup
        self.s3_bucket.delete_objects(
            Delete = {
                'Objects': [
                    {
                        'Key' : key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_empty(self):
        """Test write_df_to_s3 method with empty dataframe as input"""
        #Expected Results
        return_exp = None
        log_exp = "The dataframe is empty. No file will be written to S3"

        #Test Init
        df_empty = pd.DataFrame()
        key='test.csv'
        file_format='csv'

        #method Execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_empty,key,file_format)
            # Log message test after method execution
            self.assertIn(log_exp,logm.output[0])
        #Test after methodexecution
        self.assertEqual(return_exp,result)

    def test_write_df_to_s3_csv(self):
        """Test write_df_to_s3 method with csv as input"""
        #Expected Results
        return_exp='True'
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns = ['col1', 'col2'])
        key_exp='test.csv'
        log_exp=f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        #Test init
        file_format='csv'
        #method Execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp,key_exp,file_format)
            # Log message test after method execution
            self.assertIn(log_exp,logm.output[0])
        #Test after methodexecution
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)
        self.assertEqual(return_exp,result)
        self.assertTrue(df_exp.equals(df_result))
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_parquet(self):
        """Test write_df_to_s3 method with parquet as input"""
        #Expected Results
        return_exp='True'
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns = ['col1', 'col2'])
        key_exp='test.parquet'
        log_exp=f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        #Test init
        file_format='parquet'
        #method Execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp,key_exp,file_format)
            # Log message test after method execution
            self.assertIn(log_exp,logm.output[0])
        #Test after methodexecution
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertEqual(return_exp,result)
        self.assertTrue(df_exp.equals(df_result))
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )







if __name__ == "__main__":
    unittest.main()
