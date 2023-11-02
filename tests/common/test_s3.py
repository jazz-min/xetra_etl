""" Test S3BucketConnector Methods"""

import os

import unittest

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



    def list_files_in_prefix_all_ok(self):
        """ Test list_files_in_prefix method  - get file keys listed on the mocked S3 bucket"""
        pass

    def list_files_in_prefix_wrong_prefix(self):
        """Test list_files_in_prefix method  -  wrong or not existing prefix"""
        pass

if __name__ == "__main__":
    unittest.main()