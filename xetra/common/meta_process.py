"""
Methods for processing the meta file

"""
import collections
from datetime import datetime
import pandas as pd
from xetra.common.custom_exceptions import WrongFormatException
from xetra.common.s3 import S3BucketConnector
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFIleException


class MetaProcess():
    """ Class with Meta File Processing Methods"""

    @staticmethod
    def update_meta_file(extract_date_list: str,meta_key: str,s3_bucket_meta: S3BucketConnector):
        """Updating meta file with the dates processed from xetra and 
        today's date as the processing date"""

        #Create an empty dataframe with metafile column list
        df_new = pd.DataFrame(columns=[
            MetaProcessFormat.META_SOURCE_DATE_COL.value,
            MetaProcessFormat.META_PROCESS_COL.value
        ])

        #Update date column with extract_date_list and processed date with today's date
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        df_new[MetaProcessFormat.META_PROCESS_DATE_FORMAT.value] = \
            datetime.today().strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        
        try:
            # If old meta file exists, create a new df with the union of old and new
            df_old = s3_bucket_meta.read_csv_as_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFIleException
            df_all = pd.concat([df_old,df_new])
        
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            #If old meta file doesn't exists
            df_all = df_new

        #Writing to S3
        s3_bucket_meta.write_df_to_s3(df_all,meta_key,MetaProcessFormat.META_FILE_FORMAT.value)
        return True


        


    @staticmethod
    def return_date_list(self):
        pass

    