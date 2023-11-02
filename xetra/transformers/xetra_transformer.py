""" Xetra ETL Component """


from typing import NamedTuple

from xetra.common.s3 import S3BucketConnector
import logging

class XetraSourceConfig(NamedTuple):
    """ Class for source configuration Data """

    src_first_extract_date : str
    src_columns : list
    src_col_date : str
    src_col_isin : str
    src_col_time: str
    src_col_start_price : str
    src_col_min_price : str
    src_col_max_price : str
    src_col_traded_vol : str


class XetraTargetConfig(NamedTuple):
    """ Class for target configuration Data"""


    trg_col_isin : str
    trg_col_date : str
    trg_col_open_price : str
    trg_col_close_price : str
    trg_col_min_price : str
    trg_col_max_price : str
    trg_col_daily_traded_vol : str
    trg_col_ch_prev_close : str
    trg_key : str
    trg_key_date_format : str
    trg_format : str


class XetraETL():
    """Reads the data from source, tranforms and loads to the target"""

     def __init__(self, s3_bucket_src: S3BucketConnector,
                 s3_bucket_trg: S3BucketConnector, meta_key: str,
                 src_args: XetraSourceConfig, trg_args: XetraTargetConfig):
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date =
        self.extract_date_list =
        self.meta_update_list = 
    
    def extract(self):
        pass

    def transform_report1(self):
        pass
    
    def load(self):
        pass
    
    def etl_report1(self):
        pass