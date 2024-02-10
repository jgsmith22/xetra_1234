"""
Methods for processing meta files
"""
import pandas as pd
from datetime import datetime, timedelta
from xetra.common.constraints import MetaProcessFormart

class MetaProcess:

    """
    Class for working with meta files
    """
    
    @staticmethod
    def update_meta_file(self, extract_date_list: list, meta_key: str, s3_bucket_meta: S3BucketConnector):
        df_new = pd.DataFrame(columns = [
            MetaProcessFormart.META_SOURCE_DATE_COL.value,
            MetaProcessFormart.META_PROCESS_COL.value
        ])
        df_new[MetaProcessFormart.META_SOURCE_DATE_COL.value] = extract_date_list
        df_new[MetaProcessFormart.META_PROCESS_COL.value] = datetime.today().strftime(MetaProcessFormart.META_PROCESS_DATE_FORMAT.value)
        


    @staticmethod
    def return_date_list(self):
        pass



