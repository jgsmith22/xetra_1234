"""
File to store constraints
"""

from enum import Enum   

class S3FileTypes(Enum):
    """
    supported files for the S3 Bucket Connector
    """

    CSV = 'csv'
    PARQUET = 'parquet'


class MetaProcessFormart(Enum):
    """
    formation for Metaprocess class
    """

    META_DATE_FORMAT = '%Y-%m-%d'
    META_PROCESS_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    META_SOURCE_DATE_COL = 'source_date'
    META_PROCESS_COL = 'datetime_of_processing'
    META_FILE_FORMAT = 'csv'


