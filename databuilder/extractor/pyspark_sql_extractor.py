# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
import logging

from databuilder.extractor.base_extractor import Extractor
from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from pyspark.sql import SparkSession
from typing import Iterator, Union, List, Dict, Optional, Any  # noqa: F401
import concurrent.futures

LOGGER = logging.getLogger(__name__)

class SparkSQLExtractor(Extractor):
    """
    Run Spark SQL commands and extract output
    This requires a spark session to run that has a hive metastore populated with all of the delta tables
    that you are interested in.
    """
    # CONFIG KEYS
    DATABASE_KEY = "database"
    CLUSTER_KEY = "cluster"
    DEFAULT_CONFIG = ConfigFactory.from_dict({DATABASE_KEY: "spark_sql"})
    MODEL_CLASS = "model_class"
    QUERIES = 'query_queue'

    def init(self, conf: ConfigTree) -> None:
        self.conf = conf.with_fallback(SparkSQLMetadataExtractor.DEFAULT_CONFIG)
        self._extract_iter = None  # type: Union[None, Iterator]
        self._cluster = self.conf.get_string(SparkSQLExtractor.CLUSTER_KEY)
        self._db = self.conf.get_string(SparkSQLExtractor.DATABASE_KEY)
        self.queries = self.conf.get_list(SparkSQLExtractor.QUERIES)     
        model_class = self.conf.get(SparkSQLExtractor.MODEL_CLASS)
        if model_class:
            module_name, class_name = model_class.rsplit(".", 1)
            mod = importlib.import_module(module_name)
            self.model_class = getattr(mod, class_name)

    def set_spark(self, spark: SparkSession) -> None:
        self.spark = spark
        
    def set_query(self, query: str) -> None:
        self.query = query
        
    def push_queries(self, queries: str)

    def extract(self) -> Any:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            
            return None

    def get_scope(self) -> str:
        return 'extractor.spark_sql'

    def _get_extract_iter(self) -> Iterator[Any]:
        """
        Extracts the result of the query 
        The spark session must be set for the extractor before extraction.
        """
        if self.spark is None:
            LOGGER.error("Spark session is not assigned to Spark SQL Extractor, cannot extract")
            return None
        if self.query is None:
            LOGGER.error("Query is not assigned to Spark SQL Extractor, nothing to extract")
            return None
        try:
            rows = self.spark.sql(query).collect()
        except Exception as e:
            LOGGER.error(e)
            return None
        for row in rows:
            row_dict = row.asDict()
            if hasattr(self, 'model_class'):
                yield self.model_class(**row_dict)
            else:
                yield row_dict
