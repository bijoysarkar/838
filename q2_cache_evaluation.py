#from __future__ import print_function

import os
import sys
import time

from pyspark import SparkContext, StorageLevel, SparkConf
from pyspark.sql import HiveContext


class SparkPersistEvaluation:

    def __init__(self):
        # create the context
        self.sparkContext, self.sqlContext = self.create_context()
        # select the correct database
        self.sqlContext.sql("use tpcds_text_db_1_50")

    def create_context(self):
        conf = (SparkConf()
            .set("spark.app.name", "CS-838-Assignment2-Question2")
            .set("spark.master", "spark://10.0.1.96:7077")
            .set("spark.driver.memory", "1g")
            .set("spark.eventLog.enabled", "true")
            .set("spark.eventLog.dir", "/home/ubuntu/storage/logs")
            .set("spark.executor.memory", "21g")
            .set("spark.executor.cores", "4")
            .set("spark.task.cpus", "1"))

        sparkContext = SparkContext(conf=conf)
        sqlContext = HiveContext(sparkContext)
        return sparkContext, sqlContext

    def read_query(self): 
        with open ("/home/ubuntu/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query12.sql", "r") as myfile:
            query = myfile.read().replace('\n', ' ').replace(";", "")
        return query

    def cleanup(self):
        self.sparkContext.stop()

    def evaluate_no_cache(self):

        # execute the query
        output_rdd = self.sqlContext.sql(self.read_query())
        time1 = time.time()
        output_rdd.show()
        time2 = time.time()
        self.cleanup()
        print (time2-time1)

    def evaluate_input_cached(self):

        #cache the input tables
        self.sqlContext.cacheTable("web_sales")
        self.sqlContext.cacheTable("item")
        self.sqlContext.cacheTable("date_dim")
    
        # execute the query
        query = self.read_query()
        output_rdd = self.sqlContext.sql(query)
        time1 = time.time()
        output_rdd.show() # caches the input tables while computing
        time2 = time.time()

        output_rdd = self.sqlContext.sql(query)
        output_rdd.show() # uses the cached input table while computing
        time3 = time.time()
        self.cleanup()
        print (time2-time1),(time3-time2)

    def evaluate_output_cached(self):

        # execute the query
        output_rdd = self.sqlContext.sql(query)
        output_rdd.registerTempTable("output_rdd")
        sqlContext.cacheTable("output_rdd")
        time1 = time.time()
        output_rdd.show()
        time2 = time.time()

        output_mod = self.sqlContext.sql("SELECT * FROM output_rdd")
        output_mod.show()
        time3 = time.time()
        self.cleanup()
        print (time2-time1),(time3-time2)

if __name__ == "__main__":
    spark_persist_evaluation = SparkPersistEvaluation() 
    spark_persist_evaluation.evaluate_no_cache()
