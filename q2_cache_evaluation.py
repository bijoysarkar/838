#from __future__ import print_function

import os
import sys
import time
from subprocess import call

from pyspark import SparkContext, StorageLevel, SparkConf
from pyspark.sql import HiveContext
from stats import GetStatistics

class SparkPersistEvaluation:

    def __init__(self):
        # create the context
        self.sparkContext, self.sqlContext = self.create_context()
        # select the correct database
        self.sqlContext.sql("use tpcds_text_db_1_50")

        self.get_statistics = GetStatistics()
        #self.get_statistics.cleanup()
        #vda_read, vda_write = get_statistics.get_disc_data()
        #network_read, network_write = get_statistics.get_net_data()

    def setup_init(self):
        call(("ssh ubuntu@"+worker+" cat /proc/diskstats").split())

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
        vda_read_before, vda_write_before = self.get_statistics.get_disc_data()
        network_read_before, network_write_before = self.get_statistics.get_net_data()
        time1 = time.time()
        output_rdd = self.sqlContext.sql(self.read_query())
        output_rdd.show()
        time2 = time.time()
        network_read_after, network_write_after = self.get_statistics.get_net_data()
        vda_read_after, vda_write_after = self.get_statistics.get_disc_data()
        self.cleanup()
        print "\nTime", (time2-time1), "\nDisc Read",(vda_read_after-vda_read_before), "\nDisc Write",(vda_write_after-vda_write_before), "\nNet Read",(network_read_after-network_read_before), "\nNet Write",(network_write_after-network_write_before) 

    def evaluate_input_cached(self):

        #cache the input tables
        self.sqlContext.cacheTable("web_sales")
        self.sqlContext.cacheTable("item")
        self.sqlContext.cacheTable("date_dim")
    
        # execute the query
        query = self.read_query()
        vda_read_before, vda_write_before = self.get_statistics.get_disc_data()
        network_read_before, network_write_before = self.get_statistics.get_net_data()
        time1 = time.time()
        output_rdd = self.sqlContext.sql(query)
        output_rdd.show() # caches the input tables while computing
        time2 = time.time()
        vda_read_middle, vda_write_middle = self.get_statistics.get_disc_data()
        network_read_middle, network_write_middle = self.get_statistics.get_net_data()
        time3 = time.time()
        output_rdd = self.sqlContext.sql(query)
        output_rdd.show() # uses the cached input table while computing
        time4 = time.time()
        network_read_after, network_write_after = self.get_statistics.get_net_data()
        vda_read_after, vda_write_after = self.get_statistics.get_disc_data()
        self.cleanup()
        print "\nTime", (time2-time1), "\nCached Time", (time4-time3), "\nDisc Read",(vda_read_after-vda_read_middle), "\nDisc Write",(vda_write_after-vda_write_middle), "\nNet Read",(network_read_after-network_read_middle), "\nNet Write",(network_write_after-network_write_middle) 

    def evaluate_output_cached(self):

        query = self.read_query()
        # execute the query
        vda_read_before, vda_write_before = self.get_statistics.get_disc_data()
        network_read_before, network_write_before = self.get_statistics.get_net_data()
        time1 = time.time()
        output_rdd = self.sqlContext.sql(query)
        output_rdd.registerTempTable("output_rdd")
        self.sqlContext.cacheTable("output_rdd")
        output_rdd.show()
        time2 = time.time()

        vda_read_middle, vda_write_middle = self.get_statistics.get_disc_data()
        network_read_middle, network_write_middle = self.get_statistics.get_net_data()
        time3 = time.time()
        output_mod = self.sqlContext.sql("SELECT * FROM output_rdd")
        output_mod.show()
        time4 = time.time()
        network_read_after, network_write_after = self.get_statistics.get_net_data()
        vda_read_after, vda_write_after = self.get_statistics.get_disc_data()
        self.cleanup()
        print "\nTime", (time2-time1), "\nCached Time", (time4-time3), "\nDisc Read",(vda_read_after-vda_read_middle), "\nDisc Write",(vda_write_after-vda_write_middle), "\nNet Read",(network_read_after-network_read_middle), "\nNet Write",(network_write_after-network_write_middle) 

if __name__ == "__main__":
    spark_persist_evaluation = SparkPersistEvaluation()
    if sys.argv[1]=='2':	 
    	spark_persist_evaluation.evaluate_input_cached()
    elif sys.argv[1]=='3':
        spark_persist_evaluation.evaluate_output_cached()
    else:
        spark_persist_evaluation.evaluate_no_cache()
