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

    def create_context(self):
        # Create conf for spark context
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
        # Read query from file
        with open ("/home/ubuntu/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query12.sql", "r") as myfile:
            query = myfile.read().replace('\n', ' ').replace(";", "")
        return query

    def cleanup(self):
        self.sparkContext.stop()

    def evaluate_cacheing(self):

        query = self.read_query()

        # Part A
        disk_read_1, disk_write_1 = self.get_statistics.get_disc_data()
        net_read_1, net_write_1 = self.get_statistics.get_net_data()
        time1 = time.time()
        output_rdd = self.sqlContext.sql(query)
        output_rdd.show()
        time2 = time.time()
        disk_read_2, disk_write_2 = self.get_statistics.get_disc_data()
        net_read_2, net_write_2 = self.get_statistics.get_net_data()

        # Part B
        #cache the input tables
        self.sqlContext.cacheTable("web_sales")
        self.sqlContext.cacheTable("item")
        self.sqlContext.cacheTable("date_dim")

        # execute the query which will also cache the input
        disk_read_3, disk_write_3 = self.get_statistics.get_disc_data()
        net_read_3, net_write_3 = self.get_statistics.get_net_data()
        time3 = time.time()
        output_rdd = self.sqlContext.sql(query)
        output_rdd.show()
        time4 = time.time()
        disk_read_4, disk_write_4 = self.get_statistics.get_disc_data()
        net_read_4, net_write_4 = self.get_statistics.get_net_data()

        # execute the query when the input cache is used
        disk_read_5, disk_write_5 = self.get_statistics.get_disc_data()
        net_read_5, net_write_5 = self.get_statistics.get_net_data()
        time5 = time.time()
        output_rdd = self.sqlContext.sql(query)
        output_rdd.show()
        time6 = time.time()
        disk_read_6, disk_write_6 = self.get_statistics.get_disc_data()
        net_read_6, net_write_6 = self.get_statistics.get_net_data()

        # Part C
        # execute the query when the output will also be cached
        disk_read_7, disk_write_7 = self.get_statistics.get_disc_data()
        net_read_7, net_write_7 = self.get_statistics.get_net_data()
        time7 = time.time()
        output_rdd = self.sqlContext.sql(query)
        output_rdd.registerTempTable("output_rdd")
        self.sqlContext.cacheTable("output_rdd")
        output_rdd.show()
        time8 = time.time()
        disk_read_8, disk_write_8 = self.get_statistics.get_disc_data()
        net_read_8, net_write_8 = self.get_statistics.get_net_data()

        #execute query that uses cached output
        time9 = time.time()
        output_mod = self.sqlContext.sql("SELECT * FROM output_rdd")
        output_mod.show()
        time10 = time.time()
        disk_read_9, disk_write_9 = self.get_statistics.get_disc_data()
        net_read_9, net_write_9 = self.get_statistics.get_net_data()
        self.cleanup()

        # Print observed numbers
        print "No cache\t\tTime", (time2-time1), "\tDisk Read", (disk_read_2-disk_read_1), "\tDisk Write", (disk_write_2-disk_write_1), "\tNet Read",(net_read_2-net_read_1) , "\tNet Write", (net_write_2-net_write_1)
        print "While Input cacheing\tTime", (time4-time3), "\tDisk Read", (disk_read_4-disk_read_3), "\tDisk Write", (disk_write_4-disk_write_3), "\tNet Read",(net_read_4-net_read_3) , "\tNet Write", (net_write_4-net_write_3)
        print "With Input Cached\tTime", (time6-time5), "\tDisk Read", (disk_read_6-disk_read_5), "\tDisk Write", (disk_write_6-disk_write_5), "\tNet Read",(net_read_6-net_read_5) , "\tNet Write", (net_write_6-net_write_5)
        print "While Output Cacheing\tTime", (time8-time7), "\tDisk Read", (disk_read_8-disk_read_7), "\tDisk Write", (disk_write_8-disk_write_7), "\tNet Read",(net_read_8-net_read_7) , "\tNet Write", (net_write_8-net_write_7)
        print "With Output Cached\tTime", (time10-time9), "\tDisk Read", (disk_read_9-disk_read_8), "\tDisk Write", (disk_write_9-disk_write_8), "\tNet Read",(net_read_9-net_read_8) , "\tNet Write", (net_write_9-net_write_8)


if __name__ == "__main__":
    spark_persist_evaluation = SparkPersistEvaluation()
    spark_persist_evaluation.evaluate_cacheing()
