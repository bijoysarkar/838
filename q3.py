from __future__ import print_function

import os
import sys
import time
from operator import add

from pyspark import SparkContext, StorageLevel, SparkConf
from pyspark.sql import HiveContext


if __name__ == "__main__":
    conf = (SparkConf().set("spark.app.name", "CS-838-Assignment2-Question3")
            .set("spark.app.name", "CS-838-Assignment2-Question2")
            .set("spark.master", "spark://10.0.1.96:7077")
            .set("spark.driver.memory", "1g")
            .set("spark.eventLog.enabled", "true")
            .set("spark.eventLog.dir", "/home/ubuntu/storage/logs")
            .set("spark.executor.memory", "21g")
            .set("spark.executor.cores", "4")
            .set("spark.task.cpus", "1"))

    sc = SparkContext(conf=conf)
    product = sc.textFile("PRODUCT.txt").map(lambda x: x.split('|'))
    sales = sc.textFile("SALES.txt").map(lambda x: x.split('|'))
    
    sales_aggregate = sales.map(lambda x: [x[1], int(x[2])]).reduceByKey(add).sortBy(lambda x: x[1], ascending=False)
    sales_top_5 = sc.parallelize(sales_aggregate.take(5)) 

    result = sales_top_5.join(product.map(lambda x: (x[0], x[1]))).sortBy(lambda x: x[1][0], ascending=False).map(lambda x: x[1][1])
    result.saveAsTextFile("output"+str(int(time.time())))
