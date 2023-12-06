#!/usr/bin/env python3

# duckdb < duckdb.sql
# make run-scaled
# make submit app=tpcds.py

import os.path
import glob
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

for name in glob.glob("/opt/spark/data/tpcds_1/*.parquet"):
    table = os.path.basename(name.replace(".parquet", ""))
    print(f"Table {table}")
    spark.read.parquet(name).createOrReplaceTempView(table)


for name in sorted(glob.glob("/opt/spark/apps/queries/*.sql")):
    sql = open(name).read()
    print("%s:\n%s" % (name, sql))
    t0 = time.time()
    result = spark.sql(sql)
    result.show()
    duration = time.time()-t0
    print("%s executed in %.3f" % (name, duration))
    #result.explain(mode="extended")
