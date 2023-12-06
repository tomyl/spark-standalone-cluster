#!/usr/bin/env python3

# duckdb < duckdb.sql
# make run-scaled
# docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.sql.autoBroadcastJoinThreshold=-1 ./apps/tpcds.py

import argparse
import glob
import sys
import time
import os.path
from pyspark.sql import SparkSession

def create_views(spark, pattern):
    for name in glob.glob(pattern):
        table = os.path.basename(name.replace(".parquet", ""))
        print(f"Table {table}")
        spark.read.parquet(name).createOrReplaceTempView(table)

def run_queries(spark, pattern, start=1, end=99):
    t0 = time.time()
    for i in range(start, end+1):
        t1 = time.time()
        name = pattern % i
        if not os.path.exists(name):
            print("%s: not found" % name)
            continue
        sql = open(name).read()
        print("%s:\n%s" % (name, sql))
        df = spark.sql(sql)
        df.explain(mode="extended")
        df.collect()
        df.show()
        print("%s executed in %.3f" % (name, time.time()-t1))
    print("%d queries executed in %.3f" % (end+1-start, time.time()-t0))

def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", metavar="NUM", type=int, default=1)
    parser.add_argument("--end", metavar="NUM", type=int, default=99)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("tpcds").getOrCreate()
    create_views(spark, "/opt/spark/data/tpcds_1/*.parquet")
    run_queries(spark, "/opt/spark/apps/queries/query%02d.sql", start=args.start, end=args.end)

if __name__ == '__main__':
    sys.exit(main())

# vim: set ts=4 sw=4 et
