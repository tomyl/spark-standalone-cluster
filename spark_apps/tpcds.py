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

def strip_comments(sql):
    output = []
    for line in sql.splitlines():
        if line.startswith('-- '):
            continue
        output.append(line)
    return '\n'.join(output)

def split_statements(sql):
    statements = []
    for statement in strip_comments(sql).split(';'):
        if statement:
            statements.append(statement)
    return statements

def run_queries(spark, pattern, start=1, end=99):
    t0 = time.time()
    for i in range(start, end+1):
        t1 = time.time()
        name = pattern % i
        if not os.path.exists(name):
            print("%s: not found" % name)
            continue
        sql = open(name).read()
        print("FILE %s:\n%s" % (name, sql))
        statements = split_statements(sql)
        num = len(statements)
        for i, sql in enumerate(statements):
            if num > 1:
                print("FILE %s (%d/%d):\n%s" % (name, i+1, num, sql))
            t2 = time.time()
            df = spark.sql(sql)
            df.explain(mode="extended")
            df.collect()
            df.show()
            if num > 1:
                print("FILE %s (%d/%d) executed in %.3f" % (name, i+1, num, time.time()-t2))
        print("FILE %s executed in %.3f" % (name, time.time()-t1))
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
