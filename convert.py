#!/usr/bin/env python3

import argparse
import glob
import os
import sys
import polars as pl

def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", metavar="PATH", type=str)
    args = parser.parse_args(argv)

    for src in glob.glob(os.path.join(args.directory, '*.parquet')):
        dst = src.replace('.parquet', '.delta')
        print(src, dst)
        df = pl.read_parquet(src)
        df.write_delta(dst)

if __name__ == '__main__':
    sys.exit(main())

# vim: set ts=4 sw=4 et
