install tpcds;
load tpcds;
call dsdgen(sf = 1);
export database 'data/tpcds_1' (format parquet);
