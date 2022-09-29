#!/bin/bash

# flink-sql-benchmark install path.
export INSTALL_PATH="../../../flink-sql-benchmark"

export FLINK_HOME=${INSTALL_PATH}/packages/flink-1.16.0
export FLINK_TEST_JAR=${INSTALL_PATH}/flink-tpcds/target/flink-tpcds-0.1-SNAPSHOT-jar-with-dependencies.jar

export SCALE=10000
export FLINK_TEST_DB=tpcds_bin_orc_$SCALE

# If you try to run TPC-DS tests on partition table, you need to use the below environment variables.
# export FLINK_TEST_DB=tpcds_bin_partitioned_orc_$SCALE



