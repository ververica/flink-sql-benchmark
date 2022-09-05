#!/bin/bash

export INSTALL_PATH="/home/hadoop/flink-sql-benchmark"

export FLINK_HOME=${INSTALL_PATH}/packages/flink-1.15.3
export FLINK_TEST_JAR=${INSTALL_PATH}/flink-tpcds/target/flink-tpcds-0.1-SNAPSHOT-jar-with-dependencies.jar

export SPARK_HOME=${INSTALL_PATH}/packages/spark-3.2.1
export SPARK_CONF_DIR=$SPARK_HOME/conf
export SPARK_TEST_JAR=${INSTALL_PATH}/spark-tpcds/target/spark-tpcds-0.1-SNAPSHOT-jar-with-dependencies.jar
# xxx.xxx.xxx.xxx is Hive thrift IP address. Opening this variable if you want to analyze spark statistics.
# export SPARK_BEELINE_SERVER="jdbc:hive2://xxx.xxx.xxx.xxx:10001"

export SCALE=10000
export FLINK_TEST_DB=tpcds_bin_orc_$SCALE
export SPARK_TEST_DB=spark_tpcds_bin_orc_$SCALE

# If you try to run partition table TPC-DS tests, you need to use the below environment variables.
# export FLINK_TEST_DB=tpcds_bin_partitioned_orc_$SCALE
# export SPARK_TEST_DB=spark_tpcds_bin_partitioned_orc_$SCALE



