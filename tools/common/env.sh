#!/bin/bash

export INSTALL_PATH="/home/hadoop/flink-sql-benchmark"

export FLINK_HOME=${INSTALL_PATH}/packages/flink-1.14.3
export FLINK_TEST_JAR=${INSTALL_PATH}/flink-tpcds/target/flink-tpcds-0.1-SNAPSHOT-jar-with-dependencies.jar

export SPARK_HOME=${INSTALL_PATH}/packages/spark-3.2.1
export SPARK_CONF_DIR=$SPARK_HOME/conf
export SPARK_TEST_JAR=${INSTALL_PATH}/spark-tpcds/target/spark-tpcds-0.1-SNAPSHOT-jar-with-dependencies.jar
export SPARK_BEELINE_SERVER="jdbc:hive2://emr-header-1:10001"

export SCALE=10000
export FLINK_TEST_DB=tpcds_bin_orc_$SCALE
export SPARK_TEST_DB=spark_tpcds_bin_orc_$SCALE



