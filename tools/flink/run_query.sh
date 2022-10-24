#!/bin/bash
source ../common/env.sh
num_iters=$1
queryStatement=$2

if [! -n "$queryStatement"] ;then
  $FLINK_HOME/bin/flink run -c com.ververica.flink.benchmark.Benchmark ${FLINK_TEST_JAR} --database ${FLINK_TEST_DB} --hive_conf $HIVE_CONF_DIR --iterations $num_iters
else
  $FLINK_HOME/bin/flink run -c com.ververica.flink.benchmark.Benchmark ${FLINK_TEST_JAR} --database ${FLINK_TEST_DB} --hive_conf $HIVE_CONF_DIR --queries $queryStatement --iterations $num_iters
fi



