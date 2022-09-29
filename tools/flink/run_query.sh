#!/bin/bash
source ../common/flink_env.sh
export queryStatement=$1
export num_iters=$2

${FLINK_HOME}/bin/yarn-session.sh -d -qu default

$FLINK_HOME/bin/flink run -c com.ververica.flink.benchmark.Benchmark ${FLINK_TEST_JAR} --database ${FLINK_TEST_DB} --hive_conf $HIVE_CONF_DIR --queries $queryStatement --iterations $num_iters