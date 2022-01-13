#!/bin/bash
source flink_env.sh
export num_iters=1
$FLINK_HOME/bin/flink run -c com.ververica.flink.benchmark.Benchmark ${FLINK_TEST_JAR} --database ${FLINK_TEST_DB} --hive_conf $HIVE_CONF_DIR --iterations $num_iters
