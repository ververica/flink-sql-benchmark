#!/bin/bash
source ../common/env.sh
${FLINK_HOME}/bin/flink run -c com.ververica.flink.benchmark.UpdateHiveCatalogStats ${FLINK_TEST_JAR} ${FLINK_TEST_DB} $HIVE_CONF_DIR ${SPARK_TEST_DB}.stats