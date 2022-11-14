#!/bin/bash
source ../common/env.sh

${FLINK_HOME}/bin/flink run -c com.ververica.flink.benchmark.AnalyzeTableRunner ${FLINK_TEST_JAR} ${FLINK_TEST_DB}