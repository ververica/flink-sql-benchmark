#!/bin/bash
source ../common/env.sh
source ./flink_env.sh

${FLINK_HOME}/bin/yarn-session.sh -d -qu default

${FLINK_HOME}/bin/flink run -c com.ververica.flink.benchmark.UpdateHiveCatalogStats ${FLINK_TEST_JAR} ${FLINK_TEST_DB}