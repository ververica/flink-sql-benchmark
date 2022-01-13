#!/bin/bash
source flink_env.sh
${FLINK_HOME}/bin/yarn-session.sh -d -qu default
./run_query.sh q1.sql 1