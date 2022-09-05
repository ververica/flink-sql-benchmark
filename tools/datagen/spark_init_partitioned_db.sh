#!/bin/bash
set -e

source ../common/env.sh

cd ${INSTALL_PATH}/hive-tpcds-setup
./tpcds-build.sh
./tpcds-setup-spark_partitioned.sh $SCALE

