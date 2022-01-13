#!/bin/bash
set -e

source ../common/env.sh

cd ${INSTALL_PATH}/hive-tpcds-setup
./tpcds-build.sh
./tpcds-setup.sh $SCALE
./tpcds-setup-spark.sh $SCALE
