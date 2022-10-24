#!/bin/bash
set -e

source ../common/env.sh

cd ${INSTALL_PATH}/hive-tpcds-setup
./tpcds-setup_partitioned.sh $SCALE
