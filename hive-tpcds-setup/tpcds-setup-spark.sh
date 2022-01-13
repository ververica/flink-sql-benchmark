#!/bin/bash

function usage {
        echo "Usage: tpcds-setup.sh scale_factor"
        exit 1
}

function runcommand {
        if [ "X$DEBUG_SCRIPT" != "X" ]; then
                $1
        else
                $1 2>/dev/null
        fi
}

if [ ! -f target/hive-tpcds-setup-0.1-SNAPSHOT.jar ]; then
        echo "Please build the data generator with ./tpcds-build.sh first"
        exit 1
fi

if [ "X$HIVE_BIN" = "X" ]; then
  HIVE_BIN=`which hive`
  if [ $? -ne 0 ]; then
    echo "Script must be run where hive in PATH or HIVE_BIN env variable is set"
    exit 1
  fi
fi

# Get the parameters.
SCALE=$1
DIR=/user/hive/warehouse/tpcds_bin_orc_${SCALE}.db

# Sanity checking.
if [ X"$SCALE" = "X" ]; then
        usage
fi
if [ $SCALE -eq 1 ]; then
        echo "Scale factor must be greater than 1"
        exit 1
fi

# Create the text/flat tables as external tables.
echo "Loading data as external tables."
SPARK_DATABASE=spark_tpcds_bin_orc_${SCALE}
runcommand "$HIVE_BIN -f ddl-tpcds/bin/alltables.sql --hivevar DB=${SPARK_DATABASE} --hivevar LOCATION=${DIR}"

runcommand "$HIVE_BIN -f ddl-tpcds/bin/add_constraints.sql --hivevar DB=${SPARK_DATABASE}"
echo "Data loaded into database ${SPAR_DATABASE}."