#!/bin/bash

function usage {
	echo "Usage: tpcds-setup-partitioned.sh scale_factor [temp_directory]"
	exit 1
}

function runcommand {
	if [ "X$DEBUG_SCRIPT" != "X" ]; then
		$1
	else
		$1 2>/dev/null
	fi
}

if [ "X$HIVE_BIN" = "X" ]; then
  HIVE_BIN=`which hive`
  if [ $? -ne 0 ]; then
    echo "Script must be run where hive in PATH or HIVE_BIN env variable is set"
    exit 1
  fi
fi

# Tables in the TPC-DS schema.
DIMS="date_dim time_dim item customer customer_demographics household_demographics customer_address store promotion warehouse ship_mode reason income_band call_center web_page catalog_page web_site"
FACTS="store_sales store_returns web_sales web_returns catalog_sales catalog_returns inventory"

# Get the parameters.
SCALE=$1
if [ $SCALE -eq 1 ]; then
	echo "Scale factor must be greater than 1"
	exit 1
fi

if [ "X$BUCKET_DATA" != "X" ]; then
	BUCKETS=13
	RETURN_BUCKETS=13
else
	BUCKETS=1
	RETURN_BUCKETS=1
fi
if [ "X$DEBUG_SCRIPT" != "X" ]; then
	set -x
fi

# Create tables for the specified table format.
if [ "X$FORMAT" = "X" ]; then
	FORMAT=orc
fi
LOAD_FILE="load_${FORMAT}_${SCALE}.mk"
SILENCE="2> /dev/null 1> /dev/null"
if [ "X$DEBUG_SCRIPT" != "X" ]; then
	SILENCE=""
fi

echo -e "all: ${DIMS} ${FACTS}" > $LOAD_FILE

i=1
total=24
SOURCE=tpcds_bin_${FORMAT}_${SCALE}
DATABASE=tpcds_bin_partitioned_${FORMAT}_${SCALE}
MAX_REDUCERS=2500 # maximum number of useful reducers for any scale
REDUCERS=$((test ${SCALE} -gt ${MAX_REDUCERS} && echo ${MAX_REDUCERS}) || echo ${SCALE})

runcommand "$HIVE_BIN -f ddl-tpcds/bin_partitioned/create_alltables.sql --hivevar DB=${DATABASE}"

for t in ${DIMS}
do
	COMMAND="$HIVE_BIN -f ddl-tpcds/bin_partitioned/${t}.sql \
	    --hivevar DB=${DATABASE} --hivevar SOURCE=${SOURCE} \
            --hivevar SCALE=${SCALE} \
	    --hivevar REDUCERS=${REDUCERS} \
	    --hivevar FILE=${FORMAT}"
	echo -e "${t}:\n\t@$COMMAND $SILENCE && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
	i=`expr $i + 1`
done
for t in ${FACTS}
do
	COMMAND="$HIVE_BIN -f ddl-tpcds/bin_partitioned/${t}.sql \
	    --hivevar DB=${DATABASE} \
            --hivevar SCALE=${SCALE} \
	    --hivevar SOURCE=${SOURCE} --hivevar BUCKETS=${BUCKETS} \
	    --hivevar RETURN_BUCKETS=${RETURN_BUCKETS} --hivevar REDUCERS=${REDUCERS} --hivevar FILE=${FORMAT}"
	echo -e "${t}:\n\t@$COMMAND $SILENCE && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
	i=`expr $i + 1`
done
make -j 1 -f $LOAD_FILE
echo "Loading constraints"
runcommand "$HIVE_BIN -f ddl-tpcds/bin/add_constraints.sql --hivevar DB=${DATABASE}"
echo "Data loaded into database ${DATABASE}."