#!/bin/bash

function usage {
	echo "Usage: tpcds-setup.sh scale_factor [temp_directory]"
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

# Tables in the TPC-DS schema.
DIMS="date_dim time_dim item customer customer_demographics household_demographics customer_address store promotion warehouse ship_mode reason income_band call_center web_page catalog_page web_site"
FACTS="store_sales store_returns web_sales web_returns catalog_sales catalog_returns inventory"

# Get the parameters.
SCALE=$1
DIR=$2
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

# Sanity checking.
if [ X"$SCALE" = "X" ]; then
	usage
fi
if [ X"$DIR" = "X" ]; then
	DIR=/tmp/tpcds-generate
fi
if [ $SCALE -eq 1 ]; then
	echo "Scale factor must be greater than 1"
	exit 1
fi

# Do the actual data generation.
hdfs dfs -mkdir -p ${DIR}
hdfs dfs -ls ${DIR}/${SCALE} > /dev/null
if [ $? -ne 0 ]; then
	echo "Generating data at scale factor $SCALE."
	hadoop jar target/*.jar -d ${DIR}/${SCALE}/ -s ${SCALE}
fi
hdfs dfs -ls ${DIR}/${SCALE} > /dev/null
if [ $? -ne 0 ]; then
	echo "Data generation failed, exiting."
	exit 1
fi

hadoop fs -chmod -R 777  ${DIR}/${SCALE}

echo "TPC-DS text data generation complete."

# Create the text/flat tables as external tables.
echo "Loading text data into external tables."
TXT_DATABASE=tpcds_text_${SCALE}
runcommand "$HIVE_BIN -f ddl-tpcds/text/alltables.sql --hivevar DB=${TXT_DATABASE} --hivevar LOCATION=${DIR}/${SCALE}"

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
DATABASE=tpcds_bin_${FORMAT}_${SCALE}
MAX_REDUCERS=2500 # maximum number of useful reducers for any scale
REDUCERS=$((test ${SCALE} -gt ${MAX_REDUCERS} && echo ${MAX_REDUCERS}) || echo ${SCALE})

for t in ${DIMS}
do
	COMMAND="$HIVE_BIN -f ddl-tpcds/bin/${t}.sql \
	    --hivevar DB=${DATABASE} --hivevar SOURCE=${TXT_DATABASE} \
            --hivevar SCALE=${SCALE} \
	    --hivevar REDUCERS=${REDUCERS} \
	    --hivevar FILE=${FORMAT}"
	echo -e "${t}:\n\t@$COMMAND $SILENCE && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
	i=`expr $i + 1`
done
for t in ${FACTS}
do
	COMMAND="$HIVE_BIN -f ddl-tpcds/bin/${t}.sql \
	    --hivevar DB=${DATABASE} \
            --hivevar SCALE=${SCALE} \
	    --hivevar SOURCE=${TXT_DATABASE} --hivevar BUCKETS=${BUCKETS} \
	    --hivevar RETURN_BUCKETS=${RETURN_BUCKETS} --hivevar REDUCERS=${REDUCERS} --hivevar FILE=${FORMAT}"
	echo -e "${t}:\n\t@$COMMAND $SILENCE && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
	i=`expr $i + 1`
done
make -j 1 -f $LOAD_FILE
echo "Loading constraints"
runcommand "$HIVE_BIN -f ddl-tpcds/bin/add_constraints.sql --hivevar DB=${DATABASE}"
echo "Data loaded into database ${DATABASE}."