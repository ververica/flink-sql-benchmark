#!/bin/bash
source ../common/env.sh

spark-beeline -u ${SPARK_BEELINE_SERVER}/${SPARK_TEST_DB} -f analyze.sql