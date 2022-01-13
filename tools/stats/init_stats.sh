#!/bin/bash
set -e
./collect_spark_stats.sh
./dump_spark_stats.sh
./load_column_stats.sh