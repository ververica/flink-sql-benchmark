source ../common/env.sh
query=$1
${SPARK_HOME}/bin/spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 16G --driver-cores 8 \
        --executor-memory 5000M --executor-cores 10 --num-executors 150 \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+UseParallelGC -XX:+UseParallelOldGC" \
        --conf spark.sql.crossJoin.enabled=true \
        --conf spark.sql.cbo.joinReorder.enabled=true \
        --conf spark.sql.autoBroadcastJoinThreshold=20MB \
        --conf spark.sql.shuffle.partitions=3000 \
        --conf spark.yarn.am.waitTime=600000 \
        --conf spark.default.parallelism=3000 \
        --conf spark.executor.memoryOverhead=15000M \
        --conf spark.memory.offHeap.enabled=true \
        --conf spark.memory.offHeap.size=1200M \
        --class com.ververica.spark.benchmark.TPCDSQueryBenchmark ${SPARK_TEST_JAR} \
        --mode execute --cbo --data-location hdfs:///user/hive/warehouse/${FLINK_TEST_DB}.db --database ${SPARK_TEST_DB} --query-filter $query

