# flink-sql-benchmark
  
## Run benchmark in flink

- Step 1: Prepare your flink environment.

  - Prepare flink-conf.yaml: [Recommended Conf](https://github.com/JingsongLi/flink-sql-benchmark/blob/master/flink-conf.yaml).

  - Setup hive integration: [Hive dependencies](https://ci.apache.org/projects/flink/flink-docs-master/dev/table/hive/#dependencies).
  
  - Setup hadoop integration: [Hadoop environment](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/hadoop.html).
  
  - Setup flink cluster: [Standalone cluster](https://ci.apache.org/projects/flink/flink-docs-master/ops/deployment/cluster_setup.html) or [Yarn session](https://ci.apache.org/projects/flink/flink-docs-master/ops/deployment/yarn_setup.html#flink-yarn-session).
  
  - Recommended environment for 10T
    - 20 machines.
    - Machine: 64 processors. 256GB memory. 1 SSD disk for spill. Multi SATA disks for HDFS.
  
- Step 2: Build test jar.

  - Modify flink version and hive version of `pom.xml`.
  
  - `cd flink-tpcds`, `mvn clean install`
  
- Step 3: Run

  - `flink_home/bin/flink run -c com.ververica.flink.benchmark.Benchmark ./flink-tpcds-0.1-SNAPSHOT-jar-with-dependencies.jar --database tpcds_bin_orc_10000 --hive_conf hive_home/conf`
  
## Run benchmark in other systems

Because the prepared test data is standard hive data, other calculation frameworks integrated with hive data can also run benchmark very simply. Please build your own environment and test it.


If you have any questions, please contact:
- Jingsong Lee (jingsonglee0@gmail.com)
- Rui Li (lirui@apache.org)
