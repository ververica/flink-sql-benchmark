# flink-sql-benchmark

## Flink TPC-DS benchmark

### Step 1: Environment prepare

- Recommended configuration for Hadoop cluster
  - Resource allocation
    - master *1 :
      - vCPU 32 cores, Memory: 128 GiB /  System disk: 120GB *1, Data disk: 80GB *1
    - worker *15 :
      - vCPU 80 cores, Memory: 352 GiB / System disk: 120GB *1, Data disk: 7300GB *30
    - This document was tested in Hadoop3.2.1 and Hive3.1.2
  - Hadoop environment prepare
    - Install Hadoop(3.2.1), and then configure HDFS and Yarn according to `https://hadoop.apache.org/docs/r3.1.2/hadoop-project-dist/hadoop-common/ClusterSetup.html`
    - Set Hadoop environment variables: `${HADOOP_HOME}` and `${HADOOP_CLASSPATH}`
    - Configure Yarn, and then start ResourceManager and NodeManager
      - modify `yarn.application.classpath` in yarn-site, adding mapreduce's dependency to classpath:
        - `$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*,$HADOOP_YARN_HOME/share/hadoop/mapreduce/*`
  - Hive environment prepare
    - Run hive metastore and HiveServer2
      - Set Hive environment variable: `${HIVE_CONF_DIR}`. This variable equals to the site of `hive-site.xml`.
      - commands: `nohup hive --service metastore &` / `nohup hive --service hiveservice2 &`
    - `gcc` is also needed in your cluster
  
- TPC-DS benchmark project prepare 
  - Download flink-sql-benchmark project
    - Method one: download jar
      - Don't support now
    - Method two: clone github project, and then package it by yourself
      - project clone:
        - `git clone https://github.com/ververica/flink-sql-benchmark.git`
      - package:
        - cd project's root directory, run `mvn clean package -DskipTests`
      - upload the packaged whole project to cluster:
        - `tar -cvf flink-sql-benchmark.tar master`
        - `scp flink-sql-benchmark.tar root@xxx.xxx.xxx.xxx:folder`

- Flink environment prepare
    - Download Flink-1.16 to folder `packages` in your packaged flink-sql-benchmark project
      - Flink download path: `https://flink.apache.org/downloads.html` (recommend FLINK-1.16)
      - recommend to put Flink project into folder `${INSTALL_PATH}/flink-sql-benchmark/packages`
    - Replace `flink-conf.yaml`
      - `mv ${INSTALL_PATH}/flink-sql-benchmark/tools/flink/flink-conf.yaml ${INSTALL_PATH}/flink-sql-benchmark/packages/flink-1.16/conf/flink-conf.yaml`
    - Upload `flink-sql-connector-hive-3.1.2_2.12`
      - download `flink-sql-connector-hive-3.1.2_2.12-1.16.0.jar`, and put it to `${INSTALL_PATH}/flink-sql-benchmark/packages/flink-1.16/lib/`
        - download path: `https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2_2.12/1.16.0/flink-sql-connector-hive-3.1.2_2.12-1.16.0.jar`

### Step 2: Generate TPC-DS dataset

*Note:* In the following docs, if directory is relative directory, like `tools/common`, its absolute directory is
`${INSTALL_PATH}/flink-sql-benchmark/` + relative directory, like `${INSTALL_PATH}/flink-sql-benchmark/tools/common`. 

- Set common environment variables 
  - `cd tools/common`, and `vim env.sh`:
    - None-partitioned table
      - `SCALE` is the size of the generated dataset (GB). Recommend value is 10000 (10TB)
        - This variable is recommended to use 1000
      - `FLINK_TEST_DB` is Hive database name, which will be used by Flink
        - This variable is recommended to use the default name: `export FLINK_TEST_DB=tpcds_bin_orc_$SCALE`
    - Partitioned table
      - `FLINK_TEST_DB` need to be modified to `export FLINK_TEST_DB=tpcds_bin_partitioned_orc_$SCALE`
- Data generating
  - None-partitioned table
    - `cd tools/datagen`, run `./init_hive_db.sh`. `./init_hive_db.sh` will first launch a Mapreduce job to generate the data in text format which is stored in HDFS. Then it will create external Hive databases based on the generated text files.
      - Origin text format data will be stored in HDFS folder: `/tmp/tpcds-generate/${SCALE}`
      - Hive external database points to origin text data is: `tpcds_text_${SCALE}`
      - Hive orc format external database, which points to origin text data is: `tpcds_bin_orc_${SCALE}`
  - Partitioned table
    - `cd tools/datagen`, run `./init_partition_db.sh`. If origin text format data don't exist in HDFS, it will create origin file first. Otherwise, it will create external Hive databases with partition tables based on the generated text files.
    - Origin text format data will also be stored in HDFS folder: `/tmp/tpcds-generate/${SCALE}`
    - Hive external database points to origin text data is: `tpcds_text_${SCALE}`
    - Hive orc format external database with partition table, which points to origin text data is: `tpcds_bin_partitioned_orc_${SCALE}`
      - This command will be very slow because Hive dynamic partition data writing is very slow

### Step 3: Generate table statistics for TPC-DS dataset
- Generate statistics for table and every column:
  - `cd tools/stats`,  run `./init_stats.sh`
    - Note: this step use Flink analyze table syntax to generate statistics, so it only supports in Flink-1.16
    - For partition table, this step is very slow. it's recommend to use Hive analyze table syntax, which will generate same stats with Flink analyze table syntax
      - The document for Hive analyze table syntax: `https://cwiki.apache.org/confluence/display/hive/statsdev`
      - In Hive3.x, it can run `ANALYZE TABLE store_sales COMPUTE STATISTICS FOR ALL COLUMNS;` in hive client to generate stats for all partitions instead of specifying one partition

### Step 4: Flink run TPC-DS queries

- run single query: `cd tools/flink`, run `./run_query.sh q1.sql 1`
  - In `./run_query.sh q1.sql 1`, `q1.sql` stands for query number (q1 -> q99). `1` stands for the iterator of execution, which default number is 1
  - If iterator of execution is `1`, the total execution time will be longer than the query execution time because of Flink cluster warmup time cost 
- run all queries: `cd tools/flink`, run `./run_all_queries.sh 2`
  - Because of running all queries will cost a long time, it recommends to run this command in nohup mode: `nohup ./run_all_queries.sh 2 > partition_result.log 2>&1 &`
- Optional factors： `--warmup`: Flink cluster warmup
  - For example: `nohup ./run_all_queries.sh 2 warmup > partition_result.log 2>&1 &`