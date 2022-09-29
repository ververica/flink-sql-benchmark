# flink-sql-benchmark

## Flink TPC-DS benchmark 标准流程

### Step 1: 环境准备

- 集群 配置推荐
  - 推荐配置：
    - 主节点 *1：
      - vCPU 32 核，内存 128 GiB / 系统盘大小：120GB *1，数据盘大小：80GB *1
    - 实例 *15：
      - vCPU 80 核，内存 352 GiB / 系统盘大小：120GB *1，数据盘大小：7300GB *30
    - 本文档测试环境软件版本：Hadoop3.2.1，Hive3.1.2
  - Hadoop 环境准备
    - 安装 Hadoop, 并配置好 HDFS 和 Yarn（参考链接：`https://hadoop.apache.org/docs/r3.1.2/hadoop-project-dist/hadoop-common/ClusterSetup.html`）
    - 配置 Hadoop 环境变量： `${HADOOP_HOME}` 和 `${HADOOP_CLASSPATH}`
    - 配置 Yarn， 并启动 ResourceManager 和 NodeManager：
      - 修改 yarn-site 中 classpath 配置, 增加 mapreduce 相关类到 classpath 中：
        - `$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*,$HADOOP_YARN_HOME/share/hadoop/mapreduce/*`
  - Hive 环境准备
    - 开启 Hive 的 metastore 和 Hive 的 HiveServer2 服务
      - 开启命令 `nohup hive --service metastore &` 和 `nohup hive --service hiveservice2 &`
      - 配置环境变量 `${HIVE_CONF_DIR}` 指向 Hive conf 安装目录
    - 集群需安装 `gcc`. Hive 原始数据生成时会使用
      
- TPC-DS Benchmark 工程准备
  - 下载 TPC-DS flink-sql-benchmark 工程
    - clone github 工程，并自己打包
      - 工程 clone:
        - git clone https://github.com/ververica/flink-sql-benchmark.git`
      - 打包
        - 工程目录下执行： `mvn clean package -DskipTests`
        - 集群需已经安装 `maven`

- Flink 环境
  - 下载 Flink-1.16 到 TPC-DS Benchmark 工程的 `packages` 目录下（`${INSTALL_PATH}/flink-sql-benchmark/packages`）
    - Flink 1.16 下载地址: `https://flink.apache.org/downloads.html` 
  - 替换推荐配置文件
    - 使用 `${INSTALL_PATH}/flink-sql-benchmark/tools/common/flink-conf.yaml` 替换 `${INSTALL_PATH}/flink-sql-benchmark/packages/flink-1.16/conf/link-conf.yaml`。经过测试该配置下，Flink 能达到较优的结果。
  - 下载 `flink-sql-connector-hive包`
    - 下载对应版本的 `flink-sql-connector-hive` 放到 `${INSTALL_PATH}/flink-sql-benchmark/packages/flink-1.16/lib/` 下
      - 例如： `flink-1.16.0` 对应的 `connector` 包下载地址: `https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2_2.12/1.16.0/flink-sql-connector-hive-3.1.2_2.12-1.16.0.jar`

### Step 2: TPC-DS 原始数据生成

请先切换目录到 `${INSTALL_PATH}/flink-sql-benchmark`

- 设置环境信息
  - 执行 `vim tools/common/env.sh`：
    - 非 Partition 表
      - `SCALE` 对应生成数据集大小，单位 GB。推荐设置为 10000 (10TB)
      - `FLINK_TEST_DB` 对应生成的 Hive 外表的 Database name。该 Database 提供给 Flink 使用
        - 使用默认即可: `export FLINK_TEST_DB=tpcds_bin_orc_$SCALE`
    - Partition 表
      - `SCALE` 对应生成数据集大小，单位 GB. 推荐设置为 10000 (10TB)
      - `FLINK_TEST_DB`
      - 修改为: `export FLINK_TEST_DB=tpcds_bin_partitioned_orc_$SCALE`
- 数据生成
  - 非 Partition 表数据生成
    - 执行 `./tools/datagen/init_db.sh`，生成原始数据和对应的 Hive 外表
      - 生成的原始数据会存在 HDFS 的 `/tmp/tpcds-generate/${SCALE}` 目录下
      - Hive 中会生成指向原始数据的数据库 `tpcds_text_${SCALE}`
      - Hive 中会生成 Flink 使用的 orc 格式外表数据库 `tpcds_bin_orc_${SCALE}`
  - Partition 表数据生成
    - 执行 `./tools/datagen/init_partition_db.sh`，生成原始数据和对应的 Hive 外表
      - 生成的原始数据会存在 HDFS 的 `/tmp/tpcds-generate/${SCALE}` 目录下
      - Hive 中会生成指向原始数据的数据库 `tpcds_text_${SCALE}`
      - Hive 中会生成 Flink 使用的 orc 格式的多 partition 外表数据库 `tpcds_bin_partitioned_orc_${SCALE}`
        - 这一步因为涉及到 hive 动态分区的数据写入，时间较长，请耐心等待

### Step 3: TPC-DS 统计信息生成

请先切换目录到 `${INSTALL_PATH}/flink-sql-benchmark`

- 统计信息生成:
  - 生成各表和各列的统计信息：执行 `./tools/stats/init_stats.sh`
    - 注意：该步骤使用了 Flink analyze table 语法，只有 Flink-1.16 及后续版本才支持
      - Flink analyze table 语法参考文档：`https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/analyze/`
    - Partition table 的统计信息生成会非常的慢，这里也推荐使用 Hive analyze table 语法，其生成的统计信息与 Flink 一致
      - Hive analyze table 语法参考文档：`https://cwiki.apache.org/confluence/display/hive/statsdev`
      - Hive3.x 可以在 hive client 中执行 `ANALYZE TABLE store_sales COMPUTE STATISTICS FOR ALL COLUMNS;` 来分析 partition table 的所有分区，而不用指定具体的分区。

### Step 4: Flink 执行 TPC-DS Queries

请先切换目录到 `${INSTALL_PATH}/flink-sql-benchmark`

- 运行单 query：`./tools/flink/run_query.sh q1.sql 1`
  - 其中，`q1.sql` 代表的是执行的 query 的序号， 取值为1-99，`1` 为运行的轮次，建议取值为 2
  - 当轮次取值为 `1` 时，由于 Flink 集群有预热时间，会导致运行时间大于实际 query 执行时间
- 运行所有 queries：`./tools/flink/run_all_queries.sh 2`
  - 由于 run all queries 时间较长，可以采用后台运行的方式。命令为: `nohup ./run_all_queries.sh 2 > partition_result.log 2>&1 &`
- 可选参数：`--warmup`，加快执行时间
  - 例: `nohup ./run_all_queries.sh 2 warmup > partition_result.log 2>&1 &`