# flink-sql-benchmark

## Flink TPC-DS benchmark 标准流程

### 步骤 1：环境准备

- 集群配置推荐
  - 推荐配置：
    - master *1：
      - vCPU 32 核，内存 128 GiB / 系统盘大小：120GB *1，数据盘大小：80GB *1
    - worker *15：
      - vCPU 80 核，内存 352 GiB / 系统盘大小：120GB *1，数据盘大小：7300GB *30
    - 本文档测试环境软件版本：Hadoop3.2.1，Hive3.1.2
  - Hadoop 环境准备
    - 安装 Hadoop, 并配置好 HDFS 和 Yarn（[配置文档](https://hadoop.apache.org/docs/r3.1.2/hadoop-project-dist/hadoop-common/ClusterSetup.html)）
    - 配置 Hadoop 环境变量： `${HADOOP_HOME}` 和 `${HADOOP_CLASSPATH}`
    - 配置 Yarn， 并启动 ResourceManager 和 NodeManager：
      - 修改 yarn-site 中 classpath 配置, 增加 mapreduce 相关类到 classpath 中：
        - `$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*,$HADOOP_YARN_HOME/share/hadoop/mapreduce/*`
  - Hive 环境准备
    - 开启 Hive 的 metastore 和 Hive 的 HiveServer2 服务
      - 配置环境变量 `${HIVE_CONF_DIR}` 指向 Hive conf 安装目录
      - 开启命令 `nohup hive --service metastore &` 和 `nohup hive --service hiveservice2 &`
  - 其他
    - Master 节点需安装 `gcc` 用于构建 TPC-DS 数据生成器
      
- TPC-DS Benchmark 工程准备
  - 下载 TPC-DS flink-sql-benchmark 工程： `git clone https://github.com/ververica/flink-sql-benchmark.git` 到 master 节点
    - 本文档中，下载后的 flink-sql-benchmark 工程的绝对路径被称为 `${INSTALL_PATH}`
  - 生成 jar 包： `cd ${INSTALL_PATH}` 并执行 `mvn clean package -DskipTests` (集群需已经安装 `maven`)

- Flink 环境准备
  - 下载 Flink-1.16 到 TPC-DS Benchmark 工程的 `packages` 目录下（`${INSTALL_PATH}/packages`）
    - [Flink 1.16 下载地址](https://flink.apache.org/downloads.html`)
  - 使用推荐 Flink 配置
    - 使用 `${INSTALL_PATH}/tools/common/flink-conf.yaml` 替换 `${INSTALL_PATH}/packages/flink-1.16/conf/link-conf.yaml`
  - 下载 `flink-sql-connector-hive` 包
    - 下载对应版本的 [`flink-sql-connector-hive`](https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2_2.12/1.16.0/flink-sql-connector-hive-3.1.2_2.12-1.16.0.jar) 放到 `${INSTALL_PATH}/flink-sql-benchmark/packages/flink-1.16/lib/` 下
  - 启动 yarn session 集群
    - 切换到 `${INSTALL_PATH}/packages/flink-1.16/bin`，执行 `./yarn-session.sh -d -qu default` 命令启动集群

### 步骤 2：TPC-DS 原始数据生成

请先切换目录到 `${INSTALL_PATH}`

- 设置环境信息
  - 执行 `vim tools/common/env.sh`：
    - 非 Partition 表
      - `SCALE` 对应生成数据集大小，单位 GB。推荐设置为 10000 (10TB)
      - `FLINK_TEST_DB` 对应生成的 Hive 外表的 Database name。该 Database 提供给 Flink 使用
        - 使用默认即可: `export FLINK_TEST_DB=tpcds_bin_orc_$SCALE`
    - Partition 表
      - `SCALE` 对应生成数据集大小，单位 GB. 推荐设置为 10000 (10TB)
      - `FLINK_TEST_DB` 对应生成的 Hive 外表的 Database name。该 Database 提供给 Flink 使用
        - 使用时修改为: `export FLINK_TEST_DB=tpcds_bin_partitioned_orc_$SCALE`
- 数据生成
  - 非 Partition 表数据生成
    - 执行 `./tools/datagen/init_db_for_none_partition_tables.sh`，生成原始数据和对应的 Hive 外表
      - 生成的原始数据会存在 HDFS 的 `/tmp/tpcds-generate/${SCALE}` 目录下
      - Hive 中会生成指向原始数据的数据库 `tpcds_text_${SCALE}`
      - Hive 中会生成 Flink 使用的 orc 格式外表数据库 `tpcds_bin_orc_${SCALE}`
  - Partition 表数据生成
    - 执行 `./tools/datagen/init_db_for_partiiton_tables.sh`，生成原始数据和对应的 Hive 外表
      - 生成的原始数据会存在 HDFS 的 `/tmp/tpcds-generate/${SCALE}` 目录下
      - Hive 中会生成指向原始数据的数据库 `tpcds_text_${SCALE}`
      - Hive 中会生成 Flink 使用的 orc 格式的多 partition 外表数据库 `tpcds_bin_partitioned_orc_${SCALE}`
        - 这一步因为涉及到 hive 动态分区的数据写入，时间较长，请耐心等待

### 步骤 3：TPC-DS 统计信息生成

请先切换目录到 `${INSTALL_PATH}`

- 统计信息生成：
  - 生成各表和各列的统计信息：执行 `./tools/stats/analyze_table_stats.sh`
    - 注意：该步骤使用了 Flink `analyze table` 语法，只有 Flink-1.16 及后续版本才支持
      - [Flink analyze table 语法参考文档](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/analyze/)
    - Partition table 的统计信息生成会非常的慢，这里也推荐使用 Hive analyze table 语法，其生成的统计信息与 Flink 一致
      - [Hive analyze table 语法参考文档](https://cwiki.apache.org/confluence/display/hive/statsdev)
      - Hive3.x 可以在 hive client 中执行 `ANALYZE TABLE partition_table_name COMPUTE STATISTICS FOR COLUMNS;` 来分析 partition table 的所有分区，而不用指定具体的分区。

### 步骤 4：Flink 执行 TPC-DS Queries

请先切换目录到 `${INSTALL_PATH}`

- TPC-DS query 执行：
  - 运行指定 query：`./tools/flink/run_query.sh 1 q1.sql`
    - 其中，`1` 为运行的轮次，建议取值为 2; `q1.sql` 代表的是执行的 query 的序号， 取值为 1-99
    - 当轮次取值为 `1` 时，由于 Flink 集群有预热时间，会导致运行时间大于实际 query 执行时间
  - 运行所有 queries：`./tools/flink/run_query.sh 2`
    - 由于 run all queries 时间较长，可以采用后台运行的方式。命令为： `nohup ./run_query.sh 2 > partition_result.log 2>&1 &`
- 通过修改 `tools/flink/run_query.sh` 参数来增加可选的参数：
  - `--location`：需要执行的 sql queries 的位置。如果你只想执行部分 queries，你可以通过设置该参数来指定文件夹，并将需要执行的 sql files 放置在这个文件夹下
  - `--mode`：执行方式，`execute` 或者 `explain`，默认是 `execute`
- Kill yarn 作业：
  - 执行 `yarn application -list` 来得到当前 Flink 作业的 yarn 作业 id
  - 执行 `yarn application -kill application_xxx` 来杀死当前 yarn 作业
  - 如果你通过这种方式停止 Flink 作业，那么在下次执行 TPC-DS query 时，你需要重新启动 yarn 集群
    - 启动命令： 切换到 `${INSTALL_PATH}/packages/flink-1.16/bin`，执行 `./yarn-session.sh -d -qu default` 命令启动集群