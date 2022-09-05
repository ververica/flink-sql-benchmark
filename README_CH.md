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
  - Hive 环境
    - 开启 Hive 的 metastore 和 Hive 的 HiveServer2 服务
      - 开启命令 `nohup hive --service metastore &` 和 `nohup hive --service hiveservice2 &`
      - 配置环境变量 `${HIVE_CONF_DIR}` 指向 Hive conf 安装目录
    - 集群需安装 `gcc`. Hive 原始数据生成时会使用
      
- TPC-DS Benchmark 工程准备
  - 下载 TPC-DS flink-sql-benchmark 工程
    - 方法1: download jar 包
      - 暂不支持
    - 方法2: clone github 工程，并自己打包
      - 工程 clone:
        - git clone https://github.com/ververica/flink-sql-benchmark.git`
      - 打包
        - 工程目录下执行： `mvn clean package -DskipTests`
          - 如果在集群上打包，需要集群安装 Maven
      - 上传
        - 如果不在集群上打包，需使用 scp 命令将 TPC-DS Benchmark 工程拷贝到集群的指定目录中

- Flink 环境
  - 下载对应版本 Flink 到 TPC-DS Benchmark 工程的 `packages` 目录下
    - 下载地址: `https://flink.apache.org/downloads.html`, 下载对应版本的 Flink， 本文档测试的 Flink 版本为 Flink 1.16
    - 这里将 Flink 安装在 `packages` 目录下， 可以避免修改环境变量，与其他冲突，推荐放到 `packages` 目录下
  - 替换推荐配置文件
    - 使用TPC-DS工程目录下 `tools/flink/flink-conf.yaml` 替换 `${FLINK_HOME}/conf/flink-conf.yaml`。该配置下，Flink 能达到较优的结果。
  - 上传 `flink-sql-connector-hive包`
    - 下载对应版本的 `flink-sql-connector-hive` 放到 `${FLINK_HOME}/lib/` 下
      - 例如： `flink-1.15.0` 对应的 `connector` 包下载地址: `https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2_2.12/1.15.0/flink-sql-connector-hive-3.1.2_2.12-1.15.0.jar`

### Step 2: TPC-DS 原始数据生成

*注意：* 在后续的文档中，出现相对路径，例如：`tools/common`。其对应的绝对路径都是上传到集群的 flink-sql-benchmark 工程的根目录 + 相对路径，例如：`${INSTALL_PATH}/flink-sql-benchmark/` + `tools/common`。

- 设置环境信息
  - `cd tools/common`，并修改 `env.sh`：
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
    - cd `tools/datagen`; 运行 `./init_hive_db.sh`， 生成原始数据和对应的 Hive 外表
      - 生成的原始数据会存在 HDFS 的 `/tmp/tpcds-generate/${SCALE}` 目录下
      - Hive 中会生成指向原始数据的数据库 `tpcds_text_${SCALE}`
      - Hive 中会生成 Flink 使用的 orc 格式外表数据库 `tpcds_bin_orc_${SCALE}`
  - Partition 表数据生成
    - cd `tools/datagen`; 运行 `./init_partition_db.sh`， 生成原始数据和对应的 Hive 外表
    - 生成的原始数据会存在 HDFS 的 `/tmp/tpcds-generate/${SCALE}` 目录下
    - Hive 中会生成指向原始数据的数据库 `tpcds_text_${SCALE}`
    - Hive 中会生成 Flink 使用的 orc 格式的多 partition 外表数据库 `tpcds_bin_partitioned_orc_${SCALE}`
      - 这一步因为涉及到 hive 动态分区的数据写入，时间较长，请耐心等待

### Step 3: TPC-DS 统计信息生成
- 统计信息生成:
  - 生成各表和各列的统计信息： cd `tools/stats`; 运行 `./flink_init_stats.sh`
    - 注意：该步骤使用了 Flink analyze table 语法，只有 Flink-1.16 支持
    - Partition table 的统计信息生成会非常的慢，这里也推荐使用 Hive analyze table 语法，其生成的统计信息与 Flink 一致
      - Hive analyze table 语法参考文档：`https://cwiki.apache.org/confluence/display/hive/statsdev`
      - Hive3.x 可以在 hive client 中执行 `ANALYZE TABLE store_sales COMPUTE STATISTICS FOR ALL COLUMNS;` 来分析 partition table 的所有分区，而不用指定具体的分区。

### Step 4: Flink 执行 TPC-DS Queries

- 执行单 query：`cd tools/flink`，run `./run_query.sh q1.sql 1`
  - 其中，`q1.sql` 代表的是执行的 query 的序号， 取值为1-99，`1` 为运行的轮次，建议取值为 2
  - 当轮次取值为 `1` 时，由于 Flink 集群有预热时间，会导致运行时间大于实际 query 执行时间
- 执行所有 queries：`cd tools/flink`，run `./run_all_queries.sh 2`
  - 由于 run all queries 时间较长，可以采用后台运行的方式。命令为: `nohup ./run_all_queries.sh 2 > partition_result.log 2>&1 &`
- 可选参数：`--warmup`，加快执行时间
  - 例: `nohup ./run_all_queries.sh 2 warmup > partition_result.log 2>&1 &`