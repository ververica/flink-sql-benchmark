/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.benchmark;

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HadoopFileOpener;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveBatchPageSourceFactory;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.ParquetFileWriterConfig;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.orc.OrcBatchPageSourceFactory;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveType.toHiveType;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
public class PrestoOrcSourceTest {

	private static final JobConf conf;

	static {
		conf = new JobConf(new Configuration(false));
		conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
	}

	private static final String FILE_PATH = "/Users/zhixin/data/tpc/orc";
	private static final String[] FIELD_NAMES = new String[] {
			"ss_sold_date_sk",
			"ss_sold_time_sk",
			"ss_item_sk",
			"ss_customer_sk",
			"ss_cdemo_sk",
			"ss_hdemo_sk",
			"ss_addr_sk",
			"ss_store_sk",
			"ss_promo_sk",
			"ss_ticket_number",
			"ss_quantity",
			"ss_wholesale_cost",
			"ss_list_price",
			"ss_sales_price",
			"ss_ext_discount_amt",
			"ss_ext_sales_price",
			"ss_ext_wholesale_cost",
			"ss_ext_list_price",
			"ss_ext_tax",
			"ss_coupon_amt",
			"ss_net_paid",
			"ss_net_paid_inc_tax",
			"ss_net_profit"
	};
	private static final Type[] FIELD_TYPES = new Type[] {
			BigintType.BIGINT,
			BigintType.BIGINT,
			BigintType.BIGINT,
			BigintType.BIGINT,
			BigintType.BIGINT,
			BigintType.BIGINT,
			BigintType.BIGINT,
			BigintType.BIGINT,
			BigintType.BIGINT,
			BigintType.BIGINT,
			IntegerType.INTEGER,
			DoubleType.DOUBLE,
			DoubleType.DOUBLE,
			DoubleType.DOUBLE,
			DoubleType.DOUBLE,
			DoubleType.DOUBLE,
			DoubleType.DOUBLE,
			DoubleType.DOUBLE,
			DoubleType.DOUBLE,
			DoubleType.DOUBLE,
			DoubleType.DOUBLE,
			DoubleType.DOUBLE,
			DoubleType.DOUBLE
	};

	private static final HiveClientConfig HIVE_CLIENT_CONFIG = createHiveClientConfig();
	private static final MetastoreClientConfig METASTORE_CLIENT_CONFIG = new MetastoreClientConfig();
	private static final HdfsEnvironment HDFS_ENVIRONMENT = createTestHdfsEnvironment(HIVE_CLIENT_CONFIG, METASTORE_CLIENT_CONFIG);
	private ConnectorSession session;

	public PrestoOrcSourceTest() {
		HiveClientConfig config = new HiveClientConfig();
		config.setUseParquetColumnNames(true);
		session = new TestingConnectorSession(new HiveSessionProperties(
				config, new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
	}

	private ConnectorPageSource createReader(File file) {
		HiveBatchPageSourceFactory pageSourceFactory = new OrcBatchPageSourceFactory(
				new TypeRegistry(),
				true,
				HDFS_ENVIRONMENT,
				new FileFormatDataSourceStats(),
				100,
				new StorageOrcFileTailSource(),
				new StorageStripeMetadataSource(),
				new HadoopFileOpener());

		return createPageSource(pageSourceFactory, session, file, Arrays.asList(FIELD_NAMES),
				Arrays.asList(FIELD_TYPES), HiveStorageFormat.ORC);
	}

	private static ConnectorPageSource createPageSource(
			HiveBatchPageSourceFactory pageSourceFactory,
			ConnectorSession session,
			File targetFile,
			List<String> columnNames,
			List<Type> columnTypes,
			HiveStorageFormat format)
	{
		List<HiveColumnHandle> columnHandles = new ArrayList<>(columnNames.size());
		TypeTranslator typeTranslator = new HiveTypeTranslator();
		for (int i = 0; i < columnNames.size(); i++) {
			String columnName = columnNames.get(i);
			Type columnType = columnTypes.get(i);
			columnHandles.add(new HiveColumnHandle(columnName, toHiveType(typeTranslator, columnType), columnType.getTypeSignature(), i, REGULAR, Optional.empty()));
		}

		return pageSourceFactory
				.createPageSource(
						conf,
						session,
						new Path(targetFile.getAbsolutePath()),
						0,
						targetFile.length(),
						targetFile.length(),
						new Storage(
								StorageFormat.create(format.getSerDe(), format.getInputFormat(), format.getOutputFormat()),
								"location",
								Optional.empty(),
								false,
								ImmutableMap.of()),
						ImmutableMap.of(),
						columnHandles,
						TupleDomain.all(),
						DateTimeZone.forID(session.getTimeZoneKey().getId()),
						Optional.empty())
				.get();
	}

	private static HiveClientConfig createHiveClientConfig() {
		HiveClientConfig config = new HiveClientConfig();
		config.setHiveStorageFormat(HiveStorageFormat.ORC);
		return config;
	}

	public static HdfsEnvironment createTestHdfsEnvironment(HiveClientConfig config, MetastoreClientConfig metastoreClientConfig) {
		HdfsConfiguration hdfsConfig = new HiveHdfsConfiguration(
				new HdfsConfigurationInitializer(
						config,
						metastoreClientConfig,
						new PrestoS3ConfigurationUpdater(new HiveS3Config()),
						new HiveGcsConfigurationInitializer(new HiveGcsConfig())),
				ImmutableSet.of());
		return new HdfsEnvironment(hdfsConfig, metastoreClientConfig, new NoHdfsAuthentication());
	}

	@Benchmark
	public void readVector(Blackhole bh) throws IOException {
		for (File file : new File(FILE_PATH).listFiles()) {
			ConnectorPageSource reader = createReader(file);
			Page page;
			while ((page = reader.getNextPage()) != null) {
				for (int i = 0; i < FIELD_NAMES.length; i++) {
					bh.consume(page.getBlock(i).getLoadedBlock());
				}
			}
			reader.close();
		}
	}

	@Benchmark
	public void readFieldsRarely(Blackhole bh) throws IOException {
		for (File file : new File(FILE_PATH).listFiles()) {
			ConnectorPageSource reader = createReader(file);
			Page page;
			while ((page = reader.getNextPage()) != null) {
				for (int i = 0; i < FIELD_NAMES.length; i++) {
					bh.consume(page.getBlock(i).getLoadedBlock());
				}
				for (int i = 0; i < page.getBlock(0).getPositionCount(); i++) {
					if (i % 100 == 0) {
						for (int j = 0; j < 10; j++) {
							bh.consume(readLong(page.getBlock(j), i));
						}
						bh.consume(readInt(page.getBlock(10), i));
						for (int j = 11; j < 23; j++) {
							bh.consume(readDouble(page.getBlock(j), i));
						}
					}
				}
			}
			reader.close();
		}
	}

	@Benchmark
	public void readFields(Blackhole bh) throws IOException {
		for (File file : new File(FILE_PATH).listFiles()) {
			ConnectorPageSource reader = createReader(file);
			Page page;
			while ((page = reader.getNextPage()) != null) {
				for (int i = 0; i < FIELD_NAMES.length; i++) {
					bh.consume(page.getBlock(i).getLoadedBlock());
				}
				for (int i = 0; i < page.getBlock(0).getPositionCount(); i++) {
					for (int j = 0; j < 10; j++) {
						bh.consume(readLong(page.getBlock(j), i));
					}
					bh.consume(readInt(page.getBlock(10), i));
					for (int j = 11; j < 23; j++) {
						bh.consume(readDouble(page.getBlock(j), i));
					}
				}
			}
			reader.close();
		}
	}

	private int readInt(Block block, int position) {
		if (block.isNull(position)) {
			return 0;
		}

		return (int) DateType.DATE.getLong(block, position);
	}

	private double readDouble(Block block, int position) {
		if (block.isNull(position)) {
			return 0;
		}

		return DoubleType.DOUBLE.getDouble(block, position);
	}

	private long readLong(Block block, int position) {
		if (block.isNull(position)) {
			return 0;
		}
		return BigintType.BIGINT.getLong(block, position);
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(PrestoOrcSourceTest.class.getSimpleName())
				.build();

		new Runner(opt).run();
	}
}
