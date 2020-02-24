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

import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcColumnarRowSplitReader;
import org.apache.flink.orc.nohive.OrcNoHiveSplitReaderUtil;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class FlinkOrcSourceTest {

	private static final String FILE_PATH = "/Users/zhixin/data/tpc/004990_0.orc";
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
	private static final LogicalType[] FIELD_TYPES = new LogicalType[] {
			new BigIntType(),
			new BigIntType(),
			new BigIntType(),
			new BigIntType(),
			new BigIntType(),
			new BigIntType(),
			new BigIntType(),
			new BigIntType(),
			new BigIntType(),
			new BigIntType(),
			new IntType(),
			new DoubleType(),
			new DoubleType(),
			new DoubleType(),
			new DoubleType(),
			new DoubleType(),
			new DoubleType(),
			new DoubleType(),
			new DoubleType(),
			new DoubleType(),
			new DoubleType(),
			new DoubleType(),
			new DoubleType(),
	};

	private OrcColumnarRowSplitReader<VectorizedRowBatch> createReader() throws IOException {
		return OrcNoHiveSplitReaderUtil.genPartColumnarRowReader(
				new Configuration(),
				FIELD_NAMES,
				Arrays.stream(FIELD_TYPES)
						.map(TypeConversions::fromLogicalToDataType).toArray(DataType[]::new),
				new HashMap<>(),
				IntStream.range(0, FIELD_NAMES.length).toArray(),
				new ArrayList<>(),
				2048,
				new Path(FILE_PATH),
				0,
				Long.MAX_VALUE);
	}

	@Benchmark
	public void readVector(Blackhole bh) throws IOException {
		OrcColumnarRowSplitReader reader = createReader();
		while (!reader.reachedEnd()) {
			ColumnarRow row = (ColumnarRow) reader.nextRecord(null);
			bh.consume(row);
			bh.consume(readLong(row, 0));
		}
		reader.close();
	}

	@Benchmark
	public void readFieldsRarely(Blackhole bh) throws IOException {
		OrcColumnarRowSplitReader reader = createReader();
		int i = 0;
		while (!reader.reachedEnd()) {
			ColumnarRow row = (ColumnarRow) reader.nextRecord(null);
			bh.consume(row);
			bh.consume(readLong(row, 0));
			i++;
			if (i % 100 == 0) {
				read(bh, row);
			}
		}
		reader.close();
	}

	@Benchmark
	public void readFields(Blackhole bh) throws IOException {
		OrcColumnarRowSplitReader reader = createReader();
		while (!reader.reachedEnd()) {
			ColumnarRow row = (ColumnarRow) reader.nextRecord(null);
			bh.consume(row);
			read(bh, row);
		}
		reader.close();
	}

	private void read(Blackhole bh, ColumnarRow row) {
		bh.consume(readLong(row, 0));
		bh.consume(readLong(row, 1));
		bh.consume(readLong(row, 2));
		bh.consume(readLong(row, 3));
		bh.consume(readLong(row, 4));
		bh.consume(readLong(row, 5));
		bh.consume(readLong(row, 6));
		bh.consume(readLong(row, 7));
		bh.consume(readLong(row, 8));
		bh.consume(readLong(row, 9));
		bh.consume(readInt(row, 10));
		bh.consume(readDouble(row, 11));
		bh.consume(readDouble(row, 12));
		bh.consume(readDouble(row, 13));
		bh.consume(readDouble(row, 14));
		bh.consume(readDouble(row, 15));
		bh.consume(readDouble(row, 16));
		bh.consume(readDouble(row, 17));
		bh.consume(readDouble(row, 18));
		bh.consume(readDouble(row, 19));
		bh.consume(readDouble(row, 20));
		bh.consume(readDouble(row, 21));
		bh.consume(readDouble(row, 22));
	}

	private static int readInt(ColumnarRow row, int i) {
		return !row.isNullAt(i) ? row.getInt(i) : -1;
	}

	private static double readDouble(ColumnarRow row, int i) {
		return !row.isNullAt(i) ? row.getDouble(i) : -1;
	}

	private static long readLong(ColumnarRow row, int i) {
		return !row.isNullAt(i) ? row.getLong(i) : -1;
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(FlinkOrcSourceTest.class.getSimpleName())
				.build();

		new Runner(opt).run();
	}
}
