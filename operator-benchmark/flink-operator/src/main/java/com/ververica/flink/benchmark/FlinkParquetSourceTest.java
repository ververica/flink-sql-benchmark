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

import org.apache.flink.formats.parquet.vector.ParquetColumnarRowSplitReader;
import org.apache.flink.table.data.ColumnarRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class FlinkParquetSourceTest {

	private static final String FILE_PATH = "/Users/zhixin/data/tpc/parquet-1200-985.parquet";
	private static final String[] FIELD_NAMES = new String[] {
//			"l_orderkey",
//			"l_partkey",
//			"l_suppkey",
//			"l_linenumber",
			"l_quantity",
			"l_extendedprice",
			"l_discount",
			"l_tax",
			"l_returnflag",
			"l_linestatus",
			"l_shipdate"
//			"l_commitdate",
//			"l_receiptdate",
//			"l_shipinstruct",
//			"l_shipmode",
//			"l_comment"
	};
	private static final LogicalType[] FIELD_TYPES = new LogicalType[] {
//			new IntType(),
//			new IntType(),
//			new IntType(),
//			new IntType(),
			new DoubleType(),
			new DoubleType(),
			new DoubleType(),
			new DoubleType(),
			new VarCharType(Integer.MAX_VALUE),
			new VarCharType(Integer.MAX_VALUE),
			new DateType(),
//			new VarCharType(Integer.MAX_VALUE),
//			new VarCharType(Integer.MAX_VALUE),
//			new VarCharType(Integer.MAX_VALUE),
//			new VarCharType(Integer.MAX_VALUE),
//			new VarCharType(Integer.MAX_VALUE),
	};

	private ParquetColumnarRowSplitReader createReader() throws IOException {
		return new ParquetColumnarRowSplitReader(true, false, new Configuration(), FIELD_TYPES, FIELD_NAMES,
				VectorizedColumnBatch::new, 2048, new Path(FILE_PATH), 0, Long.MAX_VALUE);
	}

	@Benchmark
	public void readVector(Blackhole bh) throws IOException {
		ParquetColumnarRowSplitReader reader = createReader();
		while (!reader.reachedEnd()) {
			ColumnarRowData row = reader.nextRecord();
			bh.consume(row);
			bh.consume(readDouble(row, 0));
		}
		reader.close();
	}

	@Benchmark
	public void readFieldsRarely(Blackhole bh) throws IOException {
		ParquetColumnarRowSplitReader reader = createReader();
		int i = 0;
		while (!reader.reachedEnd()) {
			ColumnarRowData row = reader.nextRecord();
			bh.consume(row);
			bh.consume(readDouble(row, 0));
			i++;
			if (i % 100 == 0) {
				read(bh, row);
			}
		}
		reader.close();
	}

	@Benchmark
	public void readFields(Blackhole bh) throws IOException {
		ParquetColumnarRowSplitReader reader = createReader();
		while (!reader.reachedEnd()) {
			ColumnarRowData row = reader.nextRecord();
			bh.consume(row);
			read(bh, row);
		}
		reader.close();
	}

	private void read(Blackhole bh, ColumnarRowData row) {
		bh.consume(readDouble(row, 0));
		bh.consume(readDouble(row, 1));
		bh.consume(readDouble(row, 2));
		bh.consume(readDouble(row, 3));
		bh.consume(readString(row, 4));
		bh.consume(readString(row, 5));
		bh.consume(readInt(row, 6));
	}

	private static int readInt(ColumnarRowData row, int i) {
		return !row.isNullAt(i) ? row.getInt(i) : -1;
	}

	private static double readDouble(ColumnarRowData row, int i) {
		return !row.isNullAt(i) ? row.getDouble(i) : -1;
	}

	private static StringData readString(ColumnarRowData row, int i) {
		return !row.isNullAt(i) ? row.getString(i) : BinaryStringData.EMPTY_UTF8;
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(FlinkParquetSourceTest.class.getSimpleName())
				.build();

		new Runner(opt).run();
	}
}
