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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
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
import java.util.Arrays;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class SparkParquetSourceTest {

	private static final String FILE_PATH = "/Users/zhixin/data/tpc/parquet-1200-985.parquet";

	private Configuration configuration;
	private TaskAttemptID attemptId;
	private StructType struct;

	public SparkParquetSourceTest() {
		struct = new StructType();
//		struct = struct.add("l_orderkey", "int");
//		struct = struct.add("l_partkey", "int");
//		struct = struct.add("l_suppkey", "int");
//		struct = struct.add("l_linenumber", "int");
		struct = struct.add("l_quantity", "double");
		struct = struct.add("l_extendedprice", "double");
		struct = struct.add("l_discount", "double");
		struct = struct.add("l_tax", "double");
		struct = struct.add("l_returnflag", "String");
		struct = struct.add("l_linestatus", "String");
		struct = struct.add("l_shipdate", "int");
//		struct = struct.add("l_commitdate", "String");
//		struct = struct.add("l_receiptdate", "String");
//		struct = struct.add("l_shipinstruct", "String");
//		struct = struct.add("l_shipmode", "String");
//		struct = struct.add("l_comment", "String");
		attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0);
		configuration = new Configuration();
		configuration.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA(), struct.json());
		configuration.set("spark.sql.parquet.binaryAsString", "false");
		configuration.set("spark.sql.parquet.int96AsTimestamp", "false");
		configuration.set("spark.sql.parquet.writeLegacyFormat", "false");
		configuration.set("spark.sql.parquet.int64AsTimestampMillis", "false");
	}

	private VectorizedParquetRecordReader createReader() throws IOException {
		VectorizedParquetRecordReader reader = new VectorizedParquetRecordReader(null, false, 2048);
		reader.initialize(FILE_PATH, Arrays.asList(struct.names()));
		return reader;
	}

	@Benchmark
	public void readVector(Blackhole bh) throws IOException, InterruptedException {
		VectorizedParquetRecordReader reader = createReader();
		while (reader.nextKeyValue()) {
			InternalRow row = (InternalRow)reader.getCurrentValue();
			bh.consume(row);
			bh.consume(readDouble(row, 0));
		}
		reader.close();
	}

	@Benchmark
	public void readFieldsRarely(Blackhole bh) throws IOException, InterruptedException {
		VectorizedParquetRecordReader reader = createReader();
		int i = 0;
		while (reader.nextKeyValue()) {
			InternalRow row = (InternalRow) reader.getCurrentValue();
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
	public void readFields(Blackhole bh) throws IOException, InterruptedException {
		VectorizedParquetRecordReader reader = createReader();
		while (reader.nextKeyValue()) {
			InternalRow row = (InternalRow)reader.getCurrentValue();
			bh.consume(row);
			read(bh, row);
		}
		reader.close();
	}

	private void read(Blackhole bh, InternalRow row) {
		bh.consume(readDouble(row, 0));
		bh.consume(readDouble(row, 1));
		bh.consume(readDouble(row, 2));
		bh.consume(readDouble(row, 3));
		bh.consume(readString(row, 4));
		bh.consume(readString(row, 5));
		bh.consume(readInt(row, 6));
	}

	private static int readInt(InternalRow row, int i) {
		return !row.isNullAt(i) ? row.getInt(i) : -1;
	}

	private static double readDouble(InternalRow row, int i) {
		return !row.isNullAt(i) ? row.getDouble(i) : -1;
	}

	private static UTF8String readString(InternalRow row, int i) {
		return !row.isNullAt(i) ? row.getUTF8String(i) : UTF8String.EMPTY_UTF8;
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(SparkParquetSourceTest.class.getSimpleName())
				.build();

		new Runner(opt).run();
	}
}
