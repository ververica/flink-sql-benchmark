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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class Runner {

	private static final Logger LOG = LoggerFactory.getLogger(Runner.class);

	private final String name;
	private final String sqlQuery;
	private final int numIters;
	private final TableEnvironment tEnv;

	Runner(String name, String sqlQuery, int numIters, TableEnvironment tEnv) {
		this.name = name;
		this.sqlQuery = sqlQuery;
		this.numIters = numIters;
		Preconditions.checkArgument(numIters > 0);
		this.tEnv = tEnv;
	}

	void run(List<Tuple2<String, Long>> bestArray) {
		List<Result> results = new ArrayList<>();
		for (int i = 0; i < numIters; ++i) {
			System.err.println(
					String.format("--------------- Running %s %s/%s ---------------", name, (i + 1), numIters));
			try {
				results.add(runInternal());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		printResults(results, bestArray);
	}

	private Result runInternal() throws Exception {
		// ensures garbage from previous cases don't impact this one
		System.gc();

		LOG.info("begin register tables.");

		long startTime = System.currentTimeMillis();
		LOG.info(" begin optimize.");

		Table table = tEnv.sqlQuery(sqlQuery);

		LOG.info(" begin execute.");

		List<Row> res = CollectionUtil.iteratorToList(table.execute().collect());

		LOG.info(" end execute");

		System.out.println();

		long totalTime = System.currentTimeMillis() - startTime;
		System.out.println("total execute " + totalTime + "ms.");

		printRow(res);

		return new Result(totalTime);
	}

	private void printResults(List<Result> results, List<Tuple2<String, Long>> bestArray) {
		int itemMaxLength = 20;
		System.err.println();
		Benchmark.printLine('-', "+", itemMaxLength, "", "", "", "");
		Benchmark.printLine(' ', "|", itemMaxLength, " " + name, " Best Time(ms)", " Avg Time(ms)", " Max Time(ms)");
		Benchmark.printLine('-', "+", itemMaxLength, "", "", "", "");

		Tuple3<Long, Long, Long> t3 = getBestAvgMaxTime(results);
		Benchmark.printLine(' ', "|", itemMaxLength, " Total", " " + t3.f0, " " + t3.f1, " " + t3.f2);
		Benchmark.printLine('-', "+", itemMaxLength, "", "", "", "");
		bestArray.add(new Tuple2<>(name, t3.f0));
		System.err.println();
	}

	private Tuple3<Long, Long, Long> getBestAvgMaxTime(List<Result> results) {
		long best = Long.MAX_VALUE;
		long sum = 0L;
		long max = Long.MIN_VALUE;
		for (Result result : results) {
			long time = result.totalTime;
			if (time < best) {
				best = time;
			}
			sum += time;
			if (time > max) {
				max = time;
			}
		}
		return new Tuple3<>(best, sum / results.size(), max);
	}

	private void printRow(List<Row> rowList){
		for (Row row : rowList) {
			System.out.println(row);
		}
	}

	private static class Result {

		private final long totalTime;

		private Result(long totalTime) {
			this.totalTime = totalTime;
		}
	}

}
