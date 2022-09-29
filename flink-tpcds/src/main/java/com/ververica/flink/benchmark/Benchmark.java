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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hive.common.util.HiveVersionInfo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static com.ververica.flink.benchmark.QueryUtil.getQueries;
import static java.util.Objects.requireNonNull;

public class Benchmark {

	private static final Option HIVE_CONF = new Option("c", "hive_conf", true,
			"conf of hive.");

	private static final Option DATABASE = new Option("d", "database", true,
			"database of hive.");

	private static final Option LOCATION = new Option("l", "location", true,
			"sql query path.");

	private static final Option QUERIES = new Option("q", "queries", true,
			"sql query names. If the value is 'all', all queries will be executed.");

	private static final Option ITERATIONS = new Option("i", "iterations", true,
			"The number of iterations that will be run per case, default is 1.");

	private static final Option PARALLELISM = new Option("p", "parallelism", true,
			"The parallelism, default is 800.");

    private static final Option MODE =
            new Option("m", "mode", true, "mode: 'execute' or 'explain'");

	public static void main(String[] args) throws ParseException {
		Options options = getOptions();
		DefaultParser parser = new DefaultParser();
		CommandLine line = parser.parse(options, args, true);
        System.out.println("args: " + String.join(" ", args));

        String mode = line.getOptionValue(MODE.getOpt(), "execute");
        TableEnvironment tEnv =
                setUpEnv(
                        requireNonNull(line.getOptionValue(HIVE_CONF.getOpt())),
                        requireNonNull(line.getOptionValue(DATABASE.getOpt())),
                        Integer.parseInt(line.getOptionValue(PARALLELISM.getOpt(), "800")));
        LinkedHashMap<String, String> queries =
                getQueries(
                        line.getOptionValue(LOCATION.getOpt()),
                        line.getOptionValue(QUERIES.getOpt()));

		switch (mode) {
			case "explain":
				explain(tEnv, queries);
				break;
			case "execute":
				run(tEnv, queries, Integer.parseInt(line.getOptionValue(ITERATIONS.getOpt(), "1")));
				break;
			default:
				throw new RuntimeException(String.format("Mode '%s' doesn't support now, please changing value of --mode to execute or explain.", mode));
		}
	}

	private static void run(TableEnvironment tEnv, LinkedHashMap<String, String> queries, int iterations) {
		List<Tuple2<String, Long>> bestArray = new ArrayList<>();
		queries.forEach((name, sql) -> {
			tEnv.getConfig().getConfiguration().set(PipelineOptions.NAME, name);
			System.out.println("Start run query: " + name);
			Runner runner = new Runner(name, sql, iterations, tEnv);
			runner.run(bestArray);
		});
		printSummary(bestArray);
	}

    private static void explain(TableEnvironment tEnv, LinkedHashMap<String, String> queries) {
        List<Tuple2<String, Long>> bestArray = new ArrayList<>();
        queries.forEach(
                (name, sql) -> {
                    System.out.println("Start explain query: " + name);
                    System.out.println(tEnv.explainSql(sql, ExplainDetail.ESTIMATED_COST));
                });
        printSummary(bestArray);
    }

	private static void printSummary(List<Tuple2<String, Long>> bestArray) {
		if (bestArray.isEmpty()) {
			return;
		}
		System.err.println("--------------- tpcds Results ---------------");
		int itemMaxLength = 20;
		System.err.println();
		long total = 0L;
		double product = 1d;
		printLine('-', "+", itemMaxLength, "", "");
		printLine(' ', "|", itemMaxLength, " " + "tpcds sql", " Time(ms)");
		printLine('-', "+", itemMaxLength, "", "");

		for (Tuple2<String, Long> tuple2 : bestArray) {
			printLine(' ', "|", itemMaxLength, tuple2.f0, String.valueOf(tuple2.f1));
			total += tuple2.f1;
			product = product * tuple2.f1 / 1000d;
		}

		printLine(' ', "|", itemMaxLength, "Total", String.valueOf(total));
		printLine(' ', "|", itemMaxLength, "Average", String.valueOf(total / bestArray.size()));
		printLine(' ', "|", itemMaxLength, "GeoMean", String.valueOf((java.lang.Math.pow(product, 1d / bestArray.size()) * 1000)));
		printLine('-', "+", itemMaxLength, "", "");

		System.err.println();
	}

	static void printLine(
			char charToFill,
			String separator,
			int itemMaxLength,
			String... items) {
		StringBuilder builder = new StringBuilder();
		for (String item : items) {
			builder.append(separator);
			builder.append(item);
			int left = itemMaxLength - item.length() - separator.length();
			for (int i = 0; i < left; i++) {
				builder.append(charToFill);
			}
		}
		builder.append(separator);
		System.err.println(builder.toString());
	}

	private static TableEnvironment setUpEnv(String hiveConf, String database, int parallelism) {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
		TableEnvironment tEnv = TableEnvironment.create(settings);

		tEnv.getConfig().addConfiguration(GlobalConfiguration.loadConfiguration());

		HiveCatalog catalog = new HiveCatalog("hive", database, hiveConf, HiveVersionInfo.getVersion());
		tEnv.registerCatalog("hive", catalog);
		tEnv.useCatalog("hive");
		return tEnv;
	}

	private static Options getOptions() {
		Options options = new Options();
		options.addOption(HIVE_CONF);
		options.addOption(DATABASE);
		options.addOption(LOCATION);
		options.addOption(QUERIES);
		options.addOption(ITERATIONS);
		options.addOption(PARALLELISM);
        options.addOption(MODE);
		return options;
	}
}
