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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

class QueryUtil {

	static LinkedHashMap<String, String> getQueries(String location, String queries) {
		LinkedHashMap<String, Supplier<InputStream>> sql = new LinkedHashMap<>();
		List<String> queryList = queries == null ? null : Arrays.asList(queries.split(","));
		if (location == null) {
			for (int i = 1; i < 100; i++) {
				String name = "q" + i + ".sql";
				ClassLoader cl = Benchmark.class.getClassLoader();
				String path = "queries/" + name;
				if (cl.getResource(path) == null) {
					String a = "q" + i + "a.sql";
					sql.put(a, () -> cl.getResourceAsStream("queries/" + a));
					String b = "q" + i + "b.sql";
					sql.put(b, () -> cl.getResourceAsStream("queries/" + b));
				} else {
					sql.put(name, () -> cl.getResourceAsStream(path));
				}
			}
		} else {
			Stream<File> files = queryList == null ?
					Arrays.stream(requireNonNull(new File(location).listFiles())) :
					queryList.stream().map(file -> new File(location, file));
			files.forEach(file -> sql.put(file.getName(), () -> {
				try {
					return new FileInputStream(file);
				} catch (FileNotFoundException e) {
					return null;
				}
			}));
		}
		LinkedHashMap<String, String> ret = new LinkedHashMap<>();
		sql.forEach((name, supplier) -> {
			if (queryList == null || queryList.contains(name)) {
				InputStream in = supplier.get();
				if (in != null) {
					ret.put(name, streamToString(in));
				}
			}
		});
		return ret;
	}

	private static String streamToString(InputStream inputStream) {
		BufferedInputStream in = new BufferedInputStream(inputStream);
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		try {
			int c;
			while ((c = in.read()) != -1) {
				outStream.write(c);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				in.close();
			} catch (IOException ignored) {
			}
		}
		return new String(outStream.toByteArray(), StandardCharsets.UTF_8);
	}
}
