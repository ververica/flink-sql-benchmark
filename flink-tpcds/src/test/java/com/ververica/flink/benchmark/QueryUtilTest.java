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

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.ververica.flink.benchmark.QueryUtil.getQueries;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link QueryUtil}.
 */
public class QueryUtilTest {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Test
	public void testInternalAll() {
		LinkedHashMap<String, String> queries = getQueries(null, null);
		assertQueries(queries, 102);
	}

	@Test
	public void testInternalSelect() {
		LinkedHashMap<String, String> queries = getQueries(null, "q1.sql,q14b.sql,q95.sql");
		assertQueries(queries, 3);
	}

	@Test
	public void testAll() throws IOException {
		String dir = prepareOutFile();
		LinkedHashMap<String, String> queries = getQueries(dir, null);
		assertQueries(queries, 102);
	}

	@Test
	public void testSelect() throws IOException {
		String dir = prepareOutFile();
		LinkedHashMap<String, String> queries = getQueries(dir, "q1.sql,q14b.sql,q95.sql");
		assertQueries(queries, 3);
	}

	private String prepareOutFile() throws IOException {
		LinkedHashMap<String, String> queries = getQueries(null, null);
		File dir = TEMPORARY_FOLDER.newFolder();
		for (Map.Entry<String, String> e : queries.entrySet()) {
			Files.write(
					new File(dir, e.getKey()).toPath(),
					e.getValue().getBytes(StandardCharsets.UTF_8));
		}
		return dir.getAbsolutePath();
	}

	private void assertQueries(LinkedHashMap<String, String> queries, int size) {
		assertEquals(size, queries.size());
		queries.forEach((name, sql) -> {
			assertNotNull(name);
			assertTrue(name.length() > 0);

			assertNotNull(sql);
			assertTrue(sql.length() > 0);
		});
	}
}
