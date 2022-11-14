/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.benchmark;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.common.util.HiveVersionInfo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class AnalyzeTableRunner {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("The args should be: databaseName hiveConfFile");
            System.exit(1);
            return;
        }

        String database = args[0];
        String hiveConf = args[1];

        HiveCatalog catalog =
                new HiveCatalog("hive", database, hiveConf, HiveVersionInfo.getVersion());
        EnvironmentSettings environmentSettings = EnvironmentSettings.inBatchMode();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);
        tEnv.registerCatalog("hive", catalog);
        tEnv.useCatalog("hive");

        // flink stats analyze way.
        registerTpcdsStats(tEnv, catalog, database);
        System.out.println("Collecting stats done");
    }

    public static void registerTpcdsStats(
            TableEnvironment tEnv, HiveCatalog hiveCatalog, String database)
            throws TableNotExistException, ExecutionException, InterruptedException {
        System.out.println(String.format("begin to analyze table in database: %s", database));
        String[] tables = tEnv.listTables();
        if (tables == null || tables.length == 0) {
            throw new TableException(
                    String.format("There are no table exist in %s", tEnv.getCurrentCatalog()));
        }
        for (String table : tables) {
            System.out.println(String.format("Begin to compute statistics for table %s", table));
            ObjectPath path = new ObjectPath(database, table);
            Table hiveTable = hiveCatalog.getHiveTable(path);
            if (!isTablePartitioned(hiveTable)) {
                tEnv.executeSql(
                                String.format(
                                        "ANALYZE TABLE %s COMPUTE STATISTICS FOR ALL COLUMNS", table))
                        .await();
                System.out.println(
                        String.format("Compute statistics for table %s is finished", table));
            } else {
                List<String> partitionKeys = getFieldNames(hiveTable.getPartitionKeys());
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(String.format("ANALYZE TABLE %s PARTITION(", table));
                stringBuilder.append(partitionKeys.stream().collect(Collectors.joining(", ")));
                stringBuilder.append(") COMPUTE STATISTICS FOR ALL COLUMNS");
                tEnv.executeSql(stringBuilder.toString()).await();
                System.out.println(
                        String.format(
                                "Compute statistics for partition table %s is finished", table));
            }
        }
    }

    private static List<String> getFieldNames(List<FieldSchema> fieldSchemas) {
        List<String> names = new ArrayList(fieldSchemas.size());
        Iterator var2 = fieldSchemas.iterator();

        while (var2.hasNext()) {
            FieldSchema fs = (FieldSchema) var2.next();
            names.add(fs.getName());
        }

        return names;
    }

    private static boolean isTablePartitioned(Table hiveTable) {
        return hiveTable.getPartitionKeysSize() != 0;
    }
}
