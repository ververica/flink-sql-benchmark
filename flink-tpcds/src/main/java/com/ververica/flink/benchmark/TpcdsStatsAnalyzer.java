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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.avatica.util.DateTimeUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Table statistics information for TPC-DS qualification test, TPC-DS tool version is v2.11.0. */
public class TpcdsStatsAnalyzer {
    private static final Map<String, String> COLUMN_NAME_MAPPING = new HashMap<>();

    static {
        // COLUMN_NAME_MAPPING.put("c_last_review_date", "c_last_review_date_sk");
    }

    private static Map<String, CatalogTableStats> analyzeStatistics(String file)
            throws IOException {
        final Map<String, CatalogTableStats> catalogTableStatsMap = new HashMap<>();

        List<String> lines = Files.readAllLines(new File(file).toPath());
        List<String> items = new ArrayList<>();
        for (String line : lines) {
            if (line.startsWith("hive>") && line.endsWith(";")) {
                if (!items.isEmpty()) {
                    Tuple2<String, CatalogTableStats> tableAndStats = generateTableStats(items);
                    catalogTableStatsMap.put(tableAndStats.f0, tableAndStats.f1);
                    items.clear();
                }
            }
            items.add(line);
        }

        return catalogTableStatsMap;
    }

    private static Tuple2<String, CatalogTableStats> generateTableStats(List<String> lines) {
        String tableLine = lines.get(0);
        checkArgument(tableLine.startsWith("hive>") && tableLine.endsWith(";"));
        tableLine = tableLine.substring(0, tableLine.length() - 1); // remove ;
        String[] words = tableLine.split(" ");
        String table = words[words.length - 1];
        // validate whether table does exist
        TpcdsSchemaProvider.getTableSchema(table);

        long totalSize = 0;
        long numRows = 0;
        final Map<String, CatalogColumnStatisticsDataBase> statisticsData = new HashMap<>();

        List<String> items = new ArrayList<>();
        String currentColumn = null;
        for (String line : lines) {
            line = line.trim().replaceAll("'", "");
            int statsIdx = line.indexOf("spark.sql.statistics.");
            if (statsIdx < 0) {
                if (!items.isEmpty()) {
                    CatalogColumnStatisticsDataBase columnStats =
                            generateColumnStats(table, currentColumn, items);
                    statisticsData.put(currentColumn, columnStats);
                    items.clear();
                }
                continue;
            }
            line = line.substring(statsIdx + "spark.sql.statistics.".length(), line.length() - 1);

            if (line.startsWith("colStats.")) {
                String colStatsLine = line.substring("colStats.".length());
                words = colStatsLine.split("\\.");
                String column = words[0];

                if (currentColumn != null && !currentColumn.equals(column)) {
                    CatalogColumnStatisticsDataBase columnStats =
                            generateColumnStats(table, currentColumn, items);
                    statisticsData.put(currentColumn, columnStats);
                    items.clear();
                } else {
                    System.out.println("[ERROR]" + table + " " + column + " " + currentColumn);
                }
                currentColumn = column;
                items.add(words[1]);
            } else if (line.startsWith("numRows")) {
                words = line.split("=");
                numRows = Long.parseLong(words[1]);
            } else if (line.startsWith("totalSize")) {
                words = line.split("=");
                totalSize = Long.parseLong(words[1]);
            }
        }
        CatalogTableStatistics tableStats = new CatalogTableStatistics(numRows, 0, totalSize, 0);
        return new Tuple2<>(
                table,
                new CatalogTableStats(tableStats, new CatalogColumnStatistics(statisticsData)));
    }

    private static CatalogColumnStatisticsDataBase generateColumnStats(
            String tableName, String column, List<String> items) {
        TpcdsSchema schema = TpcdsSchemaProvider.getTableSchema(tableName);
        String realColumn = COLUMN_NAME_MAPPING.getOrDefault(column, column);
        int columnIdx = schema.getFieldNames().indexOf(realColumn);
        if (columnIdx < 0) {
            throw new IllegalStateException(
                    "Column: " + realColumn + " is not found in Table: " + tableName);
        }

        String max = "0", min = "0", ndv = "0", nullCount = "0", maxLength = "0", avgLength = "0";
        for (String item : items) {
            String[] words = item.split("=");
            String statsType = words[0];
            String statsValue = words[1];
            switch (statsType) {
                case "max":
                    max = statsValue;
                    break;
                case "min":
                    min = statsValue;
                    break;
                case "maxLen":
                    maxLength = statsValue;
                    break;
                case "avgLen":
                    avgLength = statsValue;
                    break;
                case "nullCount":
                    nullCount = statsValue;
                    break;
                case "distinctCount":
                    ndv = statsValue;
                    break;
                default:
            }
        }
        
        System.out.println(String.format("%s.%s[max=%s, min=%s, maxLen=%s, avgLen=%s, nullCount=%s, distinctCount=%s", tableName, column, max, min, maxLength, avgLength, nullCount, ndv));

        LogicalType type = schema.getFieldTypes().get(columnIdx).getLogicalType();
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return new CatalogColumnStatisticsDataString(
                        Long.parseLong(maxLength),
                        Double.parseDouble(avgLength),
                        Long.parseLong(ndv),
                        Long.parseLong(nullCount));
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
                return new CatalogColumnStatisticsDataDouble(
                        Double.parseDouble(min),
                        Double.parseDouble(max),
                        Long.parseLong(ndv),
                        Long.parseLong(nullCount));
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return new CatalogColumnStatisticsDataLong(
                        Long.parseLong(min),
                        Long.parseLong(max),
                        Long.parseLong(ndv),
                        Long.parseLong(nullCount));
            case DATE:
                return new CatalogColumnStatisticsDataDate(
                        new Date(DateTimeUtils.dateStringToUnixDate(min)),
                        new Date(DateTimeUtils.dateStringToUnixDate(max)),
                        Long.parseLong(ndv),
                        Long.parseLong(nullCount));
            default:
                throw new IllegalStateException("Unsupported type: " + type);
        }
    }

    public static void registerTpcdsStats(TableEnvironment tEnv, String statsFile)
            throws IOException {
        Map<String, CatalogTableStats> catalogTableStatsMap = analyzeStatistics(statsFile);
        for (Map.Entry<String, CatalogTableStats> enrty : catalogTableStatsMap.entrySet()) {
            String table = enrty.getKey();
            CatalogTableStats catalogTableStats = enrty.getValue();
            catalogTableStats.register2Catalog(tEnv, table);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("statistics file is empty");
            System.exit(1);
            return;
        }
        String file = args[0];
        System.out.println(analyzeStatistics(file));
    }
}
