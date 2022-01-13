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
import org.apache.flink.table.catalog.hive.HiveCatalog;

import org.apache.hive.common.util.HiveVersionInfo;

public class UpdateHiveCatalogStats {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("The args should be: databaseName hiveConfFile statsFile");
            System.exit(1);
            return;
        }

        String database = args[0];
        String hiveConf = args[1];
        String statsFile = args[2];

        HiveCatalog catalog =
                new HiveCatalog("hive", database, hiveConf, HiveVersionInfo.getVersion());
        EnvironmentSettings environmentSettings = EnvironmentSettings.inBatchMode();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);
        tEnv.registerCatalog("hive", catalog);
        tEnv.useCatalog("hive");
        TpcdsStatsAnalyzer.registerTpcdsStats(tEnv, statsFile);
        System.out.println("done");
    }
}
