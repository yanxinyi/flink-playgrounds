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

package org.apache.flink.playgrounds.spendreport;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.expressions.TimeIntervalUnit;

import static org.apache.flink.table.api.Expressions.*;

public class SpendReport {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        
        TableEnvironment tEnv = TableEnvironment.create(settings);
        
        tEnv.executeSql("CREATE FUNCTION RandomUdf AS 'org.apache.flink.playgrounds.spendreport.RandomUdf'");

        tEnv.executeSql("CREATE TABLE datagenTable (\n" +
                "    id  INT\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'number-of-rows' = '5',\n" +
                "    'rows-per-second' = '1',\n" +
                "    'fields.id.kind' = 'sequence',\n" +
                "    'fields.id.start' = '1',\n" +
                "    'fields.id.end' = '5'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE print_table (\n" +
                "    id_in_bytes  VARBINARY,\n" +
                "    id  INT\n" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")");

        tEnv.executeSql("INSERT INTO print_table SELECT * FROM ( SELECT RandomUdf(`id`) AS `id_in_bytes`, `id` FROM datagenTable ) AS ET WHERE ET.`id_in_bytes` IS NOT NULL");
    }
}
