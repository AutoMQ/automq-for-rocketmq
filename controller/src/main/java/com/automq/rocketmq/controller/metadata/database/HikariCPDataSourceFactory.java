/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.rocketmq.controller.metadata.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.ibatis.datasource.pooled.PooledDataSourceFactory;

public class HikariCPDataSourceFactory extends PooledDataSourceFactory {
    private static HikariDataSource dataSource;

    @Override
    public void setProperties(Properties properties) {
        synchronized (HikariDataSource.class) {
            if (null == dataSource) {
                HikariConfig config = new HikariConfig(properties);
                config.setMaximumPoolSize(10);
                config.setIdleTimeout(100);
                config.setLeakDetectionThreshold(3);
                dataSource = new HikariDataSource(config);
            }
        }
    }

    @Override
    public DataSource getDataSource() {
        return dataSource;
    }
}
