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

package com.automq.rocketmq.metadata;

import com.alibaba.druid.pool.DruidDataSource;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.ibatis.datasource.pooled.PooledDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HikariCPDataSourceFactory extends PooledDataSourceFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HikariCPDataSourceFactory.class);

    private static DruidDataSource dataSource;

    @Override
    public void setProperties(Properties properties) {
        synchronized (HikariCPDataSourceFactory.class) {
            if (null == dataSource) {
                dataSource = new DruidDataSource();
                dataSource.setUrl(properties.getProperty("jdbcUrl"));
                dataSource.setUsername(properties.getProperty("username"));
                dataSource.setPassword(properties.getProperty("password"));
                try {
                    dataSource.setFilters("stat,slf4j");
                } catch (SQLException e) {
                    LOGGER.error("Failed to set stat filter", e);
                }
                dataSource.setMaxActive(20);
                dataSource.setInitialSize(5);
                dataSource.setMinIdle(1);
                dataSource.setMaxWait(6000);

                dataSource.setTimeBetweenEvictionRunsMillis(60000);
                dataSource.setMinEvictableIdleTimeMillis(300000);
                dataSource.setTestWhileIdle(true);
                dataSource.setTestOnBorrow(true);
                dataSource.setTestOnReturn(false);

                dataSource.setPoolPreparedStatements(true);
                dataSource.setMaxOpenPreparedStatements(20);
                dataSource.setAsyncInit(true);
            }
        }
    }

    @Override
    public DataSource getDataSource() {
        return dataSource;
    }
}
