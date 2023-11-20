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

public class DruidDataSourceFactory extends PooledDataSourceFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DruidDataSourceFactory.class);

    private static DruidDataSource dataSource;

    @Override
    public void setProperties(Properties properties) {
        synchronized (DruidDataSourceFactory.class) {
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
                dataSource.setMaxActive(100);
                dataSource.setInitialSize(20);
                dataSource.setMinIdle(5);
                dataSource.setMaxWait(60000);

                dataSource.setTimeBetweenEvictionRunsMillis(60000);
                dataSource.setMinEvictableIdleTimeMillis(300000);
                dataSource.setTestWhileIdle(true);
                dataSource.setTestOnBorrow(true);
                dataSource.setTestOnReturn(false);

                dataSource.setPoolPreparedStatements(true);
                dataSource.setMaxOpenPreparedStatements(20);
                dataSource.setAsyncInit(true);

                // Slow SQL
                dataSource.setConnectionProperties("druid.stat.slowSqlMillis=3000");

                // Detect Connection Leakage
                // https://github.com/alibaba/druid/wiki/%E8%BF%9E%E6%8E%A5%E6%B3%84%E6%BC%8F%E7%9B%91%E6%B5%8B
                dataSource.setRemoveAbandoned(true);
                // Unit: second
                dataSource.setRemoveAbandonedTimeout(1800);
                dataSource.setLogAbandoned(true);

                // log executable SQL
                // -Ddruid.log.stmt.executableSql=true

                try {
                    dataSource.init();
                } catch (SQLException e) {
                    LOGGER.error("Failed to init druid data source", e);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public DataSource getDataSource() {
        return dataSource;
    }
}
