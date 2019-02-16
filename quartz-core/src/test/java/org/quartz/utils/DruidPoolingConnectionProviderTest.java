
/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package org.quartz.utils;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.quartz.integrations.tests.JdbcQuartzDerbyUtilities;
import org.quartz.integrations.tests.QuartzDatabaseTestSupport;

import com.alibaba.druid.pool.DruidDataSource;

import java.util.Properties;

/**
 * A integration test to ensure PoolConnectionProvider is working properly.
 */
public class DruidPoolingConnectionProviderTest extends QuartzDatabaseTestSupport {
    boolean testConnectionProviderClass = false;

    @Test
    public void testDruidCpPoolProviderWithExtraProps() throws Exception {
        validateDruidCpPoolProviderClassWithExtraProps();

        // Turn flag on for next test.
        testConnectionProviderClass = true;
    }

    @Test
    public void testDruidCpPoolProviderClassWithExtraProps() throws Exception {
        validateDruidCpPoolProviderClassWithExtraProps();

        // Turn flag off for next test.
        testConnectionProviderClass = false;
    }

    private void validateDruidCpPoolProviderClassWithExtraProps() throws Exception {
        DBConnectionManager dbManager = DBConnectionManager.getInstance();
        ConnectionProvider provider = dbManager.getConnectionProvider("myDS");

        DruidDataSource ds = ((DruidPoolingConnectionProvider)provider).getDataSource();
        

        Assert.assertThat(ds.getDriverClassName(), Matchers.is("org.apache.derby.jdbc.ClientDriver"));
        Assert.assertThat(ds.getUrl(), Matchers.is(JdbcQuartzDerbyUtilities.DATABASE_CONNECTION_PREFIX));
        Assert.assertThat(ds.getUsername(), Matchers.is("quartz"));
        Assert.assertThat(ds.getPassword(), Matchers.is("quartz"));
        Assert.assertThat(ds.getMaxActive(), Matchers.is(5));

        Assert.assertThat(Integer.parseInt(ds.getMinEvictableIdleTimeMillis() + ""), Matchers.is(300000));
    }



    @Override
    protected Properties createSchedulerProperties() {
        Properties properties = new Properties();
        properties.put("org.quartz.scheduler.instanceName","TestScheduler");
        properties.put("org.quartz.scheduler.instanceId","AUTO");
        properties.put("org.quartz.scheduler.skipUpdateCheck","true");
        properties.put("org.quartz.threadPool.class","org.quartz.simpl.SimpleThreadPool");
        properties.put("org.quartz.threadPool.threadCount","12");
        properties.put("org.quartz.threadPool.threadPriority","5");
        properties.put("org.quartz.jobStore.misfireThreshold","10000");
        properties.put("org.quartz.jobStore.class","org.quartz.impl.jdbcjobstore.JobStoreTX");
        properties.put("org.quartz.jobStore.driverDelegateClass","org.quartz.impl.jdbcjobstore.StdJDBCDelegate");
        properties.put("org.quartz.jobStore.useProperties","true");
        properties.put("org.quartz.jobStore.dataSource","myDS");
        properties.put("org.quartz.jobStore.tablePrefix","QRTZ_");
        properties.put("org.quartz.jobStore.isClustered", "false");

        properties.put("org.quartz.dataSource.myDS.provider", "druid");

        if (testConnectionProviderClass)
            properties.put("org.quartz.dataSource.myDS.connectionProvider.class", "org.quartz.utils.DruidPoolingConnectionProvider");

        properties.put("org.quartz.dataSource.myDS.provider", "druid");
        properties.put("org.quartz.dataSource.myDS.driver", "org.apache.derby.jdbc.ClientDriver");
        properties.put("org.quartz.dataSource.myDS.URL",JdbcQuartzDerbyUtilities.DATABASE_CONNECTION_PREFIX);
        properties.put("org.quartz.dataSource.myDS.username","quartz");
        properties.put("org.quartz.dataSource.myDS.password","quartz");
        properties.put("org.quartz.dataSource.myDS.maxConnections","5");

        // Set extra properties
        properties.put("org.quartz.dataSource.myDS.minEvictableIdleTimeMillis","300000");

        return properties;
    }
}