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

import org.quartz.SchedulerException;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * <p>
 * A <code>ConnectionProvider</code> implementation that creates its own
 * pool of connections.
 * </p>
 *
 * <p>
 * This class uses Druid (https://github.com/alibaba/druid) as
 * the underlying pool implementation.</p>
 *
 * @see DBConnectionManager
 * @see ConnectionProvider
 *
 * @author ldang
 */
public class DruidPoolingConnectionProvider implements PoolingConnectionProvider {

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Constants.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Data members.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    private DruidDataSource datasource;

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Constructors.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    public DruidPoolingConnectionProvider(String dbDriver, String dbURL,
                                             String dbUser, String dbPassword, int maxActive,
                                             String dbValidationQuery) throws SQLException, SchedulerException {
        initialize(
                dbDriver, dbURL, dbUser, dbPassword,
                maxActive, dbValidationQuery);
    }

    /**
     * Create a connection pool using the given properties.
     *
     * <p>
     * The properties passed should contain:
     * <UL>
     * <LI>{@link #DB_DRIVER}- The database driver class name
     * <LI>{@link #DB_URL}- The database URL
     * <LI>{@link #DB_USER}- The database user
     * <LI>{@link #DB_PASSWORD}- The database password
     * <LI>{@link #DB_MAX_CONNECTIONS}- The maximum # connections in the pool,
     * optional
     * <LI>{@link #DB_VALIDATION_QUERY}- The sql validation query, optional
     * </UL>
     * </p>
     *
     * @param config
     *            configuration properties
     */
    public DruidPoolingConnectionProvider(Properties config) throws SchedulerException, SQLException {
        PropertiesParser cfg = new PropertiesParser(config);
        initialize(
                cfg.getStringProperty(DB_DRIVER),
                cfg.getStringProperty(DB_URL),
                cfg.getStringProperty(DB_USER, ""),
                cfg.getStringProperty(DB_PASSWORD, ""),
                cfg.getIntProperty(DB_MAX_CONNECTIONS, DEFAULT_DB_MAX_CONNECTIONS),
                cfg.getStringProperty(DB_VALIDATION_QUERY));
    }
    
    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * 
     * Interface.
     * 
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * Create the underlying Druid ComboPooledDataSource with the 
     * default supported properties.
     * @throws SchedulerException
     */
    private void initialize(
            String dbDriver,
            String dbURL,
            String dbUser,
            String dbPassword,
            int maxActive,
            String dbValidationQuery) throws SQLException, SchedulerException {
        if (dbURL == null) {
            throw new SQLException(
                    "DBPool could not be created: DB URL cannot be null");
        }

        if (dbDriver == null) {
            throw new SQLException(
                    "DBPool '" + dbURL + "' could not be created: " +
                            "DB driver class name cannot be null!");
        }

        if (maxActive < 0) {
            throw new SQLException(
                    "DBPool '" + dbURL + "' could not be created: " +
                            "Max connections must be greater than zero!");
        }


        datasource = new DruidDataSource();
        datasource.setDriverClassName(dbDriver);
        datasource.setUrl(dbURL);
        datasource.setUsername(dbUser);
        datasource.setPassword(dbPassword);
        datasource.setMaxActive(maxActive);

        if (dbValidationQuery != null) {
            datasource.setValidationQuery(dbValidationQuery);
        }
    }

    /**
     * Get the Druid DruidDataSource created during initialization.
     *
     * <p>
     * This can be used to set additional data source properties in a 
     * subclass's constructor.
     * </p>
     */
    @Override
    public DruidDataSource getDataSource() {
        return datasource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return datasource.getConnection();
    }

    @Override
    public void shutdown() throws SQLException {
        datasource.close();
    }

    @Override
    public void initialize() throws SQLException {
        // do nothing, already initialized during constructor call
    }
}
