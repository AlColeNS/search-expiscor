/*
 * NorthRidge Software, LLC - Copyright (c) 2019.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.nridge.core.ds.rdbms;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

// http://www.javaranch.com/journal/200601/JDBCConnectionPooling.html

/**
 * The SQLConnectionPool is responsible for managing a pool of JDBC
 * connections for an application.  It makes extensive use of the
 * Apache DBCP interfaces for the implementation of the features.
 *
 * @see <a href="http://commons.apache.org/dbcp/configuration.html">Apache DBCP Configuration</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class SQLConnectionPool
{
    private AppMgr mAppMgr;
    private PoolingDataSource mDataSource;

    /**
     * Constructor accepts an application manager(for property
     * and logging) and a property prefix string.
     * <p>
     * The follow properties will be derived using the property
     * prefix:
     * </p>
     * <ul>
     *     <li>jdbc_url Defines the connection URI</li>
     *     <li>jdbc_driver Defines the vendor driver</li>
     *     <li>jdbc_account Defines the login account</li>
     *     <li>jdbc_password Defines the account password</li>
     *     <li>jdbc_autocommit If <i>true</i>, then enable auto-commits</li>
     * </ul>
     *
     * @param anAppMgr Application manager.
     * @param aPropertyPrefix Property prefix string.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public SQLConnectionPool(AppMgr anAppMgr, String aPropertyPrefix)
        throws NSException
    {
        if ((anAppMgr == null) || (StringUtils.isEmpty(aPropertyPrefix)))
            throw new NSException("AppMgr is null - internal error.");

        mAppMgr = anAppMgr;
        Logger appLogger = mAppMgr.getLogger(this, "SQLConnectionPool");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String jdbcURL = getConfigurationValue(anAppMgr, aPropertyPrefix + ".jdbc_url");
        String jdbcDriver = getConfigurationValue(anAppMgr, aPropertyPrefix + ".jdbc_driver");
        String jdbcAccount = getConfigurationValue(anAppMgr, aPropertyPrefix + ".jdbc_account");
        String jdbcPassword = getConfigurationValue(anAppMgr, aPropertyPrefix + ".jdbc_password");

        Properties poolProperties = new Properties();
        poolProperties.setProperty("username", jdbcAccount);
        poolProperties.setProperty("password", jdbcPassword);
        poolProperties.setProperty("url", jdbcURL);
        poolProperties.setProperty("driverClassName", jdbcDriver);

        String cfgName = "rdbms.jdbc_autocommit";
        String poolName = "defaultAutoCommit";
        String cfgValue = anAppMgr.getString(cfgName);
        if (StringUtils.isNotEmpty(cfgValue))
            poolProperties.setProperty(poolName, cfgValue);

// http://commons.apache.org/dbcp/configuration.html  (probably wrong see note below)
// http://docs.oracle.com/javase/7/docs/api/index.html

        cfgName = aPropertyPrefix + ".pool_transaction_isolation";
        poolName = "defaultTransactionIsolation";
        cfgValue = anAppMgr.getString(cfgName);
        if (StringUtils.isNotEmpty(cfgValue))
            poolProperties.setProperty(poolName, cfgValue);
        cfgName = aPropertyPrefix + ".pool_initial_size";
        poolName = "initialSize";
        cfgValue = anAppMgr.getString(cfgName);
        if (StringUtils.isNotEmpty(cfgValue))
            poolProperties.setProperty(poolName, cfgValue);
        cfgName = aPropertyPrefix + ".pool_max_active";
        poolName = "maxActive";
        cfgValue = anAppMgr.getString(cfgName);
        if (StringUtils.isNotEmpty(cfgValue))
            poolProperties.setProperty(poolName, cfgValue);
        cfgName = aPropertyPrefix + ".pool_max_idle";
        poolName = "maxIdle";
        cfgValue = anAppMgr.getString(cfgName);
        if (StringUtils.isNotEmpty(cfgValue))
            poolProperties.setProperty(poolName, cfgValue);
        cfgName = aPropertyPrefix + ".pool_min_idle";
        poolName = "minIdle";
        cfgValue = anAppMgr.getString(cfgName);
        if (StringUtils.isNotEmpty(cfgValue))
            poolProperties.setProperty(poolName, cfgValue);
        cfgName = aPropertyPrefix + ".pool_max_wait";
        poolName = "maxWait";
        cfgValue = anAppMgr.getString(cfgName);
        if (StringUtils.isNotEmpty(cfgValue))
            poolProperties.setProperty(poolName, cfgValue);
        cfgName = aPropertyPrefix + ".pool_validation_query";
        poolName = "validationQuery";
        cfgValue = anAppMgr.getString(cfgName);
        if (StringUtils.isNotEmpty(cfgValue))
            poolProperties.setProperty(poolName, cfgValue);
        cfgName = aPropertyPrefix + ".pool_test_on_borrow";
        poolName = "testOnBorrow";
        cfgValue = anAppMgr.getString(cfgName);
        if (StringUtils.isNotEmpty(cfgValue))
            poolProperties.setProperty(poolName, cfgValue);

        create(poolProperties, jdbcAccount, jdbcPassword);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Constructor that accepts a collection of parameters
     * related to the establishment of an RDBMS connection.
     *
     * @param aConnectionURI Connection URI.
     * @param aDriverName RDBMS vendor driver name.
     * @param anAccount RDBMS account name.
     * @param anPassword RDBMS account password.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public SQLConnectionPool(String aConnectionURI, String aDriverName,
                             String anAccount, String anPassword)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "SQLConnectionPool");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Properties poolProperties = new Properties();
        poolProperties.setProperty("username", anAccount);
        poolProperties.setProperty("password", anPassword);
        poolProperties.setProperty("url", aConnectionURI);
        poolProperties.setProperty("driverClassName", aDriverName);

        create(poolProperties, anAccount, anPassword);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private String getConfigurationValue(AppMgr anAppMgr, String aFieldName)
        throws NSException
    {
        String fieldValue = anAppMgr.getString(aFieldName);
        if (StringUtils.isEmpty(fieldValue))
            throw new NSException(aFieldName + ": RDBMS field is undefined.");
        else
            return fieldValue;
    }

/* Note: The following method failed to work with PostgreSQL (perhaps others).  You will
need to review the underlying source code of DBCP to see how the properties are used in
the DriverManagerConnectionFactory.  Based on the IDE debugger, it appeared that the account
and password properties were being ignored.  The property names may not match the JavaDocs
anymore. */

    private void create(Properties aProperties)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "create");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String driverClassName = aProperties.getProperty("driverClassName");
        try
        {
            Class.forName(driverClassName).getConstructor().newInstance();
        }
        catch (Exception e)
        {
            throw new NSException(String.format("%s: %s", driverClassName, e.getMessage()), e);
        }

        String validationQuery = aProperties.getProperty("validationQuery");
        boolean isAutoCommit = StrUtl.stringToBoolean("defaultAutoCommit");

        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(aProperties.getProperty("url"), aProperties);
        GenericObjectPool connectionPool = new GenericObjectPool(null);

// When you pass an ObjectPool into the PoolableConnectionFactory, it will automatically
// register itself as the PoolableObjectFactory for that pool.

        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, connectionPool, null,
            validationQuery, false, isAutoCommit);
        mDataSource = new PoolingDataSource(connectionPool);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void create(Properties aProperties, String anAccount, String anPassword)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "create");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String driverClassName = aProperties.getProperty("driverClassName");
        try
        {
            Class.forName(driverClassName).getConstructor().newInstance();
        }
        catch (Exception e)
        {
            throw new NSException(String.format("%s: %s", driverClassName, e.getMessage()), e);
        }

        String validationQuery = aProperties.getProperty("validationQuery");
        boolean isAutoCommit = StrUtl.stringToBoolean("defaultAutoCommit");

        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(aProperties.getProperty("url"),
            anAccount, anPassword);
        GenericObjectPool connectionPool = new GenericObjectPool(null);

// When you pass an ObjectPool into the PoolableConnectionFactory, it will automatically
// register itself as the PoolableObjectFactory for that pool.

        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, connectionPool, null,
            validationQuery, false, isAutoCommit);
        mDataSource = new PoolingDataSource(connectionPool);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Returns a <i>SQLConnection</i> after obtaining a JDBC connection
     * from the connection pool.
     *
     * @return SQL connection.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public SQLConnection getSQLConnection()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "getSQLConnection");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        try
        {
            Connection jdbcConnection = mDataSource.getConnection();
            appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
            return new SQLConnection(mAppMgr, jdbcConnection);
        }
        catch (SQLException e)
        {
            throw new NSException(String.format("RDBMS Data Source Error: %s", e.getMessage()), e);
        }
    }

    /**
     * Returns a <i>SQLConnection</i> after obtaining a JDBC connection
     * from the connection pool
     *
     * @param anAccount RDBMS account name.
     * @param anPassword RDBMS account password.
     *
     * @return SQL connection.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public SQLConnection getSQLConnection(String anAccount, String anPassword)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "getSQLConnection");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        try
        {
            Connection jdbcConnection = mDataSource.getConnection(anAccount, anPassword);
            appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
            return new SQLConnection(mAppMgr, jdbcConnection);
        }
        catch (SQLException e)
        {
            throw new NSException(String.format("RDBMS Data Source Error: %s", e.getMessage()), e);
        }
    }
}
