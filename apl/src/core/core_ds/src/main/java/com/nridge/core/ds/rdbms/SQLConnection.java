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
import com.nridge.core.ds.rdbms.hsqldb.HDBSQLIndex;
import com.nridge.core.ds.rdbms.hsqldb.HDBSQLSequence;
import com.nridge.core.ds.rdbms.hsqldb.HDBSQLTable;
import com.nridge.core.ds.rdbms.mysql.MySQLIndex;
import com.nridge.core.ds.rdbms.mysql.MySQLSequence;
import com.nridge.core.ds.rdbms.mysql.MySQLTable;
import com.nridge.core.ds.rdbms.oracle.OracleIndex;
import com.nridge.core.ds.rdbms.oracle.OracleSequence;
import com.nridge.core.ds.rdbms.oracle.OracleTable;
import com.nridge.core.ds.rdbms.psql.PostgreSQLIndex;
import com.nridge.core.ds.rdbms.psql.PostgreSQLSequence;
import com.nridge.core.ds.rdbms.psql.PostgreSQLTable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.sql.*;
import java.util.ArrayList;

/**
 * A SQLConnection manages the connection for a JDBC connection
 * instance.  It offers a number of wrapper methods that simplify
 * the creation of a connection and abstracts the developer away
 * from the vendor specific SQL dialects.
 *
 * @since 1.0
 * @author Al Cole
 */
public class SQLConnection implements AutoCloseable
{
    public static final String VENDOR_MYSQL_NAME = "MySQL";
    public static final String VENDOR_ORACLE_NAME = "Oracle";
    public static final String VENDOR_HYPERSQL_NAME = "HSQL Database Engine";
    public static final String VENDOR_SQLSERVER_NAME = "Microsoft SQL Server";
    public static final String VENDOR_POSTGRESQL_NAME = "PostgreSQL";
    public static final String VENDOR_UNKNOWN_NAME = "Unknown";

    private AppMgr mAppMgr;
    private Connection mConnection;
    private boolean mIsAutoNamingEnabled;
    private boolean mIsAutoCommitEnabled;
    private boolean mIsStatementEscapingEnabled;
    private String mVendorName = VENDOR_UNKNOWN_NAME;
    private String mSQLStatement = StringUtils.EMPTY;

    /**
     * Constructor that accepts an application manager (for property
     * and logging) and an existing JDBC connection instance.  This
     * constructor was designed to work in concert with the
     * <i>SQLConnectionPool</i> class.
     *
     * @param anAppMgr Application manager.
     * @param aJDBCConnection JDBC connection.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public SQLConnection(AppMgr anAppMgr, Connection aJDBCConnection)
        throws NSException
    {
        mAppMgr = anAppMgr;
        Logger appLogger = mAppMgr.getLogger(this, "SQLConnection");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mConnection = aJDBCConnection;
        identifyVendor();
        setAutoNamingEnabledFlag(true);
        setStatementEscapingEnabledFlag(true);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

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
    public SQLConnection(AppMgr anAppMgr, String aPropertyPrefix)
        throws NSException
    {
        if ((anAppMgr == null) || (StringUtils.isEmpty(aPropertyPrefix)))
            throw new NSException("Application Manager or property prefix null - internal error.");

        mAppMgr = anAppMgr;
        Logger appLogger = mAppMgr.getLogger(this, "SQLConnection");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String jdbcURL = getConfigurationValue(aPropertyPrefix + ".jdbc_url");
        String jdbcDriver = getConfigurationValue(aPropertyPrefix + ".jdbc_driver");
        String jdbcAccount = getConfigurationValue(aPropertyPrefix + ".jdbc_account");
        String jdbcPassword = getConfigurationValue(aPropertyPrefix + ".jdbc_password", StringUtils.EMPTY);
        if (anAppMgr.getBoolean(aPropertyPrefix + ".jdbc_autocommit", false))
            mIsAutoCommitEnabled = true;
        open(jdbcURL, jdbcDriver, jdbcAccount, jdbcPassword);
        identifyVendor();
        setAutoNamingEnabledFlag(true);
        setStatementEscapingEnabledFlag(true);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Returns a string summary representation of a SQL connection.
     *
     * @return String summary representation of this SQL connection.
     */
    @Override
    public String toString()
    {
        String idName;

        if (mConnection == null)
            idName = "Connection - JDBC";
        else
            idName = "Connection - " + mVendorName;

        return idName;
    }

    private String getConfigurationValue(String aFieldName)
        throws NSException
    {
        String fieldValue = mAppMgr.getString(aFieldName);
        if (StringUtils.isEmpty(fieldValue))
            throw new NSException(aFieldName + ": RDBMS field is undefined.");
        else
            return fieldValue;
    }

    private String getConfigurationValue(String aFieldName, String aDefaultValue)
        throws NSException
    {
        String fieldValue = mAppMgr.getString(aFieldName);
        if (StringUtils.isEmpty(fieldValue))
            fieldValue = aDefaultValue;

        return fieldValue;
    }

    private void identifyVendor()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "identifyVendor");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mVendorName.equals(VENDOR_UNKNOWN_NAME))
        {
            try
            {
                mConnection.setAutoCommit(mIsAutoCommitEnabled);
                DatabaseMetaData dbMetaData = mConnection.getMetaData();
                mVendorName = dbMetaData.getDatabaseProductName();
                appLogger.debug("RDBMS vendor name is " + mVendorName);
            }
            catch (SQLException e)
            {
                mVendorName = VENDOR_UNKNOWN_NAME;
                throw new NSException("Unable to identify RDBMS vendor name: " + e.getMessage());
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Returns the application manager.
     *
     * @return Application manager.
     */
    public AppMgr getAppMgr()
    {
        return mAppMgr;
    }

    /**
     * Returns the RDBMS vendor name.  This name is obtained by the
     * JDBC meta data interface.
     *
     * @return RDBMS vendor name.
     */
    public String getVendorName()
    {
        return mVendorName;
    }

    /**
     * Assigns the table and index auto-naming boolean flag.
     * If <i>true</i>, then a standard naming convention
     * will be applied to schema objects created or
     * referenced in the RDBMS.
     *
     * @param aFlag Auto-naming boolean flag.
     */
    public void setAutoNamingEnabledFlag(boolean aFlag)
    {
        mIsAutoNamingEnabled = aFlag;
    }

    /**
     * Returns <i>true</i> if auto-naming is enabled or
     * <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isAutoNamingEnabled()
    {
        return mIsAutoNamingEnabled;
    }

    /**
     * Enables/disables SQL statement character escaping. This
     * setting is passed down to the JDBC <i>Connection</i>
     * object.
     *
     * @param aFlag <i>true</i> or <i>false</i>
     */
    public void setStatementEscapingEnabledFlag(boolean aFlag)
    {
        mIsStatementEscapingEnabled = aFlag;
    }

    /**
     * Returns <i>true</i> if the JDBC SQL statement escaping
     * feature is enabled or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isStatementEscapingEnabled()
    {
        return mIsStatementEscapingEnabled;
    }

    /**
     * Enables/disables transaction auto-commits within the
     * JDBC vendor driver. This setting is passed down to the
     * JDBC <i>Connection</i> object.
     *
     * @param aFlag <i>true</i> or <i>false</i>
     */
    public void setAutoCommitEnabledFlag(boolean aFlag)
        throws NSException
    {
        mIsAutoCommitEnabled = aFlag;
        try
        {
            mConnection.setAutoCommit(mIsAutoCommitEnabled);
        }
        catch (java.sql.SQLException e)
        {
            throw new NSException(e.getMessage(), e);
        }
    }

    /**
     * Returns <i>true</i> if the transaction auto-commit feature
     * is enabled or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isIsAutoCommitEnabled()
    {
        return mIsAutoCommitEnabled;
    }

    /**
     * Returns a reference to the JDBC <i>Connection</i>.
     *
     * @return JDBC connection instance.
     */
    public Connection getJDBCConnection()
    {
        return mConnection;
    }

    /**
     * Assigns the last SQL statement string.  This information
     * is usually referenced during exception handling and its
     * related logging.
     *
     * @param aSQLStatement SQL statement.
     */
    public void setLastStatement(String aSQLStatement)
    {
        mSQLStatement = aSQLStatement;
    }

    /**
     * Returns the SQL statement executed by this connection.
     *
     * @return SQL statement.
     */
    public String getLastStatement()
    {
        return mSQLStatement;
    }

    /**
     * Returns <i>true</i> if the RDBMS vendor is MySQL
     * or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isVendorMySQL()
    {
        return mVendorName.equalsIgnoreCase(VENDOR_MYSQL_NAME);
    }

    /**
     * Returns <i>true</i> if the RDBMS vendor is Oracle
     * or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isVendorOracle()
    {
        return mVendorName.equalsIgnoreCase(VENDOR_ORACLE_NAME);
    }

    /**
     * Returns <i>true</i> if the RDBMS vendor is PostgreSQL
     * or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isVendorPostgreSQL()
    {
        return mVendorName.equalsIgnoreCase(VENDOR_POSTGRESQL_NAME);
    }

    /**
     * Returns <i>true</i> if the RDBMS vendor is SQL Server
     * or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isVendorSQLServer()
    {
        return mVendorName.equalsIgnoreCase(VENDOR_SQLSERVER_NAME);
    }

    /**
     * Returns <i>true</i> if the RDBMS vendor is Hypersonic DB
     * or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isVendorHyperSQL()
    {
        return mVendorName.equalsIgnoreCase(VENDOR_HYPERSQL_NAME);
    }

    /**
     * Opens a JDBC connection with the RDBMS instance.
     *
     * @param aConnectionURI Connection URI.
     * @param aDriverName RDBMS vendor driver name.
     * @param anAccount RDBMS account name.
     * @param anPassword RDBMS account password.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public void open(String aConnectionURI, String aDriverName,
                     String anAccount, String anPassword)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "open");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        appLogger.debug(String.format("[%s] %s (%s/%s)", aDriverName, aConnectionURI,
                                      anAccount, anPassword));
        try
        {
            Class.forName(aDriverName).getConstructor().newInstance();
            mConnection = DriverManager.getConnection(aConnectionURI, anAccount, anPassword);
            identifyVendor();
        }
        catch (Exception e)
        {
            String errMsg = String.format("%s: %s", aConnectionURI, e.getMessage());
            throw new NSException(errMsg, e);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Returns an array of RDBMS schema names associated with
     * an opened connection.
     *
     * @return An array of schema names.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public ArrayList<String> getSchemaNames()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "getSchemaNames");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mSQLStatement = StringUtils.EMPTY;
        ArrayList<String> schemaNames = new ArrayList<String>();

        try
        {
            String schemaName;
            DatabaseMetaData dbMetaData = mConnection.getMetaData();
            ResultSet rsSchemas = dbMetaData.getSchemas();
            while (rsSchemas.next())
            {
                schemaName = rsSchemas.getString("TABLE_SCHEM");
                if (! StringUtils.isEmpty(schemaName))
                    schemaNames.add(schemaName);
            }
            rsSchemas.close();
        }
        catch (SQLException e)
        {
            throw new NSException("Unable to retrieve RDBMS schemas: " + e.getMessage(), e);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return schemaNames;
    }

    /**
     * Returns an array of RDBMS table names associated with
     * an opened connection.
     *
     * @return An array of schema names.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public ArrayList<String> getTableNames()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "getTableNames");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mSQLStatement = StringUtils.EMPTY;
        ArrayList<String> tableNames = new ArrayList<String>();

        try
        {
            String tableName;
            DatabaseMetaData dbMetaData = mConnection.getMetaData();
            ResultSet rsTables = dbMetaData.getTables(null, null, null, new String[]{"TABLE"});
            while (rsTables.next())
            {
                tableName = rsTables.getString("TABLE_NAME");
                if (! StringUtils.isEmpty(tableName))
                    tableNames.add(tableName);
            }
            rsTables.close();
        }
        catch (SQLException e)
        {
            throw new NSException("Unable to retrieve RDBMS table names: " + e.getMessage(), e);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return tableNames;
    }

    /**
     * Executes the SQL statement (via the underlying JDBC connection).
     *
     * @param aSQLStatement SQL statement.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public void execute(String aSQLStatement)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "execute");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mSQLStatement = aSQLStatement;
        appLogger.debug(mSQLStatement);

        Statement stmtUpdate = null;
        try
        {
            stmtUpdate = mConnection.createStatement();
            stmtUpdate.setEscapeProcessing(mIsStatementEscapingEnabled);
            stmtUpdate.executeUpdate(mSQLStatement);
        }
        catch (SQLException e)
        {
            throw new NSException("RDBMS Statement Error: " + mSQLStatement + " : " + e.getMessage(), e);
        }
        finally
        {
            if (stmtUpdate != null)
            {
                try { stmtUpdate.close(); } catch (SQLException ignored) { }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Clears all changes made since the previous commit/rollback
     * operation and releases any database locks currently held by
     * <i>SQLConnection</i>. This method should be used only when
     * the "jdbc_autocommit" property is assigned <i>false</i>.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public void rollback()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "rollback");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        try
        {
            mConnection.rollback();
        }
        catch (SQLException e)
        {
            throw new NSException("RDBMS Rollback Error: " + e.getMessage(), e);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Applies all changes made since the previous commit/rollback
     * permanent and releases any database locks currently held by
     * <i>SQLConnection</i>. This method should be used only when
     * the "jdbc_autocommit" property is assigned <i>false</i>.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public void commit()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "commit");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        try
        {
            mConnection.commit();
        }
        catch (SQLException e)
        {
            throw new NSException("RDBMS Commit Error: " + e.getMessage(), e);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * This factory method will return a <i>SQLTable</i> abstract
     * type representing an RDBMS vendor specific SQL table instance.
     *
     * @return SQL table instance.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public SQLTable newTable()
        throws NSException
    {
        SQLTable sqlTable;
        Logger appLogger = mAppMgr.getLogger(this, "newTable");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (isVendorHyperSQL())
            sqlTable = new HDBSQLTable(this);
        else if (isVendorPostgreSQL())
            sqlTable = new PostgreSQLTable(this);
        else if (isVendorMySQL())
            sqlTable = new MySQLTable(this);
        else if (isVendorOracle())
            sqlTable = new OracleTable(this);
        else
            throw new NSException("RDBMS Vendor is not supported.");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return sqlTable;
    }

    /**
     * This factory method will return a <i>SQLTable</i> abstract
     * type representing an RDBMS vendor specific SQL table instance.
     * The type parameter supports the designation of specialized
     * table implementations (e.g. in-memory).
     *
     * @param aType SQL table type.
     *
     * @return SQL table instance.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public SQLTable newTable(String aType)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "newTable");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        SQLTable sqlTable = newTable();

        sqlTable.setType(aType);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return sqlTable;
    }

    /**
     * This factory method will return a <i>SQLSequence</i> abstract
     * type representing an RDBMS vendor specific SQL sequence instance.
     *
     * @return SQL sequence instance.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public SQLSequence newSequence()
        throws NSException
    {
        SQLSequence sqlSequence;
        Logger appLogger = mAppMgr.getLogger(this, "newSequence");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (isVendorHyperSQL())
            sqlSequence = new HDBSQLSequence(mAppMgr, this);
        else if (isVendorPostgreSQL())
            sqlSequence = new PostgreSQLSequence(mAppMgr, this);
        else if (isVendorMySQL())
            sqlSequence = new MySQLSequence(mAppMgr, this);
        else if (isVendorOracle())
            sqlSequence = new OracleSequence(mAppMgr, this);
        else
            throw new NSException(mVendorName + ": RDBMS vendor is not supported.");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return sqlSequence;
    }

    /**
     * This factory method will return a {<i>SQLIndex</i> abstract
     * type representing an RDBMS vendor specific SQL index instance.
     *
     * @return SQL index instance.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public SQLIndex newIndex()
        throws NSException
    {
        SQLIndex sqlIndex;
        Logger appLogger = mAppMgr.getLogger(this, "newIndex");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (isVendorHyperSQL())
            sqlIndex = new HDBSQLIndex(this);
        else if (isVendorPostgreSQL())
            sqlIndex = new PostgreSQLIndex(this);
        else if (isVendorMySQL())
            sqlIndex = new MySQLIndex(this);
        else if (isVendorOracle())
            sqlIndex = new OracleIndex(this);
        else
            throw new NSException(mVendorName + ": RDBMS vendor is not supported.");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return sqlIndex;
    }

    /**
     * Releases this SQL Connection JDBC resources immediately
     * instead of waiting for them to be automatically released.
     * Calling the method close on a Connection object that is already
     * closed is a no-op. It is strongly recommended that an
     * application explicitly commits or rolls back an active transaction
     * prior to calling the close method. If the close method is called
     * and there is an active transaction, the results are vendor-defined.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public void close()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "close");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        try
        {
            mConnection.close();
        }
        catch (SQLException e)
        {
            throw new NSException("RDBMS Close Error: " + e.getMessage(), e);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Convenience method that invokes <code>close()</code> but ignores
     * any SQLException that my be thrown.
     */
    public void closeSilently()
    {
        Logger appLogger = mAppMgr.getLogger(this, "closeSilently");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        try
        {
            close();
        }
        catch (NSException e)
        {
            appLogger.error("RDBMS Close Error: " + e.getMessage(), e);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
