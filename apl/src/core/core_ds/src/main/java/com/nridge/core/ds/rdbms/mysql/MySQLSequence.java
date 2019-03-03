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

package com.nridge.core.ds.rdbms.mysql;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.NSException;
import com.nridge.core.ds.rdbms.SQL;
import com.nridge.core.ds.rdbms.SQLConnection;
import com.nridge.core.ds.rdbms.SQLTable;
import com.nridge.core.ds.rdbms.SQLSequence;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Implements the MySQL RDBMS interfaces for sequence
 * operations.  Since these methods are specific to a
 * particular RDBMS vendor, an application developer
 * is encouraged to use the abstracted <i>SQLTable</i>
 * and <i>SQLSequence</i> classes instead.
 *
 * @author Al Cole
 * @since 1.0
 */
public class MySQLSequence extends SQLSequence
{
    /**
     * Constructor that accepts an application manager and
     * SQL connection.
     *
     * @param anAppMgr Application manager.
     * @param aConnection SQL connection object.
     */
    public MySQLSequence(AppMgr anAppMgr, SQLConnection aConnection)
    {
        super(anAppMgr, aConnection);
    }

    /**
     * Returns the next sequence value for the data field parameter.
     * This method will typically trigger a silent comment against the
     * RDBMS index object and thus cannot be undone.
     *
     * @param aBag   Field bag with DB name assigned.
     * @param aField Field to base the sequence operation name on.
     * @return Next sequence value.
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public int nextValue(DataBag aBag, DataField aField) throws NSException
    {
        int sequenceValue = SQL.VALUE_IS_INVALID;
        Logger appLogger = mAppMgr.getLogger(this, "nextValue");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (SQL.isSequenceManaged(aField))
        {
            String sequenceName = schemaName(aBag, aField.getName());
            mSQLConnection.execute(String.format("LOCK TABLE %s WRITE", sequenceName));
            mSQLConnection.execute(String.format("UPDATE %s SET nextval = nextval+1", sequenceName));
            SQLTable sqlTable = mSQLConnection.newTable();
            ResultSet resultSet = sqlTable.select(String.format("SELECT nextval FROM %s", sequenceName));
            try
            {
                if ((resultSet != null) && (resultSet.next()))
                {
                    sequenceValue = resultSet.getInt(1);
                    try { resultSet.close(); } catch (SQLException ignored) { }
                    mSQLConnection.commit();
                }
                mSQLConnection.execute("UNLOCK TABLES");
                appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
            }
            catch (SQLException e)
            {
                mSQLConnection.rollback();
                mSQLConnection.execute("UNLOCK TABLES");
                throw new NSException("SQL ResulSet Error: " + e.getMessage(), e);
            }
        }

        return sequenceValue;
    }

    /**
     * Returns the current sequence value for the data field parameter.
     *
     * @param aBag   Field bag with DB name assigned.
     * @param aField Field to base the sequence operation name on.
     * @return Next sequence value.
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public int currentValue(DataBag aBag, DataField aField) throws NSException
    {
        int sequenceValue = SQL.VALUE_IS_INVALID;
        Logger appLogger = mAppMgr.getLogger(this, "currentValue");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (SQL.isSequenceManaged(aField))
        {
            String sequenceName = schemaName(aBag, aField.getName());
            mSQLConnection.execute(String.format("LOCK TABLE %s READ", sequenceName));
            SQLTable sqlTable = mSQLConnection.newTable();
            ResultSet resultSet = sqlTable.select(String.format("SELECT nextval FROM %s", sequenceName));
            try
            {
                if ((resultSet != null) && (resultSet.next()))
                {
                    sequenceValue = resultSet.getInt(1);
                    try { resultSet.close(); } catch (SQLException ignored) { }
                    mSQLConnection.commit();
                }
                mSQLConnection.execute("UNLOCK TABLES");
                appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
            }
            catch (SQLException e)
            {
                mSQLConnection.rollback();
                mSQLConnection.execute("UNLOCK TABLES");
                throw new NSException("SQL ResulSet Error: " + e.getMessage(), e);
            }
        }

        return sequenceValue;
    }

    /**
     * Returns a SQL fragment suitable for inclusion in a SQL insert statement.
     * This method will compose vendor-specific syntax for including a
     * column name that forces an auto-increment on the sequence during
     * the execution of the SQL insert statement.
     *
     * @param aBag   Field bag with DB name assigned.
     * @param aField Field to base the sequence operation name on.
     * @return Next sequence value.
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public String insertValue(DataBag aBag, DataField aField) throws NSException
    {
        String sqlValue;
        Logger appLogger = mAppMgr.getLogger(this, "insertValue");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (SQL.isSequenceImplicit(aField))
            sqlValue = StringUtils.EMPTY;
        else if (SQL.isSequenceExplicit(aField))
            sqlValue = Integer.toString(nextValue(aBag, aField));
        else
            sqlValue = aField.getValue();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return sqlValue;
    }

    /**
     * Drops a sequence object from the RDBMS based on the DB name
     * assigned to the bag and the data field.
     *
     * @param aBag Field bag with DB name assigned.
     * @param aField Field to base the sequence name on.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public void drop(DataBag aBag, DataField aField)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "drop");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (SQL.isSequenceExplicit(aField))
        {
            String sequenceName = schemaName(aBag, aField.getName());
            mSQLConnection.execute(String.format("DROP TABLE %s", sequenceName));
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Creates a sequence object in the RDBMS based on the DB name
     * assigned to the bag and the data field.
     *
     * @param aBag   Field bag with DB name assigned.
     * @param aField Field to base the sequence name on.
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public void create(DataBag aBag, DataField aField) throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "create");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (SQL.isSequenceExplicit(aField))
        {
            String sequenceName = schemaName(aBag, aField.getName());
            mSQLConnection.execute(String.format("CREATE TABLE %s (nextval int not null)", sequenceName));
            mSQLConnection.execute(String.format("INSERT INTO %s (nextval) VALUES (%d)",
                                                 sequenceName, SQL.getSequenceSeed(aField)));
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
