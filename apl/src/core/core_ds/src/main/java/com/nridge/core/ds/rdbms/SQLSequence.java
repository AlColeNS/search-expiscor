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
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.NSException;
import org.apache.commons.lang3.StringUtils;

/**
 * The SQLSequence is an abstract type that represents an
 * RDBMS SQL sequence object.  Vendor specific implementations
 * will override these methods to ensure they are customized
 * to their unique SQL statement syntax.
 *
 * @author Al Cole
 * @since 1.0
 */
public abstract class SQLSequence
{
    private final String NS_SEQUENCE_PREFIX = "seq";

    protected AppMgr mAppMgr;
    protected SQLConnection mSQLConnection;

    /**
     * Constructor that accepts an application manager.
     *
     * @param anAppMgr Application manager.
     */
    public SQLSequence(AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        mSQLConnection = null;
    }

    /**
     * Constructor that accepts an application manager and
     * SQL connection.
     *
     * @param anAppMgr Application manager.
     * @param aConnection SQL connection object.
     */
    public SQLSequence(AppMgr anAppMgr, SQLConnection aConnection)
    {
        mAppMgr = anAppMgr;
        mSQLConnection = aConnection;
    }

    /**
     * Returns a schema name for sequence objects based on the DB name
     * assigned to the bag and the data field.
     *
     * @param aBag Field bag with DB name assigned.
     * @param aFieldName Name of field to base sequence on.
     *
     * @return Schema name of the index object.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public String schemaName(DataBag aBag, String aFieldName)
        throws NSException
    {
        String dbName = aBag.getName();
        if (StringUtils.isEmpty(dbName))
            throw new NSException("The name for the data bag is undefined.");

        String sequenceName;
        if (mSQLConnection.isAutoNamingEnabled())
        {
            if (StringUtils.startsWith(dbName, NS_SEQUENCE_PREFIX))
            {
                if (StringUtils.contains(dbName, aFieldName))
                    sequenceName = dbName;
                else
                    sequenceName = String.format("%s_%s_%s", NS_SEQUENCE_PREFIX, dbName, aFieldName);
            }
            else
                sequenceName = String.format("%s_%s_%s", NS_SEQUENCE_PREFIX, dbName, aFieldName);
        }
        else
            sequenceName = String.format("%s_%s", dbName, aFieldName);

        return sequenceName;
    }

    /**
     * Returns the next sequence value for the data field parameter.
     * This method will typically trigger a silent comment against the
     * RDBMS index object and thus cannot be undone.
     *
     * @param aBag Field bag with DB name assigned.
     * @param aField Field to base the sequence operation name on.
     *
     * @return Next sequence value.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract int nextValue(DataBag aBag, DataField aField) throws NSException;

    /**
     * Returns the current sequence value for the data field parameter.
     *
     * @param aBag Field bag with DB name assigned.
     * @param aField Field to base the sequence operation name on.
     *
     * @return Next sequence value.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract int currentValue(DataBag aBag, DataField aField) throws NSException;

    /**
     * Returns a SQL fragment suitable for inclusion in a SQL insert statement.
     * This method will compose vendor-specific syntax for including a
     * column name that forces an auto-increment on the sequence during
     * the execution of the SQL insert statement.
     *
     * @param aBag Field bag with DB name assigned.
     * @param aField Field to base the sequence operation name on.
     *
     * @return Next sequence value.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract String insertValue(DataBag aBag, DataField aField) throws NSException;

    /**
     * Creates a sequence object in the RDBMS based on the DB name
     * assigned to the bag and the data field.
     *
     * @param aBag Field bag with DB name assigned.
     * @param aField Field to base the sequence name on.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract void create(DataBag aBag, DataField aField) throws NSException;

    /**
     * Drops a sequence object from the RDBMS based on the DB name
     * assigned to the bag and the data field.
     *
     * @param aBag Field bag with DB name assigned.
     * @param aField Field to base the sequence name on.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public void drop(DataBag aBag, DataField aField)
        throws NSException
    {
        String sequenceName = schemaName(aBag, aField.getName());
        mSQLConnection.execute(String.format("DROP SEQUENCE %s", sequenceName));
    }
}
