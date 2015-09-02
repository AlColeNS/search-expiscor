/*
 * NorthRidge Software, LLC - Copyright (c) 2015.
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

import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.NSException;
import org.apache.commons.lang3.StringUtils;

/**
 * The SQLIndex is an abstract type that represents an RDBMS
 * SQL index object.  Vendor specific implementations will
 * override these methods to ensure they are customized to
 * their unique SQL statement syntax.
 *
 * @since 1.0
 * @author Al Cole
 */
public abstract class SQLIndex
{
    private final String NS_INDEX_PREFIX = "idx";

    protected SQLConnection mSQLConnection;

    /**
     * Default constructor.
     */
    public SQLIndex()
    {
        mSQLConnection = null;
    }

    /**
     * Constructor that accepts a SQL connection.
     *
     * @param aConnection SQL connection.
     */
    public SQLIndex(SQLConnection aConnection)
    {
        mSQLConnection = aConnection;
    }

    /**
     * Returns a schema name for index objects based on the DB name
     * assigned to the bag and the persistent field.
     *
     * @param aBag Field bag with DB name assigned.
     * @param aField Field to base the index name on.
     *
     * @return Schema name of the index object.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public String schemaName(DataBag aBag, DataField aField)
        throws NSException
    {
        String dbName = aBag.getName();
        if (StringUtils.isEmpty(dbName))
            throw new NSException("The name for the persistent bag is undefined.");

        String indexName;
        String fieldName = aField.getName();
        if (mSQLConnection.isAutoNamingEnabled())
        {
            if (StringUtils.startsWith(dbName, NS_INDEX_PREFIX))
            {
                if (StringUtils.contains(dbName, fieldName))
                    indexName = dbName;
                else
                    indexName = String.format("%s_%s_%s", NS_INDEX_PREFIX, dbName, fieldName);
            }
            else
                indexName = String.format("%s_%s_%s", NS_INDEX_PREFIX, dbName, fieldName);
        }
        else
            indexName = String.format("%s_%s", dbName, fieldName);

        return indexName;
    }

    /**
     * Creates an index object in the RDBMS based on the DB name
     * assigned to the bag and the persistent field.
     *
     * @param aBag Field bag with DB name assigned.
     * @param aField Field to base the index name on.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public abstract void create(DataBag aBag, DataField aField) throws NSException;

    /**
     * Drops an index object from the RDBMS based on the DB name
     * assigned to the bag and the persistent field.
     *
     * @param aBag Field bag with DB name assigned.
     * @param aField Field to base the index name on.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    public void drop(DataBag aBag, DataField aField)
        throws NSException
    {
        String indexName = schemaName(aBag, aField);
        SQLTable sqlTable = mSQLConnection.newTable();
        String tableName = sqlTable.schemaName(aBag);
        mSQLConnection.execute(String.format("DROP INDEX %s ON %s", indexName, tableName));
    }
}
