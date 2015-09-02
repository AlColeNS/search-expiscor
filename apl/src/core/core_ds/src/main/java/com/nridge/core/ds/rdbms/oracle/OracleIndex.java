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

package com.nridge.core.ds.rdbms.oracle;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.NSException;
import com.nridge.core.ds.rdbms.SQLConnection;
import com.nridge.core.ds.rdbms.SQLIndex;
import com.nridge.core.ds.rdbms.SQLTable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

/**
 * Implements the Oracle RDBMS interfaces for index
 * operations.  Since these methods are specific to a
 * particular RDBMS vendor, an application developer is
 * encouraged to use the abstracted <i>SQLTable</i> and
 * <i>SQLIndex</i> classes instead.
 *
 * @author Al Cole
 * @since 1.0
 */
public class OracleIndex extends SQLIndex
{
    private AppMgr mAppMgr;

    /**
     * Constructor that accepts a SQL connection.
     *
     * @param aConnection SQL connection.
     */
    public OracleIndex(SQLConnection aConnection)
    {
        super(aConnection);
        mAppMgr = aConnection.getAppMgr();
    }

    /**
     * Creates an index object in the RDBMS based on the DB name
     * assigned to the bag and the persistent field.
     *
     * @param aBag   Field bag with DB name assigned.
     * @param aField Field to base the index name on.
     *
     * @throws com.nridge.core.base.std.NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public void create(DataBag aBag, DataField aField)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "create");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aField.isFeatureAssigned(Field.FEATURE_INDEX_POLICY))
        {
            String sqlStatement;
            SQLTable sqlTable = mSQLConnection.newTable();
            String indexName = schemaName(aBag, aField);
            String tableSpace = aBag.getFeature(Field.FEATURE_TABLESPACE_NAME);
            if (aField.isFeatureEqual(Field.FEATURE_INDEX_POLICY, Field.SQL_INDEX_POLICY_UNIQUE))
                sqlStatement = String.format("CREATE UNIQUE INDEX %s ON %s(%s)",
                                             indexName, sqlTable.schemaName(aBag),
                                             sqlTable.columnName(aField.getName()));
            else
                sqlStatement = String.format("CREATE INDEX %s ON %s(%s)",
                                             indexName, sqlTable.schemaName(aBag),
                                             sqlTable.columnName(aField.getName()));
            if (StringUtils.isNotEmpty(tableSpace))
                sqlStatement += String.format(" TABLESPACE %s", tableSpace);

            mSQLConnection.execute(sqlStatement);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Drops an index object from the RDBMS based on the DB name
     * assigned to the bag and the persistent field.
     *
     * @param aBag Field bag with DB name assigned.
     * @param aField Field to base the index name on.
     *
     * @throws NSException Catch-all exception for any SQL related issue.
     */
    @Override
    public void drop(DataBag aBag, DataField aField)
        throws NSException
    {
        String indexName = schemaName(aBag, aField);
        mSQLConnection.execute(String.format("DROP INDEX %s", indexName));
    }
}
