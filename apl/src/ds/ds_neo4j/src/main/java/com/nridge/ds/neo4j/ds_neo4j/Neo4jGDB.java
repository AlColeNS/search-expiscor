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

package com.nridge.ds.neo4j.ds_neo4j;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * The Neo4jGDB class provides singleton access to the Neo4j Graph
 * Database.  Neo4j supports multi-threaded transactions, but each
 * thread must reference the same service instance of the database.
 *
 * @see <a href="http://neo4j.com/">Neo4j Graph Database</a>
 * @see <a href="http://www.journaldev.com/1377/java-singleton-design-pattern-best-practices-with-examples">Java Singleton</a>
 *
 */
public class Neo4jGDB
{
    private static GraphDatabaseService mGraphDBService = null;

    private Neo4jGDB()
    {
    }

// https://groups.google.com/forum/#!topic/nosql-databases/z9nZ80ow5QY
// http://neo4j.com/docs/milestone/tutorials-java-embedded-setup.html
// http://neo4j.com/docs/stable/tutorials-java-embedded-new-index.html
// http://stackoverflow.com/questions/22049121/does-label-mechanism-provide-auto-indexing-features-when-using-neo4j-java-api

    public static GraphDatabaseService getInstance(final AppMgr anAppMgr, DataBag aSchemaBag)
        throws DSException
    {
        if (mGraphDBService == null)
        {
            Label graphDBLabel = null;
            if (aSchemaBag != null)
            {
                String labelName = aSchemaBag.getFeature("labelName");
                if (StringUtils.isNotEmpty(labelName))
                    graphDBLabel = DynamicLabel.label(labelName);
            }
            String graphDBPathName = anAppMgr.getString(anAppMgr.APP_PROPERTY_GDB_PATH);
            String graphDBSchemaPathFileName = String.format("%s%cschema", graphDBPathName, File.separatorChar);
            File graphDBSchemaFile = new File(graphDBSchemaPathFileName);
            boolean gdbSchemaExists = graphDBSchemaFile.exists();

// Ensure we have a graph database folder for data storage.

            File graphDBPathFile = new File(graphDBPathName);
            if (! graphDBPathFile.exists())
                graphDBPathFile.mkdir();
            if (! graphDBPathFile.exists())
            {
                String msgStr = String.format("%s: Does not exist.", graphDBPathName);
                throw new DSException(msgStr);
            }

// Prevent a large number of log messages from being generated.

            Logging gdbLogging = new DevNullLoggingService();

// Enter the critical section to create the service handle.

            synchronized (Neo4jGDB.class)
            {
                if (mGraphDBService == null)
                {
                    mGraphDBService = new GraphDatabaseFactory().setLogging(gdbLogging).newEmbeddedDatabaseBuilder(graphDBPathName).newGraphDatabase();
                    if ((aSchemaBag != null) && (! gdbSchemaExists))
                    {
                        DataField pkField = aSchemaBag.getPrimaryKeyField();
                        if (pkField != null)
                        {
                            Schema gdbSchema;
                            IndexDefinition indexDefinition = null;

                            try (Transaction gdbTransaction = mGraphDBService.beginTx())
                            {
                                gdbSchema = mGraphDBService.schema();
                                for (DataField dataField : aSchemaBag.getFields())
                                {
                                    if (dataField != pkField)
                                    {
                                        if (dataField.isFeatureTrue("isIndexed"))
                                            gdbSchema.indexFor(graphDBLabel).on(dataField.getName()).create();
                                    }
                                }
                                indexDefinition = gdbSchema.indexFor(graphDBLabel).on(pkField.getName()).create();
                                gdbTransaction.success();
                            }
                            if (indexDefinition != null)
                            {
                                try ( Transaction gdbTransaction = mGraphDBService.beginTx() )
                                {
                                    gdbSchema.awaitIndexOnline(indexDefinition, 10, TimeUnit.SECONDS);
                                    gdbTransaction.success();
                                }
                            }
                        }
                    }
                }
            }
        }

        return mGraphDBService;
    }

    public static void shutdown()
    {
        if (mGraphDBService != null)
        {
            synchronized (Neo4jGDB.class)
            {
                if (mGraphDBService != null)
                {
                    mGraphDBService.shutdown();
                    mGraphDBService = null;
                }
            }
        }
    }
}
