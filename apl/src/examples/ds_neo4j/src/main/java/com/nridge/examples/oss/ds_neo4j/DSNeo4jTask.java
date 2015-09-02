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

package com.nridge.examples.oss.ds_neo4j;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.app.mgr.Task;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.io.console.DataTableConsole;
import com.nridge.core.base.io.xml.DataBagXML;
import com.nridge.core.base.io.xml.DocumentXML;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.Sleep;
import com.nridge.core.io.log.DSCriteriaLogger;
import com.nridge.core.io.log.DataTableLogger;
import com.nridge.ds.neo4j.ds_neo4j.Neo4j;
import com.nridge.ds.neo4j.ds_neo4j.Neo4jDS;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The DSNeo4jTask implements a collection of methods that the
 * Application Manager will invoke over the lifecycle of a Java
 * thread.
 */
@SuppressWarnings("FieldCanBeLocal")
class DSNeo4jTask implements Task
{
    private final String mRunName = "neo4j";
    private final String mTestName = "neo4j";

    private final String FIELD_ID_NAME = "ai_id";
    private final String SCHEMA_FILE_NAME = "ds_gdb_schema.xml";
    private final String[] PART_FILE_EXTENSIONS = {"xml"};

    private AppMgr mAppMgr;
    private AtomicBoolean mIsAlive;
    private ArrayList<String> mDocumentIds;

	/**
     * Returns the name of the run task.  This name will be used
     * by the application manager to identify which task in the
     * list to run (based on command line arguments).
     *
     * @return Name of the run task.
     */
    @Override
    public String getRunName()
    {
        return mRunName;
    }

	/**
     * Returns the name of the test task.  This name will be used
     * by the application manager to identify which task in the
     * list to test (based on command line arguments).
     *
     * @return Name of the test task.
     */
    @Override
    public String getTestName()
    {
        return mTestName;
    }

	/**
     * Returns <i>true</i> if this task was properly initialized
     * and is currently executing.
     *
     * @return <i>true</i> or <i>false</i>
     */
    @Override
    public boolean isAlive()
    {
        if (mIsAlive == null)
            mIsAlive = new AtomicBoolean(false);

        return mIsAlive.get();
    }

	/**
     * If this task is scheduled to be executed (e.g. its run/test
     * name matches the command line arguments), then this method
     * is guaranteed to be executed prior to the thread being
     * started.
     *
     * @param anAppMgr Application manager instance.
     *
     * @throws NSException Application specific exception.
     */
    @Override
    public void init(AppMgr anAppMgr)
        throws NSException
    {
        mAppMgr = anAppMgr;
        Logger appLogger = mAppMgr.getLogger(this, "init");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		mIsAlive = new AtomicBoolean(false);

        mDocumentIds = new ArrayList<>();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        mIsAlive.set(true);
    }

	/**
     * Each task supports a method dedicated to testing or exercising
     * a subset of application features without having to run the
     * mainline thread of task logic.
     *
     * @throws NSException Application specific exception.
     */
    @Override
    public void test()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "test");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (! isAlive())
        {
            appLogger.error("Initialization failed - must abort test method.");
            return;
        }

        appLogger.info("The test method was invoked.");
        Sleep.forSeconds(1);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void toConsole(DataTable aTable)
    {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        DataTableConsole dataTableConsole = new DataTableConsole(aTable);
        dataTableConsole.write(printWriter, "Data Table Console Output");
        printWriter.close();
        System.out.printf("%n%s%n", stringWriter.toString());
        System.out.printf("Total rows is %d.%n", aTable.rowCount());
    }

    private Neo4jDS createGDBDataSource()
    {
        Neo4jDS neo4jDS;
        Logger appLogger = mAppMgr.getLogger(this, "createGDBDataSource");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String schemaPathFileName = String.format("%s%c%s", mAppMgr.getString(mAppMgr.APP_PROPERTY_DS_PATH),
                                                  File.separatorChar, SCHEMA_FILE_NAME);
        DataBagXML dataBagXML = new DataBagXML();
        try
        {
            dataBagXML.load(schemaPathFileName);
            DataBag schemaBag = dataBagXML.getBag();
            neo4jDS = new Neo4jDS(mAppMgr);
            neo4jDS.setSchema(schemaBag);
            neo4jDS.resetDatabase();
        }
        catch (Exception e)
        {
            neo4jDS = null;
            e.printStackTrace();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return neo4jDS;
    }

    private void addDocumentId(Document aDocument)
    {
        DataTable dataTable = aDocument.getTable();
        DataBag dataBag = dataTable.getColumnBag();
        String docId = dataBag.getValueAsString(FIELD_ID_NAME);
        if (StringUtils.isNotEmpty(docId))
            mDocumentIds.add(docId);
    }

    private void addDocuments(Neo4jDS aNeo4jDS)
    {
        String pathFileName;
        Document partDocument;
        DocumentXML documentXML;
        Logger appLogger = mAppMgr.getLogger(this, "addDocuments");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String partPathName = String.format("%s%cdata", mAppMgr.getString(mAppMgr.APP_PROPERTY_INS_PATH),
                                            File.separatorChar);
        File partPathFile = new File(partPathName);
        Collection<File> partFiles = FileUtils.listFiles(partPathFile, PART_FILE_EXTENSIONS, true);
        for (File partFile : partFiles)
        {
            pathFileName = partFile.getAbsolutePath();
            documentXML = new DocumentXML();
            try
            {
                documentXML.load(pathFileName);
                partDocument = documentXML.getDocument();
                addDocumentId(partDocument);
                System.out.printf("%s: Adding to GDB repository.%n", partDocument.getName());
                aNeo4jDS.add(partDocument);
            }
            catch (Exception e)
            {
                String msgStr = String.format("%s: %s", pathFileName, e.getMessage());
                appLogger.error(msgStr, e);
                System.err.printf("%n%s%n", msgStr);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void queryDocuments(Neo4jDS aNeo4jDS)
    {
        Document gdbDocument;
        DSCriteria dsCriteria;
        Logger appLogger = mAppMgr.getLogger(this, "queryDocuments");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        for (String docId : mDocumentIds)
        {
            try
            {
                dsCriteria = new DSCriteria(String.format("DS Criteria for GDB '%s'", docId));
                dsCriteria.add(Neo4j.FIELD_NODE_ID, Field.Operator.EQUAL, docId);
                dsCriteria.add(Neo4j.FIELD_REL_NAME, Field.Operator.IN, "Part BOM", "Part Document", "Part CAD");
                dsCriteria.add(Neo4j.FIELD_RESOLVE_TO, Field.Operator.EQUAL, Neo4j.RESOLVE_TO_NODE_LIST);

                DSCriteriaLogger dsCriteriaLogger = new DSCriteriaLogger(appLogger);
                dsCriteriaLogger.writeFull(dsCriteria);

                gdbDocument = aNeo4jDS.fetch(dsCriteria);

                Relationship gdbRelationship = gdbDocument.getFirstRelationship(Neo4j.RESPONSE_DOCUMENT);
                Document responseDocument = gdbRelationship.getFirstDocument();
                if (responseDocument != null)
                {
                    DataTable resultTable = responseDocument.getTable();
                    toConsole(resultTable);
                    DataTableLogger dataTableLogger = new DataTableLogger(appLogger);
                    dataTableLogger.writeSimple(resultTable);
                }
            }
            catch (Exception e)
            {
                String msgStr = String.format("%s: %s", docId, e.getMessage());
                appLogger.error(msgStr, e);
                System.err.printf("%n%s%n", msgStr);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Exercise some of the core graph database features in the Expiscor
     * class library.
     */
    private void exerciseGDB()
    {
        Neo4jDS neo4jDS = createGDBDataSource();
        if (neo4jDS != null)
        {
            addDocuments(neo4jDS);
            queryDocuments(neo4jDS);
            neo4jDS.shutdown();
            Sleep.forSeconds(1);
        }
    }

	/**
     * The {@link Runnable}.run() will be executed after the task
     * has been successfully initialized.  This method is where
     * the application specific logic should be concentrated for
     * the task.
     */
    @Override
    public void run()
    {
        Logger appLogger = mAppMgr.getLogger(this, "run");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (! isAlive())
        {
            appLogger.error("Initialization failed - must abort run method.");
            return;
        }

        appLogger.info("Exercise Graph Database Features");
        exerciseGDB();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

	/**
     * Once the task has completed its run, then this method
     * will be invoked by the Application Manager to enable
     * the task to release any resources it was holding.
     * <p>
     * <b>Note:</b>If the JVM detects and external shutdown
     * event (e.g. service is being stopped), then the
     * Application Manager will asynchronously invoke this
     * in hopes that the task can save its state prior to
     * the process exiting.
	 * </p>
     */
    @Override
    public void shutdown()
    {
        Logger appLogger = mAppMgr.getLogger(this, "shutdown");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (isAlive())
        {
            appLogger.info("The shutdown method was invoked.");
            Sleep.forSeconds(1);
            mIsAlive.set(false);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
