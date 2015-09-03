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

package com.nridge.connector.fs.con_fs.task;

import com.nridge.connector.fs.con_fs.core.Constants;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.app.mgr.Task;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.io.xml.DataBagXML;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.Sleep;
import com.nridge.ds.solr.SolrSchemaXML;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The TaskSolrSchema reads the NSD schema file and generates
 * a corresponding Solr schema file containing field fragments.
 */
@SuppressWarnings("FieldCanBeLocal")
public class TaskSolrSchema implements Task
{
    private final String mRunName = "solr_schema";
    private final String mTestName = "solr_schema";

    private AppMgr mAppMgr;
    private DataBag mSchemaBag;
    private AtomicBoolean mIsAlive;

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
     * @throws com.nridge.core.base.std.NSException Application specific exception.
     */
    @Override
    public void init(AppMgr anAppMgr)
        throws NSException
    {
        mAppMgr = anAppMgr;
        Logger appLogger = mAppMgr.getLogger(this, "init");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mIsAlive = new AtomicBoolean(false);

        // Load our schema definition from the data source folder.

        String schemaPathFileName = String.format("%s%c%s", mAppMgr.getString(mAppMgr.APP_PROPERTY_DS_PATH),
                                                  File.separatorChar, Constants.SCHEMA_FILE_NAME);
        DataBagXML dataBagXML = new DataBagXML();
        try
        {
            dataBagXML.load(schemaPathFileName);
            mSchemaBag = dataBagXML.getBag();
        }
        catch (Exception e)
        {
            mSchemaBag = null;
            String msgStr = String.format("%s: %s", schemaPathFileName, e.getMessage());
            appLogger.error(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        mIsAlive.set(true);
    }

    /**
     * Each task supports a method dedicated to testing or exercising
     * a subset of application features without having to run the
     * mainline thread of task logic.
     *
     * @throws com.nridge.core.base.std.NSException Application specific exception.
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

        if (mSchemaBag == null)
            System.err.printf("%nUnable to load the data source schema from file system.%n%n");
        else
        {
            appLogger.info("Generating the Solr Schema file to the log folder.");

            String solrSchemaPathFileName = String.format("%s%csolr_schema.xml",
                                                          mAppMgr.getString(mAppMgr.APP_PROPERTY_LOG_PATH),
                                                          File.separatorChar);
            try
            {
                Document schemaDocument = new Document("NSD Schema", mSchemaBag);
                SolrSchemaXML solrSchemaXML = new SolrSchemaXML(mAppMgr, schemaDocument);
                solrSchemaXML.save(solrSchemaPathFileName);
            }
            catch (IOException e)
            {
                String msgStr = String.format("%s: %s", solrSchemaPathFileName, e.getMessage());
                appLogger.error(msgStr, e);
            }
        }

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
            mIsAlive.set(false);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
