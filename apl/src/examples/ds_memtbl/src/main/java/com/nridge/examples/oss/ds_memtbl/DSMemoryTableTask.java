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

package com.nridge.examples.oss.ds_memtbl;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.app.mgr.Task;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.io.console.DataTableConsole;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.Sleep;
import com.nridge.core.ds.memory.MemoryTable;
import com.nridge.core.io.log.DSCriteriaLogger;
import org.slf4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The DSMemoryTableTask implements a collection of methods that the
 * Application Manager will invoke over the lifecycle of a Java
 * thread.
 */
@SuppressWarnings("FieldCanBeLocal")
class DSMemoryTableTask implements Task
{
    private final String mRunName = "memtbl";
    private final String mTestName = "memtbl";

    private final String SCHEMA_FILE_NAME = "ds_schema.xml";
    private final String USER_TABLE_FILE_NAME = "user_table.csv";

    private AppMgr mAppMgr;
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

    private MemoryTable createLoadDataSource()
    {
        Logger appLogger = mAppMgr.getLogger(this, "createLoadDataSource");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String schemaPathName = String.format("%s", mAppMgr.getString(mAppMgr.APP_PROPERTY_DS_PATH));
        String dataPathName = String.format("%s%cdata", mAppMgr.getString(mAppMgr.APP_PROPERTY_INS_PATH),
                                            File.separatorChar);

        MemoryTable memoryTable = new MemoryTable(mAppMgr, "users");
        try
        {
            memoryTable.loadSchema(schemaPathName);
            memoryTable.loadValues(dataPathName);
        }
        catch (IOException | DSException | ParserConfigurationException | SAXException e)
        {
            String msgStr = String.format("%s - %s: %s", schemaPathName, dataPathName, e.getMessage());
            appLogger.error(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return memoryTable;
    }

    private void toConsole(DataTable aTable, String aTitle)
    {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        DataTableConsole dataTableConsole = new DataTableConsole(aTable);
        dataTableConsole.write(printWriter, aTitle);
        printWriter.close();
        System.out.printf("%n%s%n", stringWriter.toString());
    }

    private void exerciseMemoryTable()
    {
        Logger appLogger = mAppMgr.getLogger(this, "exerciseMemoryTable");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        MemoryTable memoryTable = createLoadDataSource();

        DSCriteria dsCriteria = new DSCriteria("Fetch DSCriteria");
        dsCriteria.add("id", Field.Operator.GREATER_THAN, 10);
        dsCriteria.add("first_name", Field.Operator.STARTS_WITH, "Ch");
        DSCriteriaLogger dsCriteriaLogger = new DSCriteriaLogger(appLogger);
        dsCriteriaLogger.writeSimple(dsCriteria);

        try
        {
            DataTable resultTable = memoryTable.fetch(dsCriteria);
            toConsole(resultTable, "Memory Table Data Source Criteria Fetch");
            resultTable.sortByColumn("last_name", Field.Order.ASCENDING);
            toConsole(resultTable, "Memory Table Data Source Sort");
        }
        catch (DSException e)
        {
            appLogger.error(e.getMessage());
        }

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

        appLogger.info("Exercise Data Source Memory Table Features");
        exerciseMemoryTable();

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
