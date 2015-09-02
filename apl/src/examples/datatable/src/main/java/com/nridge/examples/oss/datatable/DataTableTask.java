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

package com.nridge.examples.oss.datatable;

import com.google.gson.stream.JsonWriter;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.app.mgr.Task;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.FieldRow;
import com.nridge.core.base.field.data.*;
import com.nridge.core.base.io.console.DataTableConsole;
import com.nridge.core.base.io.xml.DataTableXML;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.NumUtl;
import com.nridge.core.base.std.Sleep;
import com.nridge.core.io.gson.DataTableJSON;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The DataTableTask implements a collection of methods that the
 * Application Manager will invoke over the lifecycle of a Java
 * thread.
 */
@SuppressWarnings("FieldCanBeLocal")
class DataTableTask implements Task
{
    private final String mRunName = "datatable";
    private final String mTestName = "datatable";

    private final int TOTAL_ROW_COUNT = 10;

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

        appLogger.info("The init method was invoked.");
        Sleep.forSeconds(1);

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

    private DataTable createPopulateTable()
    {
        int rowValue;
        String textValue;

// Create our field definitions for the data table.

        DataTable dataTable = new DataTable("Data Table");
        dataTable.add(new DataLongField("long_field", "Long Field"));
        dataTable.add(new DataTextField("text_field", "Text Field"));
        dataTable.add(new DataFloatField("float_field", "Float Field"));
        dataTable.add(new DataDoubleField("double_field", "Double Field"));
        dataTable.add(new DataIntegerField("integer_field", "Integer Field"));
        dataTable.add(new DataBooleanField("boolean_field", "Boolean Field"));
        dataTable.add(new DataDateTimeField("datetime_field", "Date/Time Field"));

        Date nowDate = new Date();
        for (int row = 0; row < TOTAL_ROW_COUNT; row++)
        {
            dataTable.newRow();

            rowValue = row + 1;
            dataTable.setValueByName("integer_field", rowValue);
            dataTable.setValueByName("long_field", (long) rowValue);
            dataTable.setValueByName("float_field", (float) rowValue);
            dataTable.setValueByName("double_field", (double) rowValue);
            dataTable.setValueByName("boolean_field", NumUtl.isOdd(rowValue));
            dataTable.setValueByName("datetime_field", DateUtils.addMinutes(nowDate, rowValue));
            dataTable.setValueByName("datetime_field", DateUtils.addMinutes(nowDate, rowValue),
                                     Field.FORMAT_DATETIME_DEFAULT);
            textValue = String.format("This is a test sentence %d of %d rows.", rowValue, TOTAL_ROW_COUNT);
            dataTable.setValueByName("text_field", textValue);

            dataTable.addRow();
        }

        return dataTable;
    }

    private void toXML(DataTable aTable)
    {
        StringWriter stringWriter = new StringWriter();
        try (PrintWriter printWriter = new PrintWriter(stringWriter))
        {
            DataTableXML dataTableXML = new DataTableXML(aTable);
            dataTableXML.save(printWriter);
            System.out.printf("%nData Table XML Output%n%s%n", stringWriter.toString());
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private void toJSON(DataTable aTable)
    {
        StringWriter stringWriter = new StringWriter();
        try (JsonWriter jsonWriter = new JsonWriter(stringWriter))
        {
            jsonWriter.setIndent(" ");
            DataTableJSON dataTableJSON = new DataTableJSON(aTable);
            dataTableJSON.save(jsonWriter);
            System.out.printf("%nData Table JSON Output%n%s%n", stringWriter.toString());
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private void toConsole(DataTable aTable)
    {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        DataTableConsole dataTableConsole = new DataTableConsole(aTable);
        dataTableConsole.write(printWriter, "Data Table Console Output");
        printWriter.close();
        System.out.printf("%n%s%n", stringWriter.toString());
    }

    /**
     * Exercise some of the core data table features in the Expiscor
     * class library.
     */
    private void exerciseTable()
    {
        DataTable dataTable = createPopulateTable();

        toConsole(dataTable);
        toXML(dataTable);
        toJSON(dataTable);

        FieldRow fieldRow = Field.firstFieldRow(dataTable.findValue("text_field", Field.Operator.CONTAINS, "sentence 6"));
        if (fieldRow == null)
            System.err.printf("%nCould not find text in row 6%n");
        else
            System.out.printf("%nFound Row: %s%n%n", dataTable.getValueByName(fieldRow, "text_field"));
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

        appLogger.info("Exercise Data Table Features");
        exerciseTable();

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
