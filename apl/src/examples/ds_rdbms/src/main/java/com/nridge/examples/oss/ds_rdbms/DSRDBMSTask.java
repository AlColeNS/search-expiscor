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

package com.nridge.examples.oss.ds_rdbms;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.app.mgr.Task;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.*;
import com.nridge.core.base.io.console.DataTableConsole;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.NumUtl;
import com.nridge.core.base.std.Sleep;
import com.nridge.core.ds.rdbms.SQL;
import com.nridge.core.ds.rdbms.SQLConnection;
import com.nridge.core.ds.rdbms.SQLTable;
import com.nridge.core.io.log.DSCriteriaLogger;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The DSRDBMSTask implements a collection of methods that the
 * Application Manager will invoke over the lifecycle of a Java
 * thread.
 */
@SuppressWarnings("FieldCanBeLocal")
class DSRDBMSTask implements Task
{
    private final String mRunName = "rdbms";
    private final String mTestName = "rdbms";

    private final int TOTAL_ROW_COUNT = 1000;
    private final String RDBMS_TABLE_NAME = "test_data";

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

    private DataTable createDataTable()
    {
        DataTable dataTable = new DataTable("Data Table");
        DataField dataField = new DataIntegerField(SQL.COLUMN_ID_FIELD_NAME, "Id");
        dataField.enableFeature(Field.FEATURE_IS_PRIMARY_KEY);
        dataField.enableFeature(Field.FEATURE_IS_REQUIRED);
        dataField.addFeature(Field.FEATURE_SEQUENCE_MANAGEMENT, Field.SQL_INDEX_MANAGEMENT_IMPLICIT);
        dataField.addFeature(Field.FEATURE_SEQUENCE_SEED, 1000);
        dataTable.add(dataField);
        dataTable.add(new DataLongField("long_field", "Long Field"));
        dataTable.add(new DataFloatField("float_field", "Float Field"));
        dataTable.add(new DataDoubleField("double_field", "Double Field"));
        dataTable.add(new DataIntegerField("integer_field", "Integer Field"));
        dataTable.add(new DataBooleanField("boolean_field", "Boolean Field"));
        dataTable.add(new DataDateTimeField("datetime_field", "Date/Time Field"));
        dataField = new DataTextField("text_field", "Text Field");
        dataField.enableFeature(Field.FEATURE_IS_REQUIRED);
        dataField.addFeature(Field.FEATURE_STORED_SIZE, 1024);
        dataField.addFeature(Field.FEATURE_INDEX_POLICY, Field.SQL_INDEX_POLICY_UNIQUE);
        dataTable.add(dataField);

        return dataTable;
    }

    private SQLConnection createPopulateRDBMS()
    {
        int rowValue;
        String textValue;
        SQLConnection sqlConnection;

        Logger appLogger = mAppMgr.getLogger(this, "createPopulateRDBMS");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

// Create our field definitions for the data table.

        DataTable dataTable = createDataTable();

// Create a SQL connection, table object and populate it in the RDBMS.

        try
        {
            sqlConnection = new SQLConnection(mAppMgr, SQL.PROPERTY_PREFIX_DEFAULT);
            SQLTable sqlTable = sqlConnection.newTable(Field.SQL_TABLE_TYPE_MEMORY);
            DataBag dataBag = new DataBag(dataTable.getColumnBag());
            dataBag.setName(RDBMS_TABLE_NAME);
            sqlTable.create(dataBag);

            Date nowDate = new Date();
            for (int row = 0; row < TOTAL_ROW_COUNT; row++)
            {
                dataBag.resetValues();

                rowValue = row + 1;
                dataBag.setValueByName("integer_field", rowValue);
                dataBag.setValueByName("long_field", (long) rowValue);
                dataBag.setValueByName("float_field", (float) rowValue);
                dataBag.setValueByName("double_field", (double) rowValue);
                dataBag.setValueByName("boolean_field", NumUtl.isOdd(rowValue));
                dataBag.setValueByName("datetime_field", DateUtils.addMinutes(nowDate, rowValue));
                dataBag.setValueByName("datetime_field", DateUtils.addMinutes(nowDate, rowValue));
                textValue = String.format("This is a test sentence %d of %d rows.", rowValue, TOTAL_ROW_COUNT);
                dataBag.setValueByName("text_field", textValue);

                sqlTable.insert(dataBag);
                if ((row > 0) && ((row % 50) == 0))
                    sqlTable.commit();
            }

            sqlTable.commit();
        }
        catch (NSException e)
        {
            e.printStackTrace();
            sqlConnection = null;
        }

        return sqlConnection;
    }

    private void toConsole(DataTable aTable)
    {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        DataTableConsole dataTableConsole = new DataTableConsole(aTable);
        dataTableConsole.write(printWriter, "RDBMS Table Console Output");
        printWriter.close();
        System.out.printf("%n%s%n", stringWriter.toString());
    }

    public void exerciseRDBMS()
    {
        Logger appLogger = mAppMgr.getLogger(this, "exerciseRDBMS");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        SQLConnection sqlConnection = createPopulateRDBMS();
        if (sqlConnection != null)
        {
            DataTable dataTable = createDataTable();
            DataBag dataBag = new DataBag(dataTable.getColumnBag());
            dataBag.setName(RDBMS_TABLE_NAME);

            DSCriteria dsCriteria = new DSCriteria("Fetch DSCriteria");
            dsCriteria.add("integer_field", Field.Operator.GREATER_THAN, 990);
            dsCriteria.add("boolean_field", Field.Operator.EQUAL, true);
            DSCriteriaLogger dsCriteriaLogger = new DSCriteriaLogger(appLogger);
            dsCriteriaLogger.writeSimple(dsCriteria);

            try
            {
                SQLTable sqlTable = sqlConnection.newTable(Field.SQL_TABLE_TYPE_MEMORY);
                dataTable = sqlTable.select(dataBag, dsCriteria);
                toConsole(dataTable);
            }
            catch (NSException e)
            {
                e.printStackTrace();
            }
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

        appLogger.info("Exercise RDBMS Features");
        exerciseRDBMS();

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
