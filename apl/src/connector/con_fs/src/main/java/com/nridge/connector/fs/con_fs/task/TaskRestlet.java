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
import com.nridge.connector.fs.con_fs.restlet.RestletApplication;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.app.mgr.Task;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.Sleep;
import org.restlet.Server;
import org.restlet.data.Protocol;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The TaskRestlet implements a collection of methods that the
 * Application Manager will invoke over the lifecycle of a Java
 * thread.
 */
@SuppressWarnings("FieldCanBeLocal")
public class TaskRestlet implements Task
{
    private final String mRunName = "restlet";
    private final String mTestName = "restlet";

    private AppMgr mAppMgr;
    private Server mServer;
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

        int portNumber = mAppMgr.getInt(Constants.CFG_PROPERTY_PREFIX + ".restlet.port_number",
                                        Constants.APPLICATION_PORT_NUMBER_DEFAULT);
        RestletApplication restletApplication = new RestletApplication(mAppMgr);

        appLogger.info("Starting Restlet Server.");
        mServer = new Server(Protocol.HTTP, portNumber, restletApplication);
        try
        {
            mServer.start();
        }
        catch (Exception e)
        {
            appLogger.error("Restlet Server (start): " + e.getMessage(), e);
            throw new NSException(e.getMessage());
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

        appLogger.info("Main Restlet thread is running.");

// Keep this main thread alive until we are explicitly shutdown.

        while (isAlive())
            Sleep.forSeconds(5);


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
            appLogger.info("Stopping Restlet Server.");
            try
            {
                mServer.stop();
            }
            catch (Exception e)
            {
                appLogger.error("Restlet Server (stop): " + e.getMessage(), e);
            }
            mIsAlive.set(false);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
