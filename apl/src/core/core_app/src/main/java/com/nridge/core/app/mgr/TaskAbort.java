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

package com.nridge.core.app.mgr;

import com.nridge.core.base.std.Sleep;
import org.slf4j.Logger;

import java.util.ArrayList;

/**
 * The TaskAbort class is a handler for the <code>Runtime.addShutdownHook()</code>
 * method.  It primary responsibility is to notify all executing tasks that
 * a JVM shutdown is imminent.
 * <p>
 * <b>Note:</b>&nbsp;This is a specialized class for the AppMgr and should be
 * avoided for general applications.
 * </p>
 */
public class TaskAbort  extends Thread
{
    private AppMgr mAppMgr;
    private ArrayList<Task> mTaskList;

    public TaskAbort(AppMgr anAppMgr, ArrayList<Task> aTaskList)
    {
        mAppMgr = anAppMgr;
        mTaskList = aTaskList;
    }

    @Override
    public void run()
    {
        Logger appLogger = mAppMgr.getLogger(this, "init");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mAppMgr.setIsAliveFlag(false);
        appLogger.warn("Abort request received - shutting down tasks.");

        for (Task appTask : mTaskList)
        {
            if (appTask.isAlive())
                appTask.shutdown();
        }

// Allow a little time for the resources to finish up.

        Sleep.forSeconds(1);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
