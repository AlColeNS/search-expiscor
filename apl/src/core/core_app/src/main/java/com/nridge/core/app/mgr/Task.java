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

import com.nridge.core.base.std.NSException;

/**
 * A Task is an interface that defines a collection of methods
 * that the Application Manager will expect an application to
 * implement in order to successfully run within its context.
 *
 * @since 1.0
 * @author Al Cole
 */
public interface Task extends Runnable
{
    /**
     * Returns the name of the run task.  This name will be used
     * by the application manager to identify which task in the
     * list to run (based on command line arguments).
     *
     * @return Name of the run task.
     */
    public String getRunName();

    /**
     * Returns the name of the test task.  This name will be used
     * by the application manager to identify which task in the
     * list to test (based on command line arguments).
     *
     * @return Name of the test task.
     */
    public String getTestName();

    /**
     * Returns <i>true</i> if this task was properly initialized
     * and is currently executing.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isAlive();

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
    public void init(AppMgr anAppMgr) throws NSException;

    /**
     * Each task supports a method dedicated to testing or exercising
     * a subset of application features without having to run the
     * mainline thread of task logic.
     *
     * @throws NSException Application specific exception.
     */
    public void test() throws NSException;

    /**
     * The <code>Runnable.run()</code> will be executed after the
     * task has been successfully initialized.  This method is where
     * the application specific logic should be concentrated for
     * the task.
     */
//  public void run();

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
    public void shutdown();
}
