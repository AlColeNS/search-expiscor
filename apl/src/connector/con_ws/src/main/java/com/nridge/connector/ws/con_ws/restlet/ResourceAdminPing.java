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

package com.nridge.connector.ws.con_ws.restlet;

import com.nridge.core.app.mgr.AppMgr;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

/**
 * The AdminPing class handles GET method requests within the Restlet framework.
 *
 * @see <a href="http://restlet.org/">Restlet Framework</a>
 */
public class ResourceAdminPing extends ServerResource
{
    /**
     * Set-up method to initialize the state of the resource.
     *
     * @throws org.restlet.resource.ResourceException Encapsulates a response status and the
     * optional cause as a checked exception.
     */
    @Override
    protected void doInit()
        throws ResourceException
    {
        RestletApplication restletApplication = (RestletApplication) getApplication();
        AppMgr appMgr = restletApplication.getAppMgr();

        Logger appLogger = appMgr.getLogger(this, "doInit");

        appLogger.trace(appMgr.LOGMSG_TRACE_ENTER);

        appLogger.trace(appMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Invoked when an error or an exception is caught during initialization,
     * handling or releasing. By default, updates the response status with
     * the result.
     *
     * @param aThrowable The Throwable class is the superclass of all errors
     *                   and exceptions in the Java language.
     */
    @Override
    protected void doCatch(Throwable aThrowable)
    {
        RestletApplication restletApplication = (RestletApplication) getApplication();
        AppMgr appMgr = restletApplication.getAppMgr();

        Logger appLogger = appMgr.getLogger(this, "doCatch");

        appLogger.trace(appMgr.LOGMSG_TRACE_ENTER);

        appLogger.error(aThrowable.getMessage(), aThrowable);

        appLogger.trace(appMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Clean-up method that can be overridden in order to release the state of the resource.
     *
     * @throws org.restlet.resource.ResourceException Encapsulates a response status and the optional cause as
     * a checked exception.
     */
    @Override
    protected void doRelease()
        throws ResourceException
    {
        RestletApplication restletApplication = (RestletApplication) getApplication();
        AppMgr appMgr = restletApplication.getAppMgr();

        Logger appLogger = appMgr.getLogger(this, "doRelease");

        appLogger.trace(appMgr.LOGMSG_TRACE_ENTER);

        appLogger.trace(appMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Returns a full representation containing the Solr XML response message
     * to the Restlet framework.
     *
     * @return A Representation - the content of a representation can be retrieved
     * several times if there is a stable and accessible source, like a local file
     * or a string.
     *
     * @throws org.restlet.resource.ResourceException Encapsulates a response status and the optional
     * cause as a checked exception.
     */
    @Override
    protected Representation get()
        throws ResourceException
    {
        Representation replyRepresentation;
        RestletApplication restletApplication = (RestletApplication) getApplication();
        AppMgr appMgr = restletApplication.getAppMgr();

        Logger appLogger = appMgr.getLogger(this, "get");

        appLogger.trace(appMgr.LOGMSG_TRACE_ENTER);

        Date nowDate = new Date();
        String appName = appMgr.getString("app.name", "Data Source Application Service");
        String appVersion = appMgr.getString("app.version", "1.0");

        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        printWriter.printf("<!DOCTYPE html>%n");
        printWriter.printf("<html>%n");
        printWriter.printf(" <head>%n");
        printWriter.printf("  <title>%s - Release %s</title>%n", appName, appVersion);
        printWriter.printf("</head>%n");
        printWriter.printf(" <body>%n");
        printWriter.printf("  <p>The current date and time is %s</p>%n", nowDate);
        printWriter.printf(" </body>%n");
        printWriter.printf("</html>%n");
        printWriter.flush();
        printWriter.close();
        replyRepresentation = new StringRepresentation(stringWriter.toString(), MediaType.TEXT_HTML);

        appLogger.trace(appMgr.LOGMSG_TRACE_DEPART);

        return replyRepresentation;
    }
}
