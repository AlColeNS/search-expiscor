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

package com.nridge.connector.fs.con_fs.restlet;

import com.nridge.connector.fs.con_fs.core.Constants;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.ds.solr.Solr;
import com.nridge.ds.solr.SolrDS;
import org.apache.commons.lang3.StringUtils;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Reference;
import org.restlet.representation.FileRepresentation;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

/**
 * The ResourceDocView class handles document view method requests within
 * the Restlet framework.
 *
 * @see <a href="http://restlet.org/">Restlet Framework</a>
 */
public class ResourceDocView extends ServerResource
{
    private final String PROPERTY_ERROR_MESSAGE = "errorMessage";
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

    private DataBag loadDocument(String anId)
    {
        RestletApplication restletApplication = (RestletApplication) getApplication();
        AppMgr appMgr = restletApplication.getAppMgr();
        Logger appLogger = appMgr.getLogger(this, "loadDocument");

        DataBag docBag = new DataBag(anId, "Solr Document Load");

        appLogger.trace(appMgr.LOGMSG_TRACE_ENTER);

        String propertyName = Constants.CFG_PROPERTY_PREFIX + ".solr.request_uri";
        String solrURI = appMgr.getString(propertyName);
        if (StringUtils.isNotEmpty(solrURI))
        {
            propertyName = Constants.CFG_PROPERTY_PREFIX + ".solr.request_handler";
            String propertyValue = appMgr.getString(propertyName, Constants.SOLR_REQUEST_HANDLER_DEFAULT);
            String requestHandler = StringUtils.removeStart(propertyValue, "/");
            String solrURL = String.format("%s/%s?q=nsd_id%%3A%s&wt=xml&echoParams=none",
                                            solrURI, requestHandler, anId);
            SolrDS solrDS = new SolrDS(appMgr);
            solrDS.setCfgPropertyPrefix(Constants.CFG_PROPERTY_PREFIX + ".solr");
            DSCriteria dsCriteria = new DSCriteria("Solr Document Exists");
            dsCriteria.add(Solr.FIELD_URL_NAME, Field.Operator.EQUAL, solrURL);
            try
            {
                Document solrDocument = solrDS.fetch(dsCriteria, 0, 1);
                Document responseDocument = solrDocument.getFirstRelatedDocument(Solr.RESPONSE_DOCUMENT);
                DataTable resultTable = responseDocument.getTable();
                int rowCount = resultTable.rowCount();
                appLogger.debug(String.format("[%d] %s", rowCount, solrURL));
                if (rowCount > 0)
                    docBag = resultTable.getRowAsBag(0);
                else
                {
                    String msgStr = String.format("Cannot view document - document '%s' does not exist in Solr.", anId);
                    docBag.addProperty(PROPERTY_ERROR_MESSAGE, msgStr);
                    appLogger.error(msgStr);
                }
            }
            catch (DSException e)
            {
                appLogger.error(String.format("%s: %s", solrURL, e.getMessage()));
            }
            finally
            {
                solrDS.shutdown();
            }
        }
        else
        {
            String msgStr = String.format("Cannot view document - '%s' is undefined.", propertyName);
            docBag.addProperty(PROPERTY_ERROR_MESSAGE, msgStr);
            appLogger.error(msgStr);
        }

        appLogger.trace(appMgr.LOGMSG_TRACE_DEPART);

        return docBag;
    }

    private Representation messageRepresentation(String aMessage)
    {
        RestletApplication restletApplication = (RestletApplication) getApplication();
        AppMgr appMgr = restletApplication.getAppMgr();

        Logger appLogger = appMgr.getLogger(this, "messageRepresentation");

        appLogger.trace(appMgr.LOGMSG_TRACE_ENTER);

        String appName = appMgr.getString("app.name", "NSD FS Service");
        String appVersion = appMgr.getString("app.version", "1.0");

        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        printWriter.printf("<!DOCTYPE html>%n");
        printWriter.printf("<html>%n");
        printWriter.printf(" <head>%n");
        printWriter.printf("  <title>%s - Release %s</title>%n", appName, appVersion);
        printWriter.printf("</head>%n");
        printWriter.printf(" <body>%n");
        printWriter.printf("  <p>%s</p>%n", aMessage);
        printWriter.printf(" </body>%n");
        printWriter.printf("</html>%n");
        printWriter.flush();
        printWriter.close();

        appLogger.trace(appMgr.LOGMSG_TRACE_DEPART);

        return new StringRepresentation(stringWriter.toString(), MediaType.TEXT_HTML);
    }

    /**
     * Returns a full representation containing the document response message
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
        String msgStr;
        Representation replyRepresentation;
        RestletApplication restletApplication = (RestletApplication) getApplication();
        AppMgr appMgr = restletApplication.getAppMgr();

        Logger appLogger = appMgr.getLogger(this, "get");

        appLogger.trace(appMgr.LOGMSG_TRACE_ENTER);

        Reference originalReference = getOriginalRef();
        Form queryForm = originalReference.getQueryAsForm();
        String docId = queryForm.getFirstValue("id");

        if (StringUtils.isNotEmpty(docId))
        {
            DataBag docBag = loadDocument(docId);
            if (docBag.count() == 0)
            {
                msgStr = (String) docBag.getProperty(PROPERTY_ERROR_MESSAGE);
                replyRepresentation = messageRepresentation(msgStr);
            }
            else
            {
                String nsdURL = docBag.getValueAsString("nsd_url");
                if (StringUtils.startsWith(nsdURL, "file:"))
                {
                    String mimeType = docBag.getValueAsString("nsd_mime_type");
                    if (StringUtils.isEmpty(mimeType))
                        mimeType = "application/octet-stream";
                    try
                    {
                        URI uriFile = new URI(nsdURL);
                        File viewFile = new File(uriFile);
                        replyRepresentation = new FileRepresentation(viewFile, MediaType.valueOf(mimeType));
                    }
                    catch (URISyntaxException e)
                    {
                        msgStr = String.format("%s: %s", nsdURL, e.getMessage());
                        appLogger.error(msgStr, e);
                        msgStr = String.format("Cannot view document - invalid URI for document id '%s'.", docId);
                        replyRepresentation = messageRepresentation(msgStr);
                    }
                }
                else
                {
                    msgStr = String.format("Cannot view document - invalid URI for document id '%s'.", docId);
                    replyRepresentation = messageRepresentation(msgStr);
                }
            }
        }
        else
            replyRepresentation = messageRepresentation("Cannot view document - missing id in request.");

        appLogger.trace(appMgr.LOGMSG_TRACE_DEPART);

        return replyRepresentation;
    }
}
