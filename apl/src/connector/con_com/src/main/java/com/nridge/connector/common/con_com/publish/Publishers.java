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

package com.nridge.connector.common.con_com.publish;

import com.nridge.connector.common.con_com.Connector;
import com.nridge.connector.common.con_com.crawl.CrawlQueue;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import com.nridge.ds.ds_common.DSDocument;
import org.slf4j.Logger;

import java.util.HashMap;

/**
 * The Publishers is responsible for executing one or more
 * document publications on behalf of a connector.
 *
 * @since 1.0
 * @author Al Cole
 */
public class Publishers
{
    private final AppMgr mAppMgr;
    private final CrawlQueue mCrawlQueue;
    private HashMap<String, PublishInterface> mPublishers;
    private String mCfgPropertyPrefix = Connector.CFG_PROPERTY_PREFIX;

    /**
     * Default constructor that accepts an AppMgr instance for
     * configuration and logging purposes.
     *
     * @param anAppMgr Application manager instance.
     * @param aCrawlQueue Crawl queue instance.
     * @param aCfgPropertyPrefix Property prefix.
     */
    public Publishers(final AppMgr anAppMgr, final CrawlQueue aCrawlQueue,
                      String aCfgPropertyPrefix)
    {
        mAppMgr = anAppMgr;
        mCrawlQueue = aCrawlQueue;
        setCfgPropertyPrefix(aCfgPropertyPrefix);
        register();
    }

    /**
     * Returns the configuration property prefix string.
     *
     * @return Property prefix string.
     */
    public String getCfgPropertyPrefix()
    {
        return mCfgPropertyPrefix;
    }

    /**
     * Assigns a configuration property prefix string.
     *
     * @param aPropertyPrefix Property prefix.
     */
    public void setCfgPropertyPrefix(String aPropertyPrefix)
    {
        mCfgPropertyPrefix = aPropertyPrefix;
    }

    /**
     * Convenience method that returns the value of a property using
     * the concatenation of the property prefix and suffix values.
     *
     * @param aSuffix Property name suffix.
     * @return Matching property value.
     */
    private String getCfgString(String aSuffix)
    {
        String propertyName;

        if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName);
    }

    /**
     * Convenience method that returns the value of a property using
     * the concatenation of the property prefix and suffix values.
     * If the property is not found, then the default value parameter
     * will be returned.
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value.
     *
     * @return Matching property value or the default value.
     */
    private String getCfgString(String aSuffix, String aDefaultValue)
    {
        String propertyName;

        if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName, aDefaultValue);
    }

    /**
     * Returns a typed value for the property name identified
     * or the default value (if unmatched).
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value to return if property
     *                      name is not matched.
     *
     * @return Value of the property.
     */
    private int getCfgInteger(String aSuffix, int aDefaultValue)
    {
        String propertyName;

        if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getInt(propertyName, aDefaultValue);
    }

    /**
     * Returns <i>true</i> if the a property value evaluates to <i>true</i>.
     *
     * @param aSuffix Property name suffix.
     *
     * @return <i>true</i> or <i>false</i>
     */
    private boolean isCfgStringTrue(String aSuffix)
    {
        String propertyValue = getCfgString(aSuffix);
        return StrUtl.stringToBoolean(propertyValue);
    }

    private void register()
    {
        Logger appLogger = mAppMgr.getLogger(this, "register");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mPublishers = new HashMap<String, PublishInterface>();
        mPublishers.put("solr_ws", new PSolr(mAppMgr, mCrawlQueue, mCfgPropertyPrefix));

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Validates that the publishing feature is properly configured
     * to run as part of the parent application pipeline.
     *
     * @throws NSException Indicates a configuration issue.
     */
    public void validate()
        throws NSException
    {
        PublishInterface publishInterface;
        Logger appLogger = mAppMgr.getLogger(this, "validate");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = mCfgPropertyPrefix + ".publish.pipe_line";
        if (mAppMgr.isPropertyMultiValue(propertyName))
        {
            String[] publisherList = mAppMgr.getStringArray(propertyName);
            for (String publisherName : publisherList)
            {
                publishInterface = mPublishers.get(publisherName);
                if (publishInterface == null)
                {
                    String msgStr = String.format("%s: Unable to match pipeline publisher to class.",
                                                  publisherName);
                    throw new NSException(msgStr);
                }
                else
                    publishInterface.validate();
            }
        }
        else
        {
            String publisherName = mAppMgr.getString(propertyName);
            publishInterface = mPublishers.get(publisherName);
            if (publishInterface == null)
            {
                String msgStr = String.format("%s: Unable to match pipeline publisher to class.",
                                              publisherName);
                throw new NSException(msgStr);
            }
            else
                publishInterface.validate();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Returns the data source document instance currently being managed by the
     * publisher component.
     *
     * @param aName Publisher name.
     *
     * @return Data source document instance or <i>null</i> if it cannot match it.
     */
    public DSDocument getDS(String aName)
    {
        DSDocument dsDocument;
        Logger appLogger = mAppMgr.getLogger(this, "getDS");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        PublishInterface publishInterface = mPublishers.get(aName);
        if (publishInterface != null)
            dsDocument = publishInterface.getDS();
        else
            dsDocument = null;

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dsDocument;
    }

    /**
     * Initializes the publisher in anticipation of executing
     * document loads as part of the parent application pipeline.
     *
     * @param aSchemaBag Data bag instance defining a schema.
     *
     * @throws NSException Indicates a configuration issue.
     */
    public void initialize(DataBag aSchemaBag)
        throws NSException
    {
        PublishInterface publishInterface;
        Logger appLogger = mAppMgr.getLogger(this, "initialize");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = mCfgPropertyPrefix + ".publish.pipe_line";
        if (mAppMgr.isPropertyMultiValue(propertyName))
        {
            String[] publisherList = mAppMgr.getStringArray(propertyName);
            for (String publisherName : publisherList)
            {
                publishInterface = mPublishers.get(publisherName);
                if (publishInterface == null)
                {
                    String msgStr = String.format("%s: Unable to match pipeline publisher to class.",
                                                  publisherName);
                    throw new NSException(msgStr);
                }
                else
                    publishInterface.initialize(aSchemaBag);
            }
        }
        else
        {
            String publisherName = mAppMgr.getString(propertyName);
            publishInterface = mPublishers.get(publisherName);
            if (publishInterface == null)
            {
                String msgStr = String.format("%s: Unable to match pipeline publisher to class.",
                                              publisherName);
                throw new NSException(msgStr);
            }
            else
                publishInterface.initialize(aSchemaBag);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Sends the document instance to the registered publishers.
     *
     * @param aDocument Document instance.
     *
     * @throws NSException Indicates a configuration issue.
     */
    public void send(Document aDocument)
        throws NSException
    {
        PublishInterface publishInterface;
        Logger appLogger = mAppMgr.getLogger(this, "send");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = mCfgPropertyPrefix + ".publish.pipe_line";
        if (mAppMgr.isPropertyMultiValue(propertyName))
        {
            String[] publisherList = mAppMgr.getStringArray(propertyName);
            for (String publisherName : publisherList)
            {
                publishInterface = mPublishers.get(publisherName);
                if (publishInterface == null)
                {
                    String msgStr = String.format("%s: Unable to match pipeline publisher to class.",
                                                  publisherName);
                    throw new NSException(msgStr);
                }
                else
                    publishInterface.add(aDocument);
            }
        }
        else
        {
            String publisherName = mAppMgr.getString(propertyName);
            publishInterface = mPublishers.get(publisherName);
            if (publishInterface == null)
            {
                String msgStr = String.format("%s: Unable to match pipeline publisher to class.",
                                              publisherName);
                throw new NSException(msgStr);
            }
            else
                publishInterface.add(aDocument);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Performs a shutdown (e.g. resource de-allocation) on
     * each publisher associated with the parent application
     * pipeline.
     *
     * @throws NSException Indicates a configuration issue.
     */
    public void shutdown()
        throws NSException
    {
        PublishInterface publishInterface;
        Logger appLogger = mAppMgr.getLogger(this, "shutdown");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = mCfgPropertyPrefix + ".publish.pipe_line";
        if (mAppMgr.isPropertyMultiValue(propertyName))
        {
            String[] publisherList = mAppMgr.getStringArray(propertyName);
            for (String publisherName : publisherList)
            {
                publishInterface = mPublishers.get(publisherName);
                if (publishInterface == null)
                {
                    String msgStr = String.format("%s: Unable to match pipeline publisher to class.",
                                                  publisherName);
                    throw new NSException(msgStr);
                }
                else
                    publishInterface.shutdown();
            }
        }
        else
        {
            String publisherName = mAppMgr.getString(propertyName);
            publishInterface = mPublishers.get(publisherName);
            if (publishInterface == null)
            {
                String msgStr = String.format("%s: Unable to match pipeline publisher to class.",
                                              publisherName);
                throw new NSException(msgStr);
            }
            else
                publishInterface.shutdown();
        }

// Re-register publishers to clear out old instances.

        register();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
