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
import com.nridge.core.base.std.FilUtl;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import com.nridge.ds.ds_common.DSDocument;
import com.nridge.ds.solr.Solr;
import com.nridge.ds.solr.SolrDS;
import com.nridge.ds.solr.SolrDocumentXML;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

/**
 * The Publisher Solr class is responsible for publishing the
 * documents to the search index service.  The logic is currently
 * dedicated to supporting the Apache Solr search index.
 *
 * @see <a href="http://lucene.apache.org/solr/">Apache Solr</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class PSolr implements PublishInterface
{
    private SolrDS mSolrDS;
    private int mCurDocCount;
    private long mMaxDocCount;
    private int mBatchDocCount;
    private long mTotalDocCount;
    private int mCommitDocCount;
    private final AppMgr mAppMgr;
    private String mCfgPropertyPrefix;
    private final CrawlQueue mCrawlQueue;
    private ArrayList<Document> mDocuments;
    private SolrDocumentXML mSolrDocumentXML;

    /**
     * Default constructor that accepts an AppMgr instance for
     * configuration and logging purposes.
     *
     * @param anAppMgr Application manager instance.
     * @param aCrawlQueue Crawl queue instance.
     * @param aCfgPropertyPrefix Property prefix.
     */
    public PSolr(final AppMgr anAppMgr, final CrawlQueue aCrawlQueue,
                 String aCfgPropertyPrefix)
    {
        mAppMgr = anAppMgr;
        mCrawlQueue = aCrawlQueue;
        mDocuments = new ArrayList<>();
        mCfgPropertyPrefix = Connector.CFG_PROPERTY_PREFIX;

        mCurDocCount = 0;
        mTotalDocCount = 0L;
        setCfgPropertyPrefix(aCfgPropertyPrefix);
        mMaxDocCount = Connector.PUBLISH_MAX_DOC_COUNT;
        mBatchDocCount = Connector.PUBLISH_BATCH_DOC_COUNT;
        mCommitDocCount = Connector.PUBLISH_COMMIT_DOC_COUNT;
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

        if (StringUtils.startsWith(aSuffix, "."))
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

        if (StringUtils.startsWith(aSuffix, "."))
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

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getInt(propertyName, aDefaultValue);
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
    private long getCfgLong(String aSuffix, long aDefaultValue)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getLong(propertyName, aDefaultValue);
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

    /**
     * Validates that the publish feature is properly configured
     * to run as part of the parent application pipeline.
     *
     * @throws NSException Indicates a configuration issue.
     */
    @Override
    public void validate() throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "validate");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (isCfgStringTrue("publish.upload_enabled"))
        {
            mSolrDS = new SolrDS(mAppMgr);
            mSolrDS.setCfgPropertyPrefix(getCfgPropertyPrefix() + ".solr");
            mSolrDS.count();
            mSolrDS.shutdown();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Returns the data source document instance currently being managed by the
     * publisher component.
     *
     * @return Data source document instance.
     */
    @Override
    public DSDocument getDS()
    {
        return mSolrDS;
    }

    private void createOpenSolrFile()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "createOpenSolrFile");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (isCfgStringTrue("publish.save_files"))
        {
            String publishPathName = mCrawlQueue.crawlPathName(Connector.QUEUE_PUBLISH_NAME);
            String publishPathFileName = FilUtl.generateUniquePathFileName(publishPathName, "solr", ".xml");
            try
            {
                PrintWriter printWriter = new PrintWriter(publishPathFileName, StrUtl.CHARSET_UTF_8);
                mSolrDocumentXML = new SolrDocumentXML(mAppMgr, Solr.DOC_OPERATION_ADD, printWriter);
                mSolrDocumentXML.setIncludeChildrenFlag(isCfgStringTrue("solr.save_children"));
                mSolrDocumentXML.writeHeader();
            }
            catch (IOException e)
            {
                String msgStr = String.format("%s: %s", publishPathFileName, e.getMessage());
                throw new NSException(msgStr);
            }
        }
        else
            mSolrDocumentXML = null;

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Initializes the publishing process by configuring the data source
     * components and initializing the internal state of the tracking
     * variables.
     *
     * @param aSchemaBag Data bag instance defining a schema.
     *
     * @throws NSException Indicates a configuration or I/O error condition.
     */
    @Override
    public void initialize(DataBag aSchemaBag)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "initialize");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mCurDocCount = 0;
        mTotalDocCount = 0L;
        mDocuments.clear();

        mSolrDS = new SolrDS(mAppMgr);
        mSolrDS.setCfgPropertyPrefix(getCfgPropertyPrefix() + ".solr");
        mSolrDS.setIncludeChildrenFlag(isCfgStringTrue("solr.save_children"));
        if (aSchemaBag != null)
            mSolrDS.setSchema(aSchemaBag);

        mMaxDocCount = getCfgLong("publish.feed_maximum_count", Connector.PUBLISH_MAX_DOC_COUNT);
        mBatchDocCount = getCfgInteger("publish.feed_batch_count", Connector.PUBLISH_BATCH_DOC_COUNT);
        mCommitDocCount = getCfgInteger("publish.feed_commit_count", Connector.PUBLISH_COMMIT_DOC_COUNT);
        if (mMaxDocCount <= 0)
            mMaxDocCount = Long.MAX_VALUE;
        if (mCommitDocCount <= 0L)
            mCommitDocCount = Integer.MAX_VALUE;

        createOpenSolrFile();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Adds the document to the configured publishing endpoint (e.g. search index,
     * NoSQL storage)..
     *
     * @param aDocument Document instance.
     * @throws NSException Indicates a data validation error condition.
     */
    @Override
    public void add(Document aDocument)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "add");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aDocument == null)
            throw new NSException("Document instance is null.");

        if (mTotalDocCount < mMaxDocCount)
        {
            mDocuments.add(aDocument);
            mCurDocCount++;
            mTotalDocCount++;

            if (mCurDocCount >= mBatchDocCount)
            {
                if (isCfgStringTrue("publish.upload_enabled"))
                    mSolrDS.add(mDocuments);
                if (mSolrDocumentXML != null)
                {
                    mSolrDocumentXML.writeContent(mDocuments, 1);
                    mSolrDocumentXML.writeTrailerAndClose();
                    createOpenSolrFile();
                }
                mCurDocCount = 0;
                mDocuments.clear();
            }

            if (isCfgStringTrue("publish.upload_enabled"))
            {
                if ((mTotalDocCount > 0) && (mCommitDocCount > 0))
                {
                    if ((mTotalDocCount % mCommitDocCount) == 0)
                    {
                        mSolrDS.commit();
                        if (mSolrDocumentXML != null)
                            mSolrDocumentXML.writeCommit();
                    }
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Flushes the the internally managed memory list of documents to
     * the publishing service and resetting the internal state variables.
     *
     * @throws NSException Indicates a configuration error condition.
     */
    public void flushAndCommit()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "flushAndCommit");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mDocuments.size() > 0)
        {
            if (isCfgStringTrue("publish.upload_enabled"))
            {
                mSolrDS.add(mDocuments);
                mSolrDS.commit();
                mTotalDocCount += mDocuments.size();
            }
            if (mSolrDocumentXML != null)
                mSolrDocumentXML.writeContent(mDocuments, 1);

            mCurDocCount = 0;
            mDocuments.clear();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Completes the publishing process by flushing the internally managed
     * memory list of documents to the search service index and resetting
     * the internal state variables.
     *
     * @throws NSException Indicates a configuration error condition.
     */
    @Override
    public void shutdown()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "shutdown");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        flushAndCommit();

        if (isCfgStringTrue("publish.upload_enabled"))
        {
            if (isCfgStringTrue("publish.optimize_upon_completion"))
                mSolrDS.optimize();

            mSolrDS.shutdown();
        }
        if (mSolrDocumentXML != null)
        {
            mSolrDocumentXML.writeCommit();
            if (isCfgStringTrue("publish.optimize_upon_completion"))
                mSolrDocumentXML.writeOptimize();
            mSolrDocumentXML.writeTrailerAndClose();
            mSolrDocumentXML = null;
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
