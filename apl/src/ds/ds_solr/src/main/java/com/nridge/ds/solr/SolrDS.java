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

package com.nridge.ds.solr;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.std.StrUtl;
import com.nridge.ds.ds_common.DSDocument;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.std.NSException;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;

import java.util.ArrayList;

/**
 * The SolDS data source supports CRUD operations and advanced
 * queries using a <i>DSCriteria</i> against an Apache Lucene/Solr
 * managed index.
 *
 * @see <a href="http://lucene.apache.org/solr/">Apache Solr</a>
 * @see <a href="http://wiki.apache.org/solr/FrontPage">Apache Solr Wiki</a>
 * @see <a href="https://github.com/LucidWorks/solrj-nested-docs/blob/master/NestedDocs2.java">Solr Child Document</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class SolrDS extends DSDocument
{
    private final String DS_TYPE_NAME = "Solr";
    private final String DS_TITLE_DEFAULT = "Solr Data Source";

    private SolrServer mSolrServer;
    private boolean mIncludeChildren;
    private SolrQueryBuilder mSolrQueryBuilder;
    private String mSolrIdentity = StringUtils.EMPTY;

    /**
     * Constructor accepts an application manager parameter and initializes
     * the data source accordingly.
     *
     * @param anAppMgr Application manager.
     */
    public SolrDS(AppMgr anAppMgr)
    {
        super(anAppMgr);
        setName(DS_TYPE_NAME);
        setTitle(DS_TITLE_DEFAULT);
        setCfgPropertyPrefix(Solr.CFG_PROPERTY_PREFIX);
    }

    /**
     * Constructor accepts an application manager parameter and initializes
     * the data source accordingly.
     *
     * @param anAppMgr Application manager.
     * @param aName Name of the data source.
     */
    public SolrDS(AppMgr anAppMgr, String aName)
    {
        super(anAppMgr, aName);
        setCfgPropertyPrefix(Solr.CFG_PROPERTY_PREFIX);
    }

    /**
     * Constructor accepts an application manager parameter and initializes
     * the data source accordingly.
     *
     * @param anAppMgr Application manager.
     * @param aName Name of the data source.
     * @param aTitle Title of the data source.
     */
    public SolrDS(AppMgr anAppMgr, String aName, String aTitle)
    {
        super(anAppMgr, aName, aTitle);
        setCfgPropertyPrefix(Solr.CFG_PROPERTY_PREFIX);
    }

    /**
     * Creates a Solr Server instance for use within the SolrJ framework.
     * This is an advanced method that should not be used for standard
     * data source operations.
     *
     * @return Solr server instance.
     *
     * @throws DSException Data source related exception.
     */
    public SolrServer createSolrServer()
        throws DSException
    {
        SolrServer solrServer;
        Logger appLogger = mAppMgr.getLogger(this, "createSolrServer");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = getCfgPropertyPrefix() + ".cloud_zk_host_names";
        String zookeeperHosts = mAppMgr.getString(propertyName);
        if (StringUtils.isNotEmpty(zookeeperHosts))
        {
            CloudSolrServer cloudSolrServer = new CloudSolrServer(zookeeperHosts);
            propertyName = getCfgPropertyPrefix() + ".cloud_collection";
            String defaultCollection = mAppMgr.getString(propertyName);
            if (StringUtils.isNotEmpty(defaultCollection))
            {
                mSolrIdentity = String.format("SolrCloud (%s)", defaultCollection);
                cloudSolrServer.setDefaultCollection(defaultCollection);
            }
            propertyName = getCfgPropertyPrefix() + ".cloud_zk_timeout";
            int zookeeperTimeout = mAppMgr.getInt(propertyName, Solr.CONNECTION_TIMEOUT_MINIMUM);
            if (zookeeperTimeout > Solr.CONNECTION_TIMEOUT_MINIMUM)
                cloudSolrServer.setZkConnectTimeout(zookeeperTimeout);
            solrServer = cloudSolrServer;
            appLogger.debug(String.format("SolrCloud: %s (%s) - %d connection timeout.",
                                          zookeeperHosts, defaultCollection, zookeeperTimeout));
        }
        else
        {
            propertyName = getCfgPropertyPrefix() + ".request_uri";
            String solrBaseURL = mAppMgr.getString(propertyName);
            if (StringUtils.isEmpty(solrBaseURL))
            {
                String msgStr = String.format("%s: Property is undefined.", propertyName);
                throw new DSException(msgStr);
            }
            solrServer = new HttpSolrServer(solrBaseURL);
            mSolrIdentity = String.format("SolrServer (%s)", solrBaseURL);
            appLogger.debug(mSolrIdentity);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return solrServer;
    }

    private void initialize()
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "initialize");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mSolrQueryBuilder == null)
        {
            mSolrQueryBuilder = new SolrQueryBuilder(mAppMgr);
            mSolrQueryBuilder.setCfgPropertyPrefix(getCfgPropertyPrefix());
            mSolrServer = createSolrServer();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private SolrResponseBuilder createResponseBuilder()
    {
        DataBag schemaBag = getSchema();
        SolrResponseBuilder solrResponseBuilder = new SolrResponseBuilder(mAppMgr, schemaBag);
        solrResponseBuilder.setCfgPropertyPrefix(getCfgPropertyPrefix());

        return solrResponseBuilder;
    }

    /**
     * Assigns the data bag instance as the schema for the
     * document data source.
     *
     * @param aBag Data bag instance.
     */
    @Override
    public void setSchema(DataBag aBag)
    {
        super.setSchema(aBag);
    }

    /**
     * If assigned <i>true</i>, then child documents will be
     * included in add/update operations.
     *
     * @param aFlag Enable/disable flag.
     */
    public void setIncludeChildrenFlag(boolean aFlag)
    {
        mIncludeChildren = aFlag;
    }

    /**
     * Given a Solr Document (one that was previously populated from a
     * call to <code>fetch()</code>, it will return a table containing
     * the rows from the result set.
     *
     * @param aSolrDocument Data source Solr document instance.
     *
     * @return Data table instance or <i>null</i> if empty.
     */
    public DataTable getResultTable(Document aSolrDocument)
    {
        Logger appLogger = mAppMgr.getLogger(this, "getResultTable");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataTable resultTable = null;
        if (aSolrDocument != null)
        {
            Relationship documentRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_DOCUMENT);
            if (documentRelationship != null)
            {
                if (documentRelationship.count() > 0)
                {
                    Document resultDocument = documentRelationship.getDocuments().get(0);
                    resultTable = resultDocument.getTable();
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return resultTable;
    }

    /**
     * Convenience method that executes the Solr query based on
     * the HTTP method property.
     *
     * @param aSolrQuery Solr query instance.
     *
     * @return Solr response instance.
     */
    public QueryResponse queryExecute(SolrQuery aSolrQuery)
        throws DSException
    {
        QueryResponse queryResponse;
        Logger appLogger = mAppMgr.getLogger(this, "queryExecute");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = getCfgPropertyPrefix() + ".request_method";
        String requestMethod = mAppMgr.getString(propertyName);

        try
        {
            if (StringUtils.equalsIgnoreCase(requestMethod, "POST"))
                queryResponse = mSolrServer.query(aSolrQuery, SolrRequest.METHOD.POST);
            else
                queryResponse = mSolrServer.query(aSolrQuery, SolrRequest.METHOD.GET);
        }
        catch (SolrServerException e)
        {
            appLogger.error(e.getMessage(), e);
            throw new DSException(e.getMessage());
        }

        int statusCode = queryResponse.getStatus();
        if (statusCode != Solr.RESPONSE_STATUS_SUCCESS)
        {
            String msgStr = String.format("Solr query failed with a response code of %d.", statusCode);
            throw new DSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return queryResponse;
    }

    /**
     * Calculates a count (using a wildcard criteria) of all the
     * rows stored in the content source and returns that value.
     *
     * @return Count of all rows in the content source.
     * @throws DSException Data source related exception.
     */
    @Override
    public int count()
        throws DSException
    {
        int documentCount;
        Logger appLogger = mAppMgr.getLogger(this, "count");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        DSCriteria dsCriteria = new DSCriteria("Solr Query");
        dsCriteria.add(Solr.FIELD_QUERY_NAME, Field.Operator.EQUAL, Solr.QUERY_ALL_DOCUMENTS);

        SolrQuery solrQuery = mSolrQueryBuilder.create(dsCriteria);
        solrQuery.setStart(0);
        solrQuery.setRows(1);

        appLogger.debug(String.format("%s: %s %s", dsCriteria.getName(),
                                      mSolrIdentity, solrQuery.toString()));

        QueryResponse queryResponse = queryExecute(solrQuery);
        SolrDocumentList solrDocumentList = queryResponse.getResults();
        documentCount = (int) solrDocumentList.getNumFound();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return documentCount;
    }

    /**
     * Returns a count of rows that match the <i>DSCriteria</i> specified
     * in the parameter.
     *
     * @param aDSCriteria Data source criteria.
     *
     * @return Count of rows matching the data source criteria.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public int count(DSCriteria aDSCriteria)
        throws DSException
    {
        int documentCount;
        Logger appLogger = mAppMgr.getLogger(this, "count");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        SolrQuery solrQuery = mSolrQueryBuilder.create(aDSCriteria);
        solrQuery.setStart(0);
        solrQuery.setRows(1);

        appLogger.debug(String.format("%s: %s %s", aDSCriteria.getName(),
                                      mSolrIdentity, solrQuery.toString()));

        QueryResponse queryResponse = queryExecute(solrQuery);
        SolrDocumentList solrDocumentList = queryResponse.getResults();
        documentCount = (int) solrDocumentList.getNumFound();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return documentCount;
    }

    /**
     * Returns a <i>Document</i> representation of all documents
     * fetched from the underlying content source (using a wildcard
     * criteria).
     * <p>
     * <b>Note:</b> Depending on the size of the content source
     * behind this data source, this method could consume large
     * amounts of heap memory.  Therefore, it should only be
     * used when the number of column and rows is known to be
     * small in size.
     * </p>
     *
     * @return Document hierarchy representing all documents in
     * the content source.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public Document fetch()
        throws DSException
    {
        Document solrDocument;
        Logger appLogger = mAppMgr.getLogger(this, "fetch");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        DSCriteria dsCriteria = new DSCriteria("Solr Query");
        dsCriteria.add(Solr.FIELD_QUERY_NAME, Field.Operator.EQUAL, Solr.QUERY_ALL_DOCUMENTS);

        SolrQuery solrQuery = mSolrQueryBuilder.create(dsCriteria);
        solrQuery.setStart(Solr.QUERY_OFFSET_DEFAULT);
        solrQuery.setRows(Solr.QUERY_PAGESIZE_DEFAULT);

        appLogger.debug(String.format("%s: %s %s", dsCriteria.getName(),
                                      mSolrIdentity, solrQuery.toString()));

        SolrResponseBuilder solrResponseBuilder = createResponseBuilder();
        QueryResponse queryResponse = queryExecute(solrQuery);
        solrDocument = solrResponseBuilder.extract(queryResponse);
        String requestHandler = solrQuery.getRequestHandler();
        solrResponseBuilder.updateHeader(solrQuery.getQuery(), requestHandler,
                                         solrQuery.getStart(), solrQuery.getRows());

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return solrDocument;
    }

    /**
     * Returns a <i>Document</i> representation of the documents
     * that match the <i>DSCriteria</i> specified in the parameter.
     * <p>
     * <b>Note:</b> Depending on the size of the content source
     * behind this data source and the criteria specified, this
     * method could consume large amounts of heap memory.
     * Therefore, the developer is encouraged to use the alternative
     * method for fetch where an offset and limit parameter can be
     * specified.
     * </p>
     *
     * @param aDSCriteria Data source criteria.
     *
     * @return Document hierarchy representing all documents that
     * match the criteria in the content source.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public Document fetch(DSCriteria aDSCriteria)
        throws DSException
    {
        Document solrDocument;
        Logger appLogger = mAppMgr.getLogger(this, "fetch");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        SolrQuery solrQuery = mSolrQueryBuilder.create(aDSCriteria);

        appLogger.debug(String.format("%s: %s %s", aDSCriteria.getName(),
                                      mSolrIdentity, solrQuery.toString()));

        SolrResponseBuilder solrResponseBuilder = createResponseBuilder();
        QueryResponse queryResponse = queryExecute(solrQuery);
        solrDocument = solrResponseBuilder.extract(queryResponse);
        String requestHandler = solrQuery.getRequestHandler();
        solrResponseBuilder.updateHeader(solrQuery.getQuery(), requestHandler,
                                         solrQuery.getStart(), solrQuery.getRows());
        if (Solr.isCriteriaParentChild(aDSCriteria))
        {
            SolrParentChild solrParentChild = new SolrParentChild(mAppMgr, this);
            solrParentChild.expand(solrDocument, aDSCriteria);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return solrDocument;
    }

    /**
     * Returns a <i>Document</i> representation of the documents
     * that match the <i>DSCriteria</i> specified in the parameter.
     * In addition, this method offers a paging mechanism where the
     * starting offset and a fetch limit can be applied to each
     * content fetch query.
     *
     * @param aDSCriteria Data source criteria.
     * @param anOffset    Starting offset into the matching content rows.
     * @param aLimit      Limit on the total number of rows to extract from
     *                    the content source during this fetch operation.
     *
     * @return Document hierarchy representing all documents that
     * match the criteria in the content source. (based on the offset
     * and limit values).
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public Document fetch(DSCriteria aDSCriteria, int anOffset, int aLimit)
        throws DSException
    {
        Document solrDocument;
        Logger appLogger = mAppMgr.getLogger(this, "fetch");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        SolrQuery solrQuery = mSolrQueryBuilder.create(aDSCriteria);
        solrQuery.setStart(anOffset);
        solrQuery.setRows(aLimit);

        appLogger.debug(String.format("%s: %s %s", aDSCriteria.getName(),
                                      mSolrIdentity, solrQuery.toString()));

        SolrResponseBuilder solrResponseBuilder = createResponseBuilder();
        QueryResponse queryResponse = queryExecute(solrQuery);
        solrDocument = solrResponseBuilder.extract(queryResponse, anOffset, aLimit);
        String requestHandler = solrQuery.getRequestHandler();
        solrResponseBuilder.updateHeader(solrQuery.getQuery(), requestHandler,
                                         anOffset, aLimit);
        if (Solr.isCriteriaParentChild(aDSCriteria))
        {
            SolrParentChild solrParentChild = new SolrParentChild(mAppMgr, this);
            solrParentChild.expand(solrDocument, aDSCriteria);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return solrDocument;
    }

    private SolrInputDocument toSolrInputDocument(DataBag aBag)
        throws NSException
    {
        String fieldValueString;
        Logger appLogger = mAppMgr.getLogger(this, "toSolrInputDocument");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        SolrInputDocument solrInputDocument = new SolrInputDocument();

        DataField primaryKeyField = aBag.getPrimaryKeyField();
        if ((primaryKeyField == null) || (! primaryKeyField.isAssigned()))
            throw new NSException("Primary field is undefined or unpopulated.");
        if (aBag.count() > 0)
        {
            if (aBag.isValid())
            {
                for (DataField dataField : aBag.getFields())
                {
                    if ((dataField.isAssigned()) && (dataField.isFeatureFalse(Field.FEATURE_IS_HIDDEN)))
                    {
                        fieldValueString = dataField.getValue();
                        if (StringUtils.isNotEmpty(fieldValueString))
                        {
                            if (dataField.isMultiValue())
                            {
                                ArrayList<String> fieldValues = dataField.getValues();
                                for (String fieldValue : fieldValues)
                                    solrInputDocument.addField(dataField.getName(), fieldValue);
                            }
                            else
                                solrInputDocument.addField(dataField.getName(), dataField.getValueAsObject());
                        }
                    }
                }
            }
            else
            {
                ArrayList<String> msgList = aBag.getValidationMessages();
                if (msgList.size() > 0)
                {
                    StringBuilder stringBuilder = new StringBuilder();
                    for (String message : msgList)
                    {
                        if (stringBuilder.length() > 0)
                            stringBuilder.append(StrUtl.CHAR_COMMA);
                        stringBuilder.append(message);
                    }
                    appLogger.error(stringBuilder.toString());
                }
                throw new DSException("The data bag is not valid and cannot be added to Solr index.");
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return solrInputDocument;
    }

    private SolrInputDocument toSolrInputDocument(Document aDocument)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "toSolrInputDocument");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        SolrInputDocument solrInputDocument = toSolrInputDocument(aDocument.getBag());
        if (mIncludeChildren)
        {
            ArrayList<Relationship> relationshipList = aDocument.getRelationships();
            for (Relationship relationship : relationshipList)
                solrInputDocument.addChildDocument(toSolrInputDocument(relationship.getBag()));
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return solrInputDocument;
    }

    /**
     * Adds the field values captured in the <i>Document</i> to
     * the content source.  The fields must be derived from the
     * same collection defined in the schema definition.
     * <p>
     * <b>Note:</b> Depending on the data source and its ability
     * to support transactions, you may need to apply
     * <code>commit()</code> and <code>rollback()</code>
     * logic around this method.
     * </p>
     *
     * @param aDocument Document to store.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public void add(Document aDocument)
        throws DSException
    {
        UpdateResponse updateResponse;
        Logger appLogger = mAppMgr.getLogger(this, "add");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aDocument == null)
            throw new DSException("Document is null.");

        initialize();

        ArrayList<SolrInputDocument> solrInputDocuments = new ArrayList<SolrInputDocument>();
        try
        {
            solrInputDocuments.add(toSolrInputDocument(aDocument));
            updateResponse = mSolrServer.add(solrInputDocuments);
        }
        catch (Exception e)
        {
            appLogger.error(e.getMessage(), e);
            throw new DSException(e.getMessage());
        }

        if (updateResponse.getStatus() != Solr.RESPONSE_STATUS_SUCCESS)
        {
            String msgStr = String.format("%s: Response contained non success status code of %d.",
                                          updateResponse.getRequestUrl(),updateResponse.getStatus());
            appLogger.error(msgStr);
            throw new DSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Adds the field values captured in the array of <i>Document</i>
     * to the content source.  The fields must be derived from the
     * same collection defined in the schema definition.
     * <p>
     * <b>Note:</b> Depending on the data source and its ability
     * to support transactions, you may need to apply
     * <code>commit()</code> and <code>rollback()</code>
     * logic around this method.
     * </p>
     *
     * @param aDocuments An array of Documents to store.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public void add(ArrayList<Document> aDocuments)
        throws DSException
    {
        UpdateResponse updateResponse;
        Logger appLogger = mAppMgr.getLogger(this, "add");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aDocuments == null)
            throw new DSException("Document list is null.");

        initialize();

        ArrayList<SolrInputDocument> solrInputDocuments = new ArrayList<SolrInputDocument>();
        try
        {
            for (Document document : aDocuments)
                solrInputDocuments.add(toSolrInputDocument(document));
            updateResponse = mSolrServer.add(solrInputDocuments);
        }
        catch (Exception e)
        {
            appLogger.error(e.getMessage(), e);
            throw new DSException(e.getMessage());
        }

        if (updateResponse.getStatus() != Solr.RESPONSE_STATUS_SUCCESS)
        {
            String msgStr = String.format("%s: Response contained non success status code of %d.",
                                          updateResponse.getRequestUrl(),updateResponse.getStatus());
            appLogger.error(msgStr);
            throw new DSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Updates the field values captured in the <i>Document</i>
     * within the content source.  The fields must be derived from the
     * same collection defined in the schema definition.
     * <p>
     * <b>Note:</b> The Document must designate a field as a primary
     * key and that value must be assigned prior to using this
     * method.  Also, depending on the data source and its ability
     * to support transactions, you may need to apply
     * <code>commit()</code> and <code>rollback()</code>
     * logic around this method.
     * </p>
     *
     * @param aDocument Document to update.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public void update(Document aDocument)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "update");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        add(aDocument);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Updates the field values captured in the array of
     * <i>Document</i> within the content source.  The fields must
     * be derived from the same collection defined in the schema
     * definition.
     * <p>
     * <b>Note:</b> The Document must designate a field as a primary
     * key and that value must be assigned prior to using this
     * method.  Also, depending on the data source and its ability
     * to support transactions, you may need to apply
     * <code>commit()</code> and <code>rollback()</code>
     * logic around this method.
     * </p>
     *
     * @param aDocuments An array of Documents to update.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public void update(ArrayList<Document> aDocuments)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "update");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        add(aDocuments);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void bagToSolrForDelete(ArrayList<String> aDocIds, DataBag aBag)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "bagToSolrForDelete");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataField primaryKeyField = aBag.getPrimaryKeyField();
        if ((primaryKeyField == null) || (! primaryKeyField.isAssigned()))
            throw new NSException("Primary field is undefined or unpopulated.");
        else
            aDocIds.add(primaryKeyField.getValue());

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void delSingleDocument(ArrayList<String> aDocIds, Document aDocument)
        throws DSException
    {
        DataBag dataBag;

        Logger appLogger = mAppMgr.getLogger(this, "delSingleDocument");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataTable dataTable = aDocument.getTable();
        int rowCount = dataTable.rowCount();
        if (rowCount > 0)
        {
            for (int row = 0; row < rowCount; row++)
            {
                dataBag = dataTable.getRowAsBag(row);
                try
                {
                    bagToSolrForDelete(aDocIds, dataBag);
                }
                catch (NSException e)
                {
                    String msgStr = String.format("%s [%d]: %s", dataTable.getName(), row, e.getMessage());
                    appLogger.error(msgStr);
                    throw new DSException(msgStr);
                }
            }
        }
        else
        {
            dataBag = aDocument.getBag();
            try
            {
                bagToSolrForDelete(aDocIds, dataBag);
            }
            catch (NSException e)
            {
                String msgStr = String.format("%s: %s", dataBag.getName(), e.getMessage());
                appLogger.error(msgStr);
                throw new DSException(msgStr);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Deletes the document identified by the <i>Document</i> from
     * the content source.  The fields must be derived from the
     * same collection defined in the schema definition.
     * <p>
     * <b>Note:</b> The document must designate a field as a primary
     * key and that value must be assigned prior to using this
     * method. Also, depending on the data source and its ability
     * to support transactions, you may need to apply
     * <code>commit()</code> and <code>rollback()</code>
     * logic around this method.
     * </p>
     *
     * @param aDocument Document where the primary key field value is
     *                  assigned.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public void delete(Document aDocument)
        throws DSException
    {
        UpdateResponse updateResponse;
        Logger appLogger = mAppMgr.getLogger(this, "delete");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        ArrayList<String> docIds = new ArrayList<String>();
        delSingleDocument(docIds, aDocument);

        try
        {
            updateResponse = mSolrServer.deleteById(docIds);
        }
        catch (Exception e)
        {
            appLogger.error(e.getMessage(), e);
            throw new DSException(e.getMessage());
        }

        if (updateResponse.getStatus() != Solr.RESPONSE_STATUS_SUCCESS)
        {
            String msgStr = String.format("%s: Response contained non success status code of %d.",
                                          updateResponse.getRequestUrl(),updateResponse.getStatus());
            appLogger.error(msgStr);
            throw new DSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Deletes the documents identified by the array of <i>Document</i>
     * from the content source.  The fields must be derived from the
     * same collection defined in the schema definition.
     * <p>
     * <b>Note:</b> The bag must designate a field as a primary
     * key and that value must be assigned prior to using this
     * method.
     * </p>
     *
     * @param aDocuments An array of documents where the primary key
     *                   field value is assigned.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    @Override
    public void delete(ArrayList<Document> aDocuments)
        throws DSException
    {
        UpdateResponse updateResponse;
        Logger appLogger = mAppMgr.getLogger(this, "delete");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        ArrayList<String> docIds = new ArrayList<String>();
        for (Document document : aDocuments)
            delSingleDocument(docIds, document);

        try
        {
            updateResponse = mSolrServer.deleteById(docIds);
        }
        catch (Exception e)
        {
            appLogger.error(e.getMessage(), e);
            throw new DSException(e.getMessage());
        }

        if (updateResponse.getStatus() != Solr.RESPONSE_STATUS_SUCCESS)
        {
            String msgStr = String.format("%s: Response contained non success status code of %d.",
                                          updateResponse.getRequestUrl(),updateResponse.getStatus());
            appLogger.error(msgStr);
            throw new DSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Deletes the documents based on the {@link com.nridge.core.base.ds.DSCriteria}.
     * The fields must be derived from the same collection defined in the schema
     * definition.
     * <p>
     * <b>Note:</b> Depending on the data source and its ability
     * to support transactions, you may need to apply
     * <code>commit()</code> and <code>rollback()</code>
     * logic around this method.
     * </p>
     *
     * @param aDSCriteria Data source criteria.
     *
     * @throws com.nridge.core.base.ds.DSException Data source related exception.
     */
    public void delete(DSCriteria aDSCriteria)
        throws DSException
    {
        UpdateResponse updateResponse;
        Logger appLogger = mAppMgr.getLogger(this, "delete");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        String solrQuery = mSolrQueryBuilder.createAsString(aDSCriteria);

        try
        {
            updateResponse = mSolrServer.deleteByQuery(solrQuery);
        }
        catch (Exception e)
        {
            appLogger.error(e.getMessage(), e);
            throw new DSException(e.getMessage());
        }

        if (updateResponse.getStatus() != Solr.RESPONSE_STATUS_SUCCESS)
        {
            String msgStr = String.format("%s: Response contained non success status code of %d.",
                                          updateResponse.getRequestUrl(),updateResponse.getStatus());
            appLogger.error(msgStr);
            throw new DSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Commits any staged add/update/delete transactions to the content
     * source.
     * <p>
     * <b>Note:</b> This method does not perform any commit operation
     * (it is a no-op).  The concrete class must override this method
     * if the underlying content source supports transactions.
     * </p>
     *
     * @throws DSException Data source related exception.
     */
    @Override
    public void commit()
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "commit");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        try
        {
            mSolrServer.commit();
        }
        catch (Exception e)
        {
            appLogger.error(e.getMessage(), e);
            throw new DSException(e.getMessage());
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Commits any staged add/update/delete transactions to the content
     * source.
     * <p>
     * <b>Note:</b> This method does not perform any commit operation
     * (it is a no-op).  The concrete class must override this method
     * if the underlying content source supports transactions.
     * </p>
     *
     * @throws DSException Data source related exception.
     */
    @Override
    public void rollback()
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "rollback");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        try
        {
            mSolrServer.rollback();
        }
        catch (Exception e)
        {
            appLogger.error(e.getMessage(), e);
            throw new DSException(e.getMessage());
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Performs an explicit optimize, causing a merge of all segments to one.
     * <p>
     * <b>Note:</b> An optimize operation can take a long time to complete
     * depending on the number of documents stored within a index.
     * </p>
     *
     * @throws DSException Data source related exception.
     */
    public void optimize()
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "optimize");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        try
        {
            mSolrServer.optimize();
        }
        catch (Exception e)
        {
            appLogger.error(e.getMessage(), e);
            throw new DSException(e.getMessage());
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Releases any allocated resources for the data source session.
     *
     * <p>
     * <b>Note:</b> This method should be invoked once the data
     * source is no longer needed.
     * </p>
     */
    @Override
    public void shutdown()
    {
        Logger appLogger = mAppMgr.getLogger(this, "shutdown");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mSolrServer != null)
        {
            mSolrServer.shutdown();
            mSolrServer = null;
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
