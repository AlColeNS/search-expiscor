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

package com.nridge.ds.neo4j.ds_neo4j;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.data.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.slf4j.Logger;

/**
 * The Neo4jResponseBuilder provides a collection of methods that
 * can extract a response hierarchy from a Neo4j graph database
 * and store it in a {@link com.nridge.core.base.doc.Document} instance.
 * It is a helper class for the {@link Neo4jDS} class.
 *
 * @author Al Cole
 * @since 1.0
 */
public class Neo4jResponseBuilder
{
    private DataBag mBag;
    private Document mDocument;
    private final AppMgr mAppMgr;
    private Neo4jConvert mConverter;
    private String mCfgPropertyPrefix = StringUtils.EMPTY;

    /**
     * Constructor that accepts an application manager instance, but
     * does not specify an existing document with a schema defined.
     *
     * @param anAppMgr Application manager instance.
     * @param aConverter Object converter instance.
     */
    public Neo4jResponseBuilder(final AppMgr anAppMgr, Neo4jConvert aConverter)
    {
        mAppMgr = anAppMgr;
        mConverter =aConverter;
        instantiate(null);
        setCfgPropertyPrefix(Neo4j.CFG_PROPERTY_PREFIX);
    }

    /**
     * Constructor that accepts an application manager instance and
     * an existing data bag that specifies and existing schema to
     * base the field parsing on.
     *
     * @param anAppMgr Application manager instance.
     * @param aConverter Object converter instance.
     * @param aBag Data bag instance.
     */
    public Neo4jResponseBuilder(AppMgr anAppMgr, Neo4jConvert aConverter, final DataBag aBag)
    {
        mAppMgr = anAppMgr;
        mConverter =aConverter;
        instantiate(aBag);
        setCfgPropertyPrefix(Neo4j.CFG_PROPERTY_PREFIX);
    }

    /**
     * Constructor that accepts an application manager instance and
     * an existing document that specifies and existing schema to
     * base the field parsing on.
     *
     * @param anAppMgr Application manager instance.
     * @param aDocument Document instance.
     */
    public Neo4jResponseBuilder(AppMgr anAppMgr, final Document aDocument)
    {
        mAppMgr = anAppMgr;
        instantiate(aDocument.getBag());
    }

    /**
     * Returns an reference to the internally managed Document
     * instance representing the Solr response payload.
     *
     * @return Document instance.
     */
    public Document getDocument()
    {
        return mDocument;
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
     * Assigns the configuration property prefix to the document data source.
     *
     * @param aPropertyPrefix Property prefix.
     */
    public void setCfgPropertyPrefix(String aPropertyPrefix)
    {
        mCfgPropertyPrefix = aPropertyPrefix;
    }

    /**
     * Convenience method that returns the value of an application
     * manager configuration property using the concatenation of
     * the property prefix and suffix values.
     *
     * @param aSuffix Property name suffix.
     *
     * @return Matching property value.
     */
    public String getCfgString(String aSuffix)
    {
        String propertyName;

        if (org.apache.commons.lang3.StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName);
    }

    private DataBag createHeaderBag()
    {
        DataBag headerBag = new DataBag("Header Fields");

// Assigned before query executes.

        headerBag.add(new DataLongField("offset_start", "Offset Start"));
        headerBag.add(new DataIntegerField("query_limit", "Query Limit"));
        headerBag.add(new DataIntegerField("offset_finish", "Offset Finish"));
        headerBag.add(new DataTextField("resolve_to", "Resolve To"));
        headerBag.add(new DataTextField("direction", "Direction"));

// Assigned after query executes.

        headerBag.add(new DataIntegerField("status_code", "Status Code"));
        headerBag.add(new DataIntegerField("query_time", "Query Time"));
        headerBag.add(new DataTextField("status_message", "Status Message"));

        return headerBag;
    }

    private void instantiate(final DataBag aBag)
    {
        if (aBag == null)
            mBag = new DataBag("Neo4j Response Bag");
        else
            mBag = new DataBag(aBag);
        mBag.setAssignedFlagAll(false);
        mDocument = new Document(Neo4j.DOCUMENT_TYPE, mBag);
        mDocument.addRelationship(Neo4j.RESPONSE_HEADER, createHeaderBag());
        mDocument.addRelationship(Neo4j.RESPONSE_DOCUMENT, mBag);
    }

    private void resetHeader()
    {
        Relationship headerRelationship = mDocument.getFirstRelationship(Neo4j.RESPONSE_HEADER);
        if (headerRelationship != null)
        {
            DataBag headerBag = headerRelationship.getBag();
            headerBag.resetValues();
        }
    }

    private void populateHeader(StopWatch aStopWatch, int aStatusCode, String aMessage)
    {
        Logger appLogger = mAppMgr.getLogger(this, "populateHeader");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Relationship headerRelationship = mDocument.getFirstRelationship(Neo4j.RESPONSE_HEADER);
        if (headerRelationship != null)
        {
            DataBag headerBag = headerRelationship.getBag();
            aStopWatch.stop();
            headerBag.setValueByName("query_time", aStopWatch.getTime());
            headerBag.setValueByName("status_code", aStatusCode);
            if (StringUtils.isNotEmpty(aMessage))
                headerBag.setValueByName("status_message", aMessage);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void populateHeader(String aResolveTo, String aDirection, int aLimit)
    {
        Logger appLogger = mAppMgr.getLogger(this, "populateHeader");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Relationship headerRelationship = mDocument.getFirstRelationship(Neo4j.RESPONSE_HEADER);
        if (headerRelationship != null)
        {
            DataBag headerBag = headerRelationship.getBag();
            if (StringUtils.isNotEmpty(aResolveTo))
                headerBag.setValueByName("resolve_to", aResolveTo);
            if (StringUtils.isNotEmpty(aDirection))
                headerBag.setValueByName("direction", aDirection);
            headerBag.setValueByName("offset_start", 0);
            headerBag.setValueByName("query_limit", aLimit);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void populateDocument(Iterable<Node> aNodes, String aResolveTo,
                                  String aDirection, int aLimit,
                                  StopWatch aStopWatch)
    {
        DataBag docBag;
        Document docTree;
        Logger appLogger = mAppMgr.getLogger(this, "populateDocument");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Relationship documentRelationship = mDocument.getFirstRelationship(Neo4j.RESPONSE_DOCUMENT);
        if (documentRelationship != null)
        {
            int nodeCount = 0;
            documentRelationship.getDocuments().clear();
            if (StringUtils.equals(aResolveTo, Neo4j.RESOLVE_TO_NODE_TREE))
            {
                for (Node node : aNodes)
                {
                    nodeCount++;
                    docTree = mConverter.convertNodeToDocument(node, aResolveTo, aDirection);
                    documentRelationship.add(docTree);
                    if ((aLimit > 0) && (nodeCount >= aLimit))
                        break;
                }
            }
            else
            {
                DataBag resultBag = documentRelationship.getBag();
                DataTable resultTable = new DataTable(resultBag);
                for (Node node : aNodes)
                {
                    nodeCount++;
                    docBag = new DataBag(resultBag);
                    mConverter.convertNodeToBag(node, docBag);
                    resultTable.addRow(docBag);
                    if ((aLimit > 0) && (nodeCount >= aLimit))
                        break;
                }
                Document responseDocument = new Document(Neo4j.RESPONSE_DOCUMENT, resultTable);
                documentRelationship.add(responseDocument);
            }
        }

        resetHeader();
        populateHeader(aStopWatch, Neo4j.STATUS_CODE_SUCCESS, Neo4j.STATUS_MESSAGE_SUCCESS);
        populateHeader(aResolveTo, aDirection, aLimit);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Extracts the query response message from Neo4j nodes into a normalized
     * NS Document representation.
     *
     * @param aNodes Iterable collection of nodes.
     * @param aResolveTo Identifies how the response should be populated.
     * @param aDirection Direction relationships are recognized from.
     * @param aLimit Node limit.
     * @param aStopWatch Stop watch instance for header population.
     *
     * @return NS Document instance.
     */
    public Document extract(Iterable<Node> aNodes, String aResolveTo,
                            String aDirection, int aLimit,
                            StopWatch aStopWatch)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "extract");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        populateDocument(aNodes, aResolveTo, aDirection, aLimit, aStopWatch);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return mDocument;
    }

    private void populateDocument(ResourceIterable<org.neo4j.graphdb.Relationship> aRelationships,
                                  StopWatch aStopWatch)
    {
        DataBag relBag;
        Logger appLogger = mAppMgr.getLogger(this, "populateDocument");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Relationship documentRelationship = mDocument.getFirstRelationship(Neo4j.RESPONSE_DOCUMENT);
        if (documentRelationship != null)
        {
            documentRelationship.getDocuments().clear();
            DataBag resultBag = documentRelationship.getBag();
            DataTable resultTable = new DataTable(resultBag);
            for (org.neo4j.graphdb.Relationship relationship : aRelationships)
            {
                relBag = new DataBag(resultBag);
                mConverter.convertRelationshipToBag(relationship, relBag);
                resultTable.addRow(relBag);
            }
            Document responseDocument = new Document(Neo4j.RESPONSE_DOCUMENT, resultTable);
            documentRelationship.add(responseDocument);
        }

        populateHeader(aStopWatch, Neo4j.STATUS_CODE_SUCCESS, Neo4j.STATUS_MESSAGE_SUCCESS);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Extracts the query response message from Neo4j nodes into a normalized
     * NS Document representation.
     *
     * @param aRelationships Iterable collection of relationships.
     * @param aStopWatch Stop watch instance for header population.
     *
     * @return NS Document instance.
     */
    public Document extract(ResourceIterable<org.neo4j.graphdb.Relationship> aRelationships,
                            StopWatch aStopWatch)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "extract");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        populateDocument(aRelationships, aStopWatch);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return mDocument;
    }
}
