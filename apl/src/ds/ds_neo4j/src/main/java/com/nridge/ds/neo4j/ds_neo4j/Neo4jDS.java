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
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.ds.ds_common.DSDocument;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * The Neo4jDS data source supports CRUD operations and advanced
 * queries using a <i>DSCriteria</i> against a Neo4j managed
 * graph database.
 *
 * @see <a href="http://neo4j.com/">Neo4j Graph Database</a>
 * @see <a href="http://neo4j.com/developer/get-started/">Neo4j Getting Started</a>
 * @see <a href="http://neo4j.com/docs/milestone/">Neo4j Developer Documentation</a>
 * @see <a href="http://neo4j.com/docs/milestone/tutorials-java-embedded-hello-world.html">Neo4j Hello World</a>
 * @see <a href="http://neo4j.com/docs/stable/indexing.html">Neo4j Lucene Indexing</a>
 * @see <a href="http://stackoverflow.com/questions/26256461/how-lucene-works-with-neo4j">Neo4j Neo4j Indexing History</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class Neo4jDS extends DSDocument
{
    private final String DS_TYPE_NAME = "Neo4j";
    private final String DS_TITLE_DEFAULT = "Neo4j Data Source";

    private Label mGraphDBLabel;
    private Neo4jConvert mConverter;
	private GraphDatabaseService mGraphDB;
    private Neo4jQueryBuilder mQueryBuilder;
    private String mSchemaPKName = StringUtils.EMPTY;

	/**
	 * Constructor accepts an application manager parameter and initializes
	 * the data source accordingly.
	 *
	 * @param anAppMgr Application manager.
	 */
	public Neo4jDS(AppMgr anAppMgr)
	{
		super(anAppMgr);
		setName(DS_TYPE_NAME);
		setTitle(DS_TITLE_DEFAULT);
		setCfgPropertyPrefix(Neo4j.CFG_PROPERTY_PREFIX);
	}

	/**
	 * Constructor accepts an application manager parameter and initializes
	 * the data source accordingly.
	 *
	 * @param anAppMgr Application manager.
	 * @param aName Name of the data source.
	 */
	public Neo4jDS(AppMgr anAppMgr, String aName)
	{
		super(anAppMgr, aName);
		setCfgPropertyPrefix(Neo4j.CFG_PROPERTY_PREFIX);
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
        DataField pkField = aBag.getPrimaryKeyField();
        if (pkField != null)
            mSchemaPKName = pkField.getName();
        String labelName = aBag.getFeature("labelName");
        if (StringUtils.isNotEmpty(labelName))
            mGraphDBLabel = DynamicLabel.label(labelName);
    }

	private void initialize()
		throws DSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "initialize");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		if (mGraphDB == null)
		{
            if (mGraphDBLabel == null)
                throw new DSException("A 'labelName' feature must be assigned to the schema.");
            mGraphDB = Neo4jGDB.getInstance(mAppMgr, getSchema());
		}
        if (mQueryBuilder == null)
            mQueryBuilder = new Neo4jQueryBuilder(mAppMgr, mGraphDB);
        if (mConverter == null)
            mConverter = new Neo4jConvert(mAppMgr, mGraphDB, getSchema());

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

    private Node findNodeById(String anId)
    {
        String docId;
        Logger appLogger = mAppMgr.getLogger(this, "findNodeById");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Node graphNode = null;
        if (StringUtils.isNotEmpty(anId))
        {
            if (mGraphDBLabel == null)
            {
                GlobalGraphOperations gdbOperations = GlobalGraphOperations.at(mGraphDB);
                ResourceIterable<Node> nodeIterable = gdbOperations.getAllNodes();
                for (Node iNode : nodeIterable)
                {
                    docId = (String) iNode.getProperty(mSchemaPKName);
                    if (StringUtils.equals(docId, anId))
                    {
                        graphNode = iNode;
                        break;
                    }
                }
            }
            else
            {
                try (ResourceIterator<Node> nodeIterator = mGraphDB.findNodes(mGraphDBLabel, mSchemaPKName, anId))
                {
                    if (nodeIterator.hasNext())
                        graphNode = nodeIterator.next();
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return graphNode;
    }

	/**
	 * Calculates a count (using a wildcard criteria) of all the
	 * rows stored in the content source and returns that value.
	 *
	 * @return Count of all rows in the content source.
	 * @throws com.nridge.core.base.ds.DSException Data source related exception.
	 */
	@Override
	public int count()
		throws DSException
	{
        Logger appLogger = mAppMgr.getLogger(this, "count");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        int nodeCount = 0;
        try (Transaction gdbTransaction = mGraphDB.beginTx())
        {
            GlobalGraphOperations gdbOperations = GlobalGraphOperations.at(mGraphDB);
            ResourceIterable<Node> nodeIterable = gdbOperations.getAllNodes();
            for (Node iNode : nodeIterable)
                nodeCount++;
            gdbTransaction.success();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return nodeCount;
	}

    private Iterable<Node> fetchNodesByCriteria(DSCriteria aDSCriteria)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "fetchNodesByCriteria");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        String nodeId = mQueryBuilder.getNodeId(aDSCriteria);
        if (StringUtils.isEmpty(nodeId))
            throw new DSException("A graph database criteria must have a starting node identified.");

        Node startingNode = findNodeById(nodeId);
        if (startingNode == null)
            throw new DSException(String.format("%s: Unable to locate starting node in graph database.", nodeId));

        TraversalDescription traversalDescription = mQueryBuilder.create(aDSCriteria);
        String resolveTo = mQueryBuilder.getResolveTo(aDSCriteria);
        if (StringUtils.equals(resolveTo, Neo4j.RESOLVE_TO_RELATIONSHIP_LIST))
            throw new DSException("Query resolution for relationship list not supported yet.");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return traversalDescription.traverse(startingNode).nodes();
    }

    private ResourceIterable<org.neo4j.graphdb.Relationship> fetchRelationshipsByCriteria(DSCriteria aDSCriteria)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "fetchRelationshipsByCriteria");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        String nodeId = mQueryBuilder.getNodeId(aDSCriteria);
        if (StringUtils.isEmpty(nodeId))
            throw new DSException("A graph database criteria must have a starting node identified.");

        Node startingNode = findNodeById(nodeId);
        TraversalDescription traversalDescription = mQueryBuilder.create(aDSCriteria);
        String resolveTo = mQueryBuilder.getResolveTo(aDSCriteria);
        if (! StringUtils.equals(resolveTo, Neo4j.RESOLVE_TO_RELATIONSHIP_LIST))
            throw new DSException("Query resolution must be relationship list.");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return traversalDescription.traverse(startingNode).relationships();
    }

	/**
	 * Returns a count of rows that match the <i>DSCriteria</i> specified
	 * in the parameter.
	 *
	 * @param aDSCriteria Data source criteria.
	 * @return Count of rows matching the data source criteria.
	 * @throws DSException Data source related exception.
	 */
	@Override
	public int count(DSCriteria aDSCriteria)
		throws DSException
	{
        Logger appLogger = mAppMgr.getLogger(this, "count");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        int nodeCount = 0;
        try (Transaction gdbTransaction = mGraphDB.beginTx())
        {
            Iterable<Node> gdbNodes = fetchNodesByCriteria(aDSCriteria);
            for (Node gdbNode : gdbNodes)
                nodeCount++;
            gdbTransaction.success();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

		return nodeCount;
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
	 * @throws DSException Data source related exception.
	 */
	@Override
	public Document fetch()
		throws DSException
	{
        Document responseDocument;
        Logger appLogger = mAppMgr.getLogger(this, "fetch");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try (Transaction gdbTransaction = mGraphDB.beginTx())
        {
            GlobalGraphOperations gdbOperations = GlobalGraphOperations.at(mGraphDB);
            ResourceIterable<Node> gdbNodes = gdbOperations.getAllNodes();

            Neo4jResponseBuilder responseBuilder = new Neo4jResponseBuilder(mAppMgr, mConverter, getSchema());
            responseDocument = responseBuilder.extract(gdbNodes, Neo4j.RESOLVE_TO_NODE_LIST,
                                                       Neo4j.DIRECTION_OUTBOUND, Neo4j.LIMIT_UNLIMITED,
                                                       stopWatch);
            gdbTransaction.success();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

		return responseDocument;
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
	 * @return Document hierarchy representing all documents that
	 * match the criteria in the content source.
	 * @throws DSException Data source related exception.
	 */
	@Override
	public Document fetch(DSCriteria aDSCriteria)
		throws DSException
	{
        Document responseDocument;
        Logger appLogger = mAppMgr.getLogger(this, "fetch");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try (Transaction gdbTransaction = mGraphDB.beginTx())
        {
            Iterable<Node> gdbNodes = fetchNodesByCriteria(aDSCriteria);

            String resolveTo = mQueryBuilder.getResolveTo(aDSCriteria);
            String relationshipDirection = mQueryBuilder.getDirection(aDSCriteria);

            Neo4jResponseBuilder responseBuilder = new Neo4jResponseBuilder(mAppMgr, mConverter, getSchema());
            responseDocument = responseBuilder.extract(gdbNodes, resolveTo, relationshipDirection,
                                                       Neo4j.LIMIT_UNLIMITED, stopWatch);
            gdbTransaction.success();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return responseDocument;
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
	 * @return Document hierarchy representing all documents that
	 * match the criteria in the content source. (based on the offset
	 * and limit values).
	 * @throws DSException Data source related exception.
	 */
	@Override
	public Document fetch(DSCriteria aDSCriteria, int anOffset, int aLimit)
		throws DSException
	{
        Document responseDocument;
        Logger appLogger = mAppMgr.getLogger(this, "fetch");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try (Transaction gdbTransaction = mGraphDB.beginTx())
        {
            Iterable<Node> gdbNodes = fetchNodesByCriteria(aDSCriteria);

            String resolveTo = mQueryBuilder.getResolveTo(aDSCriteria);
            String relationshipDirection = mQueryBuilder.getDirection(aDSCriteria);

            Neo4jResponseBuilder responseBuilder = new Neo4jResponseBuilder(mAppMgr, mConverter, getSchema());
            responseDocument = responseBuilder.extract(gdbNodes, resolveTo, relationshipDirection,
                                                       aLimit, stopWatch);
            gdbTransaction.success();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return responseDocument;
	}

    /**
     * Determines if the document is valid for adding/updating in the graph
     * database.  A valid document is one where there is a primary key and
     * at least one related document.
     *
     * @param aDocument Document instance.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValid(Document aDocument)
    {
        if (aDocument != null)
        {
            DataField primaryKeyField = aDocument.getBag().getPrimaryKeyField();
            if (primaryKeyField != null)
            {
                ArrayList<Relationship> docRelationshipList = aDocument.getRelationships();
                for (Relationship docRelationship : docRelationshipList)
                {
                    if (docRelationship.count() > 0)
                        return true;
                }
            }
        }

        return false;
    }

/* Note: The initialize() method must be invoked prior to
   using this method. */

    private boolean existsInGDB(Document aDocument)
    {
        Logger appLogger = mAppMgr.getLogger(this, "existsInGDB");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        boolean nodeExists = false;
        String docId = mConverter.getSourcePrimaryKeyId(aDocument.getBag());
        try (Transaction gdbTransaction = mGraphDB.beginTx())
        {
            Node docNode = mConverter.findNodeById(docId);
            if (docNode != null)
                nodeExists = true;
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return nodeExists;
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
	 * @throws DSException Data source related exception.
	 */
	@Override
	public void add(Document aDocument)
		throws DSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "add");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		if (isValid(aDocument))
        {
            initialize();
            boolean nodeExists = existsInGDB(aDocument);
            if (nodeExists)
                mConverter.setUpdateFlag(true);
            try (Transaction gdbTransaction = mGraphDB.beginTx())
            {
                Node docNode = mConverter.toNode(aDocument);
                gdbTransaction.success();
            }
            if (nodeExists)
                mConverter.setUpdateFlag(false);
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
	 * @throws DSException Data source related exception.
	 */
	@Override
	public void add(ArrayList<Document> aDocuments)
		throws DSException
	{
        Logger appLogger = mAppMgr.getLogger(this, "add");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();
        if (aDocuments != null)
        {
            for (Document document : aDocuments)
            {
                if (isValid(document))
                {
                    boolean nodeExists = existsInGDB(document);
                    if (nodeExists)
                        mConverter.setUpdateFlag(true);
                    try (Transaction gdbTransaction = mGraphDB.beginTx())
                    {
                        Node docNode = mConverter.toNode(document);
                        gdbTransaction.success();
                    }
                    if (nodeExists)
                        mConverter.setUpdateFlag(false);
                }
            }
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
	 * @throws DSException Data source related exception.
	 */
	@Override
	public void update(Document aDocument)
		throws DSException
	{
        Logger appLogger = mAppMgr.getLogger(this, "update");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (isValid(aDocument))
        {
            initialize();
            mConverter.setUpdateFlag(true);
            try (Transaction gdbTransaction = mGraphDB.beginTx())
            {
                Node docNode = mConverter.toNode(aDocument);
                gdbTransaction.success();
            }
            mConverter.setUpdateFlag(false);
        }

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
	 * @throws DSException Data source related exception.
	 */
	@Override
	public void update(ArrayList<Document> aDocuments)
		throws DSException
	{
        Node docNode;
        Logger appLogger = mAppMgr.getLogger(this, "update");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();
        if (aDocuments != null)
        {
            for (Document document : aDocuments)
            {
                if (isValid(document))
                {
                    mConverter.setUpdateFlag(true);
                    try (Transaction gdbTransaction = mGraphDB.beginTx())
                    {
                        docNode = mConverter.toNode(document);
                        gdbTransaction.success();
                    }
                    mConverter.setUpdateFlag(false);
                }
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
     * ToDo: Future Enhancements
     *
     *  When performing a delete, you need to add logic that does
     *     the following (review Neo4jConvert for sample logic):
     *     a) If there are any inbound relationships, abort the
     *        operation.
     *     b) For each outbound relationship, get a list of nodes and
     *        examine them for inbound relationships that do not
     *        match the parent id.  If there is none, then ensure
     *        that node does not have an outbound relationship.
     *        If (b) test completes, delete this relationship node.
     *     c) Delete each outbound relationship.
     *     d) Delete the parent node.
	 *
	 * @param aDocument Document where the primary key field value is
	 *                  assigned.
	 * @throws DSException Data source related exception.
	 */
	@Override
	public void delete(Document aDocument)
		throws DSException
	{
        Logger appLogger = mAppMgr.getLogger(this, "delete");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        String nodeId = mConverter.getSourcePrimaryKeyId(aDocument.getBag());
        try (Transaction gdbTransaction = mGraphDB.beginTx())
        {
            Node gdbNode = findNodeById(nodeId);
            if (gdbNode == null)
                throw new DSException(String.format("%s: Unable to locate node for deletion.", nodeId));
            Iterable<org.neo4j.graphdb.Relationship> relIterable = gdbNode.getRelationships(Direction.OUTGOING);
            for (org.neo4j.graphdb.Relationship nodeRelationship : relIterable)
                nodeRelationship.delete();
            gdbNode.delete();
            gdbTransaction.success();
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
	 * @throws DSException Data source related exception.
	 */
	@Override
	public void delete(ArrayList<Document> aDocuments)
		throws DSException
	{
        Node gdbNode;
        String nodeId;
        Logger appLogger = mAppMgr.getLogger(this, "delete");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();
        if (aDocuments != null)
        {
            for (Document document : aDocuments)
            {
                nodeId = mConverter.getSourcePrimaryKeyId(document.getBag());
                try (Transaction gdbTransaction = mGraphDB.beginTx())
                {
                    gdbNode = findNodeById(nodeId);
                    if (gdbNode == null)
                        throw new DSException(String.format("%s: Unable to locate node for deletion.", nodeId));
                    gdbNode.delete();
                    gdbTransaction.success();
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

    /**
     * The fastest way to reset a Neo4j database is to shut it
     * down and delete all of its related files and re-open it.
     * This method will do exactly those steps.
     *
     * @throws DSException Data source related exception.
     */
    public void resetDatabase()
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "resetDatabase");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String graphDBPathName = mAppMgr.getString(mAppMgr.APP_PROPERTY_GDB_PATH);
        if (StringUtils.isNotEmpty(graphDBPathName))
        {
            File graphDBPathFile = new File(graphDBPathName);
            if (graphDBPathFile.exists())
            {
                String graphDBSchemaPathFileName = String.format("%s%cschema", graphDBPathName, File.separatorChar);
                File graphDBSchemaFile = new File(graphDBSchemaPathFileName);
                if (graphDBSchemaFile.exists())
                {
                    Neo4jGDB.shutdown();
                    shutdown();
                    try
                    {
                        FileUtils.deleteDirectory(graphDBPathFile);
                    }
                    catch (IOException e)
                    {
                        appLogger.warn("%s: %s", graphDBPathName, e.getMessage());
                    }
                    initialize();
                }
            }
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

		if (mGraphDB != null)
		{
//			mGraphDB.shutdown();  Use Neo4jGDB.shutdown() only once in the application.
			mGraphDB = null;
		}
        if (mConverter != null)
            mConverter = null;

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}
}
