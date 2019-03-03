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
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;

import java.util.ArrayList;

/**
 * The Neo4jConvert provides a collection of methods that
 * can convert objects from a Neo4j graph database to their
 * NS {@link Document} counterparts. It is a helper class
 * for the {@link Neo4jDS} class.
 *
 * @author Al Cole
 * @since 1.0
 */
public class Neo4jConvert
{
    private boolean mIsUpdate;
    private DataBag mSchemaBag;
    private Label mGraphDBLabel;
    private final AppMgr mAppMgr;
    private GraphDatabaseService mGraphDB;
    private String mSchemaPKName = StringUtils.EMPTY;
    private String mSourcePKName = StringUtils.EMPTY;

    /**
     * Constructor that accepts an application manager and graph
     * database instance, but does not specify an existing
     * document with a schema defined.
     *
     * @param anAppMgr Application manager instance.
     * @param aGraphDB Graph database instance.
     */
    public Neo4jConvert(final AppMgr anAppMgr, GraphDatabaseService aGraphDB)
    {
        mAppMgr = anAppMgr;
        mGraphDB = aGraphDB;
    }

    /**
     * Constructor that accepts an application manager and graph
     * database instance along with a schema data bag instance.
     *
     * @param anAppMgr Application manager instance.
     * @param aGraphDB Graph database instance.
     * @param aSchemaBag Data bag instance.
     */
    public Neo4jConvert(final AppMgr anAppMgr, GraphDatabaseService aGraphDB,
                        final DataBag aSchemaBag)
    {
        mAppMgr = anAppMgr;
        mGraphDB = aGraphDB;
        setSchema(aSchemaBag);
    }

    /**
     * Assigns the data bag instance as the schema for the
     * document data source.
     *
     * @param aBag Data bag instance.
     */
    public void setSchema(DataBag aBag)
    {
        mSchemaBag = aBag;
        DataField pkField = mSchemaBag.getPrimaryKeyField();
        if (pkField != null)
        {
            mSchemaPKName = pkField.getName();
            String featureValue = pkField.getFeature("sourcePrivateKey");
            if (StringUtils.isNotEmpty(featureValue))
                mSourcePKName = featureValue;
        }
        String labelName = aBag.getFeature("labelName");
        if (StringUtils.isNotEmpty(labelName))
            mGraphDBLabel = Label.label(labelName);
    }

    /**
     * Assigns a flag identifying if the current operation is
     * an update.  If yes, then the graph nodes are cleared of
     * their information (properties, labels and relationships)
     * before data is assigned.
     *
     * @param anIsUpdate Boolean flag.
     */
    public void setUpdateFlag(boolean anIsUpdate)
    {
        mIsUpdate = anIsUpdate;
    }

    /**
     * Returns the update flag.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isUpdate()
    {
        return mIsUpdate;
    }

    /**
     * Returns the primary key source field name from the schema.
     *
     * @param aBag Data bag instance.
     *
     * @return Primary source key id string.
     */
    public String getSourcePrimaryKeyId(DataBag aBag)
    {
        if (StringUtils.isNotEmpty(mSourcePKName))
        {
            DataField sourcePKField = aBag.getFieldByName(mSourcePKName);
            if (sourcePKField != null)
                return sourcePKField.getValue();
        }

        return aBag.getValueAsString(mSchemaPKName);
    }

    private boolean isFieldValidForAdd(DataField aField)
    {
        if ((aField != null) && (mSchemaBag != null))
        {
            DataField dataField =  mSchemaBag.getFieldByName(aField.getName());
            if (dataField == null)
                return false;
        }
        return true;
    }

    /**
     * Performs a fast lookup of the node (by its unique id) and
     * returns an instance of it if it exists.  Otherwise, it
     * returns <i>null</i>.
     *
     * @param anId Unique document id.
     *
     * @return Node instance if found or <i>null</i> otherwise
     */
    public Node findNodeById(String anId)
    {
        String docId;
        Logger appLogger = mAppMgr.getLogger(this, "findNodeById");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Node graphNode = null;
        if (StringUtils.isNotEmpty(anId))
        {
            if (mGraphDBLabel == null)
            {
                ResourceIterable<Node> nodeIterable = mGraphDB.getAllNodes();
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
     * Assigns the property information from the data bag instance to
     * the graph node instance.
     *
     * @param aNode Graph database node instance.
     * @param aBag Data bag instance.
     */
    private void assignProperties(Node aNode, DataBag aBag)
    {
        String fieldName;
        Logger appLogger = mAppMgr.getLogger(this, "assignProperties");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

// Prior to applying the update, we will clear relationships, labels and properties.

        if (mIsUpdate)
        {
            Iterable<org.neo4j.graphdb.Relationship> relIterable = aNode.getRelationships(Direction.OUTGOING);
            for (org.neo4j.graphdb.Relationship nodeRelationship : relIterable)
                nodeRelationship.delete();
            Iterable<Label> labelsIterable = aNode.getLabels();
            for (Label label : labelsIterable)
                aNode.removeLabel(label);
            Iterable<String> pKeysIterable = aNode.getPropertyKeys();
            for (String propertyName : pKeysIterable)
                aNode.removeProperty(propertyName);
        }

        for (DataField docField : aBag.getFields())
        {
            if (isFieldValidForAdd(docField))
            {
                fieldName = docField.getName();
                if (docField.isMultiValue())
                    aNode.setProperty(fieldName, docField.collapse());
                else
                    aNode.setProperty(fieldName, docField.getValue());
            }
        }

// Assign our database wide label to the node.

        if (mGraphDBLabel != null)
            aNode.addLabel(mGraphDBLabel);

// If the source primary key field name was assigned, the apply it now.

        if (StringUtils.isNotEmpty(mSourcePKName))
        {
            DataField sourcePKField = aBag.getFieldByName(mSourcePKName);
            if (sourcePKField != null)
                aNode.setProperty(mSchemaPKName, sourcePKField.getValue());
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Assigns the property information from the data bag instance to
     * the graph relationship instance.
     *
     * @param aRelationship Graph database relationship instance.
     * @param aBag Data bag instance.
     */
    private void assignProperties(org.neo4j.graphdb.Relationship aRelationship, DataBag aBag)
    {
        String fieldName;
        Logger appLogger = mAppMgr.getLogger(this, "assignProperties");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

// Updates are limited to properties.

        if (mIsUpdate)
        {
            Iterable<String> pKeysIterable = aRelationship.getPropertyKeys();
            for (String propertyName : pKeysIterable)
                aRelationship.removeProperty(propertyName);
        }

        for (DataField docField : aBag.getFields())
        {
            if (isFieldValidForAdd(docField))
            {
                fieldName = docField.getName();
                if (docField.isMultiValue())
                    aRelationship.setProperty(fieldName, docField.collapse());
                else
                    aRelationship.setProperty(fieldName, docField.getValue());
            }
        }

// If the source primary key field name was assigned, the apply it now.

        if (StringUtils.isNotEmpty(mSourcePKName))
        {
            DataField sourcePKField = aBag.getFieldByName(mSourcePKName);
            if (sourcePKField != null)
                aRelationship.setProperty(mSchemaPKName, sourcePKField.getValue());
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private Node toNode(DataBag aBag)
    {
        Logger appLogger = mAppMgr.getLogger(this, "toNode");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String docId = getSourcePrimaryKeyId(aBag);
        Node docNode = findNodeById(docId);
        if (docNode == null)
            docNode = mGraphDB.createNode();
        assignProperties(docNode, aBag);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return docNode;
    }

    /**
     * Transforms an NS document instance into a graph database node
     * instance.
     *
     * @param aDocument Document instance.
     *
     * @return Graph database node instance.
     */
    public Node toNode(Document aDocument)
    {
        Node relNode;
        String relId, relType;
        DataBag relBag, relDocBag;
        RelationshipType gdbRelType;
        org.neo4j.graphdb.Relationship gdbRelationship;
        Logger appLogger = mAppMgr.getLogger(this, "toNode");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Node docNode = toNode(aDocument.getBag());
        ArrayList<Relationship> docRelationshipList = aDocument.getRelationships();
        for (Relationship docRelationship : docRelationshipList)
        {
            relBag = docRelationship.getBag();
            for (Document document : docRelationship.getDocuments())
            {
                relDocBag = document.getBag();
                relType = docRelationship.getType();
                relId = getSourcePrimaryKeyId(relDocBag);
                relNode = findNodeById(relId);
                if (relNode == null)
                    relNode = toNode(document);
                else
                    assignProperties(relNode, relDocBag);

                gdbRelType = RelationshipType.withName(relType);
                gdbRelationship = docNode.createRelationshipTo(relNode, gdbRelType);
                assignProperties(gdbRelationship, relBag);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return docNode;
    }

    /**
     * Assigns the property information from the data bag instance to
     * the graph node instance.
     *
     * @param aNode Graph database node instance.
     * @param aBag Data bag instance.
     */
    public void convertNodeToBag(Node aNode, DataBag aBag)
    {
        String fieldValue;
        DataField dataField;
        Logger appLogger = mAppMgr.getLogger(this, "convertNodeToBag");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Iterable<String> pKeysIterable = aNode.getPropertyKeys();
        for (String fieldName : pKeysIterable)
        {
            dataField = aBag.getFieldByName(fieldName);
            if (dataField != null)
            {
                fieldValue = (String) aNode.getProperty(fieldName);
                if (dataField.isMultiValue())
                    dataField.expand(fieldValue);
                else
                    dataField.setValue(fieldValue);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private Direction directionFromString(String aDirection)
    {
        if (StringUtils.equals(aDirection, Neo4j.DIRECTION_BOTH))
            return Direction.BOTH;
        else if (StringUtils.equals(aDirection, Neo4j.DIRECTION_INBOUND))
            return Direction.INCOMING;
        else
            return Direction.OUTGOING;
    }

    /**
     * Assigns the property information from the data bag instance to
     * the graph relationship instance.
     *
     * @param aRelationship Graph database relationship instance.
     * @param aBag Data bag instance.
     */
    public void convertRelationshipToBag(org.neo4j.graphdb.Relationship aRelationship, DataBag aBag)
    {
        String fieldValue;
        DataField dataField;
        Logger appLogger = mAppMgr.getLogger(this, "convertRelationshipToBag");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Iterable<String> pKeysIterable = aRelationship.getPropertyKeys();
        for (String fieldName : pKeysIterable)
        {
            dataField = aBag.getFieldByName(fieldName);
            if (dataField != null)
            {
                fieldValue = (String) aRelationship.getProperty(fieldName);
                if (dataField.isMultiValue())
                    dataField.expand(fieldValue);
                else
                    dataField.setValue(fieldValue);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Converts the graph database node instance and its relationships into
     * an NS document based on how the graph should be resolved and the
     * direction to follow the relationships.
     *
     * @param aNode Graph database node instance.
     * @param aResolveTo Resolve to specification.
     * @param aDirection Relationship direction.
     * @return
     */
    public Document convertNodeToDocument(Node aNode, String aResolveTo, String aDirection)
    {
        DataBag gdbBag;
        Node nodeRelNode;
        Relationship gdbRelationship;
        Logger appLogger = mAppMgr.getLogger(this, "convertNodeToDocument");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String docType = (String) aNode.getProperty("nsd_doc_type");
        if (StringUtils.isEmpty(docType))
            docType = "GDB Type";

        Document gdbDocument = new Document(docType, mSchemaBag);
        gdbBag = gdbDocument.getBag();
        convertNodeToBag(aNode, gdbBag);
        if (StringUtils.equals(aResolveTo, Neo4j.RESOLVE_TO_NODE_TREE))
        {
            Iterable<org.neo4j.graphdb.Relationship> relIterable = aNode.getRelationships(directionFromString(aDirection));
            for (org.neo4j.graphdb.Relationship nodeRelationship : relIterable)
            {
                gdbRelationship = new Relationship(nodeRelationship.getType().name(), mSchemaBag);
                gdbBag = gdbRelationship.getBag();
                convertRelationshipToBag(nodeRelationship, gdbBag);
                nodeRelNode = nodeRelationship.getEndNode();
                gdbRelationship.add(convertNodeToDocument(nodeRelNode, aResolveTo, aDirection));
                gdbDocument.addRelationship(gdbRelationship);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return gdbDocument;
    }
}
