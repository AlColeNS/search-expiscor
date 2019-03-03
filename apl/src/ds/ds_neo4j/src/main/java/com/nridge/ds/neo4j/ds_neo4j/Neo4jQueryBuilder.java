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
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSCriterionEntry;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.StrUtl;
import freemarker.template.Configuration;
import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.slf4j.Logger;

import java.io.StringWriter;

/**
 * The Neo4jQueryBuilder provides a collection of methods that can generate
 * an Neo4j graph database query based on a <i>DSCriteria</i>.  It is a helper
 * class for the {@link Neo4jDS} class.
 *
 * @author Al Cole
 * @since 1.0
 */
public class Neo4jQueryBuilder
{
    private final AppMgr mAppMgr;
    private String mCfgPropertyPrefix;
    private Configuration mConfiguration;
    private GraphDatabaseService mGraphDB;

    /**
     * Constructor that accepts an application manager instance.
     *
     * @param anAppMgr Application manager instance.
     */
    public Neo4jQueryBuilder(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        setCfgPropertyPrefix(Neo4j.CFG_PROPERTY_PREFIX);
    }

    /**
     * Constructor that accepts an application manager and
     * graph database instance.
     *
     * @param anAppMgr Application manager instance.
     * @param aGraphDB Graph database instance.
     */
    public Neo4jQueryBuilder(final AppMgr anAppMgr, GraphDatabaseService aGraphDB)
    {
        mAppMgr = anAppMgr;
        mGraphDB = aGraphDB;
        setCfgPropertyPrefix(Neo4j.CFG_PROPERTY_PREFIX);
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

    /**
     * Returns <i>true</i> if the application manager configuration
     * property value evaluates to <i>true</i>.
     *
     * @param aSuffix Property name suffix.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isCfgStringTrue(String aSuffix)
    {
        String propertyValue = getCfgString(aSuffix);
        return StrUtl.stringToBoolean(propertyValue);
    }

    /**
     * Creates a collapsed Cipher query string based on the
     * <i>DSCriteria</i> instance.
     *
     * @param aCriteria DS Criteria instance.
     *
     * @return Collapsed Cipher query string.
     *
     * @throws DSException Invalid field or logical operator in criteria.
     */
    public String createAsString(DSCriteria aCriteria)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "createAsString");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        StringWriter stringWriter = new StringWriter();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return stringWriter.toString();
    }

    private Direction getDirection(String aDirection)
    {
        if (StringUtils.equals(aDirection, Neo4j.DIRECTION_INBOUND))
            return Direction.INCOMING;
        else if (StringUtils.equals(aDirection, Neo4j.DIRECTION_BOTH))
            return Direction.BOTH;
        else
            return Direction.OUTGOING;
    }

    public String getDirection(DSCriteria aCriteria)
    {
        DataField dataField;

        if ((aCriteria != null) && (aCriteria.count() > 0))
        {
            for (DSCriterionEntry ce : aCriteria.getCriterionEntries())
            {
                dataField = ce.getField();
                if (StringUtils.equals(dataField.getName(), Neo4j.FIELD_REL_DIRECTION))
                    return dataField.getValue();
            }
        }

        return Neo4j.DIRECTION_OUTBOUND;
    }

    public String getNodeId(DSCriteria aCriteria)
    {
        DataField dataField;

        if ((aCriteria != null) && (aCriteria.count() > 0))
        {
            for (DSCriterionEntry ce : aCriteria.getCriterionEntries())
            {
                dataField = ce.getField();
                if (StringUtils.equals(dataField.getName(), Neo4j.FIELD_NODE_ID))
                    return dataField.getValue();
            }
        }

        return StringUtils.EMPTY;
    }

    public String getResolveTo(DSCriteria aCriteria)
    {
        DataField dataField;

        if ((aCriteria != null) && (aCriteria.count() > 0))
        {
            for (DSCriterionEntry ce : aCriteria.getCriterionEntries())
            {
                dataField = ce.getField();
                if (StringUtils.equals(dataField.getName(), Neo4j.FIELD_RESOLVE_TO))
                    return dataField.getValue();
            }
        }

        return Neo4j.RESOLVE_TO_NODE_LIST;
    }

    public TraversalDescription create(DSCriteria aCriteria)
        throws DSException
    {
        String fieldName;
        DataField dataField;
        Logger appLogger = mAppMgr.getLogger(this, "create");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mGraphDB == null)
            throw new DSException("Cannot create graph database query - database is null.");

        TraversalDescription traversalDescription = mGraphDB.traversalDescription();
        Direction gdbDirection = getDirection(getDirection(aCriteria));
        if ((aCriteria != null) && (aCriteria.count() > 0))
        {
            for (DSCriterionEntry ce : aCriteria.getCriterionEntries())
            {
                dataField = ce.getField();
                fieldName = dataField.getName();
                if (StringUtils.equals(fieldName, Neo4j.FIELD_REL_NAME))
                {
                    if (dataField.isMultiValue())
                    {
                        for (String relName : dataField.getValues())
                            traversalDescription = traversalDescription.relationships(RelationshipType.withName(relName), gdbDirection);
                    }
                    else
                        traversalDescription = traversalDescription.relationships(RelationshipType.withName(dataField.getValue()), gdbDirection);
                }
                else if (StringUtils.equals(fieldName, Neo4j.FIELD_REL_TRAVERSAL))
                {
                    String criteriaTraversal = dataField.getValue();
                    if (StringUtils.equals(criteriaTraversal, Neo4j.TRAVERSAL_DEPTH_FIRST))
                        traversalDescription = traversalDescription.depthFirst();
                    else if (StringUtils.equals(criteriaTraversal, Neo4j.TRAVERSAL_BREADTH_FIRST))
                        traversalDescription = traversalDescription.breadthFirst();
                }
                else if (StringUtils.equals(fieldName, Neo4j.FIELD_REL_DEPTH))
                {
                    int depthValue = dataField.getValueAsInt();
                    if (depthValue > 0)
                        traversalDescription = traversalDescription.evaluator(Evaluators.toDepth(depthValue));
                }
                else if (StringUtils.equals(fieldName, Neo4j.FIELD_REL_UNIQUENESS))
                {
                    if (dataField.isMultiValue())
                    {
                        for (String uniqueName : dataField.getValues())
                            traversalDescription = traversalDescription.uniqueness(Uniqueness.valueOf(uniqueName));
                    }
                    else
                        traversalDescription = traversalDescription.uniqueness(Uniqueness.valueOf(dataField.getValue()));
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return traversalDescription;
    }
}
