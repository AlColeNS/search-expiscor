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
import com.nridge.core.base.ds.DSCriterionEntry;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.FieldRow;
import com.nridge.core.base.field.data.*;
import com.nridge.core.base.std.StrUtl;
import com.nridge.core.io.log.DSCriteriaLogger;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * The SolrParentChild provides a collection of methods that can expand
 * a Solr response {@link Document} with additional parents and children
 * results.  It is a helper class for the {@link SolrDS} class.
 *
 * NOTE: If permission models need to be supported, then this logic will
 * need to be moved to an application service.
 *
 * @author Al Cole
 * @since 1.0
 */
@SuppressWarnings("unchecked")
public class SolrParentChild
{
    private final String FIELD_NSD_ID_NAME = "nsd_id";
    private final String FIELD_NSD_NAME_NAME = "nsd_name";
    private final String FIELD_NSD_IS_PARENT_NAME = "nsd_is_parent";
    private final String FIELD_NSD_PARENT_ID_NAME = "nsd_parent_id";
    private final String FIELD_NSD_IS_EXPANDED_NAME = "nsd_is_expanded";

    private final String PROPERTY_HASH_MAP_NAME = "IdRowHashMap";

    private final String FIELD_FETCH_LIMIT_DEFAULT = "5";
    private final String FIELD_FETCH_OFFSET_DEFAULT = "0";

    private SolrDS mSolrDS;
    private final AppMgr mAppMgr;

    public SolrParentChild(AppMgr anAppMgr, SolrDS aSolrDS)
    {
        mAppMgr = anAppMgr;
        mSolrDS = aSolrDS;
    }

    private void logExpansionDetails(DSCriteria aCriteria, DataTable aTable,
                                     int aCountBefore, int aCountAfter)
    {
        int versionNumber;
        String rowStr, docId, parentId, docName, isParent, versionString, isExpanded;
        Logger appLogger = mAppMgr.getLogger(this, "logExpansionDetails");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        appLogger.debug(String.format("+++ Start Parent/Child Listing for '%s'", aCriteria.getName()));
        appLogger.debug(String.format("Row count before expansion: %d", aCountBefore));
        DSCriteriaLogger dsCriteriaLogger = new DSCriteriaLogger(appLogger);
        dsCriteriaLogger.writeSimple(aCriteria);
        int rowCount = aTable.rowCount();
        appLogger.debug(String.format("%s: %d documents found in Solr", aTable.getName(), aTable.rowCount()));
        for (int row = 0; row < rowCount; row++)
        {
            docId = aTable.getValueByName(row, "nsd_id");
            docName = aTable.getValueByName(row, "nsd_name");
            parentId = aTable.getValueByName(row, "nsd_parent_id");
            isExpanded = aTable.getValueByName(row, "nsd_is_expanded");
            if (StrUtl.stringToBoolean(isExpanded))
                isExpanded = "E";                   // query expansion
            else
                isExpanded = "H";                   // query hit
            if (StringUtils.isEmpty(parentId))
                isParent = "P";
            else
            {
                isParent = " C";
                parentId = String.format(" (%s)", parentId);
            }
            versionString = aTable.getValueByName(row, "nsd_version");
            if (StringUtils.isEmpty(versionString))
                versionNumber = 0;
            else
                versionNumber = Integer.parseInt(versionString);

            rowStr = String.format("%02d: %s-[%s]%s: %s '%s' - v%d", row, isParent, docId, parentId,
                                    isExpanded, docName, versionNumber);
            appLogger.debug(rowStr);
        }
        appLogger.debug(String.format("Row count after expansion: %d", aCountAfter));
        appLogger.debug(String.format("=== Finish Parent/Child Listing for '%s'", aCriteria.getName()));

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /* Format: expandOption, expandOption(limit), expandOption(offset,limit) */
    private String extractOffset(String aValue)
    {
        String startingOffset = FIELD_FETCH_OFFSET_DEFAULT;
        if (StringUtils.isNotEmpty(aValue))
        {
            int offset1 = aValue.lastIndexOf("(");
            int offset2 = aValue.lastIndexOf(")");
            if ((offset1 != -1) && (offset2 != -1))
            {
                String countString = aValue.substring(offset1+1, offset2);
                if (StringUtils.contains(countString, StrUtl.CHAR_COMMA))
                {
                    ArrayList<String> offsetLimitList = StrUtl.expandToList(countString, StrUtl.CHAR_COMMA);
                    if (offsetLimitList.size() > 0)
                    {
                        String offsetValue = offsetLimitList.get(0);
                        if (StringUtils.isNumeric(offsetValue))
                            startingOffset = offsetValue;
                    }
                }
            }
        }

        return startingOffset;
    }

    /* Format: expandOption, expandOption(limit), expandOption(offset,limit) */
    private String extractLimit(String aValue)
    {
        String fetchLimit = FIELD_FETCH_LIMIT_DEFAULT;
        if (StringUtils.isNotEmpty(aValue))
        {
            int offset1 = aValue.lastIndexOf("(");
            int offset2 = aValue.lastIndexOf(")");
            if ((offset1 != -1) && (offset2 != -1))
            {
                String countString = aValue.substring(offset1 + 1, offset2);
                if (StringUtils.contains(countString, StrUtl.CHAR_COMMA))
                {
                    ArrayList<String> offsetLimitList = StrUtl.expandToList(countString, StrUtl.CHAR_COMMA);
                    if (offsetLimitList.size() > 1)
                    {
                        String limitValue = offsetLimitList.get(1);
                        if (StringUtils.isNumeric(limitValue))
                            fetchLimit = limitValue;
                    }
                }
                else
                {
                    if (StringUtils.isNumeric(countString))
                        fetchLimit = countString;
                }
            }
        }

        return fetchLimit;
    }

    private DataField pcExpansionField(String aValue)
    {
        String fieldName;

        if (StringUtils.startsWithIgnoreCase(aValue, Solr.PC_EXPANSION_BOTH))
            fieldName = Solr.PC_EXPANSION_BOTH;
        else if (StringUtils.startsWithIgnoreCase(aValue, Solr.PC_EXPANSION_CHILD))
            fieldName = Solr.PC_EXPANSION_CHILD;
        else if (StringUtils.equalsIgnoreCase(aValue, Solr.PC_EXPANSION_PARENT))
            fieldName = Solr.PC_EXPANSION_PARENT;
        else
            fieldName = Solr.PC_EXPANSION_NONE;

        DataField expansionField = new DataIntegerField(fieldName, Field.nameToTitle(fieldName));
        expansionField.setMultiValueFlag(true);
        expansionField.addValue(extractOffset(aValue));
        expansionField.addValue(extractLimit(aValue));

        return expansionField;
    }

    private DataBag findBagInTableById(DataTable aTable, String aFieldName, String anId)
    {
        if (aTable != null)
        {
            FieldRow fieldRow = Field.firstFieldRow(aTable.findValue(aFieldName, Field.Operator.EQUAL, anId));
            if (fieldRow != null)
                return aTable.getRowAsBag(fieldRow);
        }

        return null;
    }

    private boolean isRowInTableById(DataTable aTable, String aFieldName, String anId)
    {
        if (StringUtils.isNotEmpty(anId))
        {
            FieldRow fieldRow = Field.firstFieldRow(aTable.findValue(aFieldName, Field.Operator.EQUAL, anId));

            return (fieldRow != null);
        }
        else
            return false;
    }

    private HashMap<String,DataBag> createTableHashMap(DataTable aTable)
    {
        HashMap<String,DataBag> hashMapRows = new HashMap<String,DataBag>();
        aTable.addProperty(PROPERTY_HASH_MAP_NAME, hashMapRows);

        return hashMapRows;
    }

    private HashMap<String,DataBag> getTableHashMap(DataTable aTable)
    {
        return (HashMap<String,DataBag>) aTable.getProperty(PROPERTY_HASH_MAP_NAME);
    }

    private boolean addRowUniqueToTable(DataTable aTable, DataBag aRowBag)
    {
        boolean isAdded;
        Logger appLogger = mAppMgr.getLogger(this, "addRowUniqueToTable");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        HashMap<String,DataBag> hashMapRows = getTableHashMap(aTable);
        String docId = aRowBag.getValueAsString(FIELD_NSD_ID_NAME);
        String parentId = aRowBag.getValueAsString(FIELD_NSD_PARENT_ID_NAME);
        String keyId = String.format("%s:%s", docId, parentId);
        DataBag rowBag = hashMapRows.get(keyId);
        if (rowBag == null)
        {
            aTable.addRow(aRowBag);
            hashMapRows.put(keyId, aRowBag);
            isAdded = true;
        }
        else
            isAdded = false;

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return isAdded;
    }

    /* This method will ensure that all possible parents are in returned table. */

    private DataTable expandParents(DataTable aResponseTable)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "expandParents");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataTable parentTable = null;

// First, we need to extract all unique parent ids from the response table.

        int rowCount = aResponseTable.rowCount();
        if (rowCount > 0)
        {
            String parentId;
            DataField parentIdsField;

            DataField fetchParentIds = new DataTextField("parent_ids", "Parent Ids");
            fetchParentIds.setMultiValueFlag(true);

            for (int row = 0; row < rowCount; row++)
            {
                parentIdsField = aResponseTable.getFieldByRowName(row, FIELD_NSD_PARENT_ID_NAME);
                if ((parentIdsField != null) && (parentIdsField.valueCount() > 0))
                {
                    parentId = parentIdsField.getFirstValue();
                    if (! isRowInTableById(aResponseTable, FIELD_NSD_ID_NAME, parentId))
                        fetchParentIds.addValueUnique(parentId);
                }
            }

// Fetch our parent table of documents.

            if (fetchParentIds.valueCount() > 0)
            {
                DSCriteria parentCriteria = new DSCriteria("Parent Criteria");
                parentCriteria.add(Solr.FIELD_QUERY_NAME, Field.Operator.EQUAL, Solr.QUERY_ALL_DOCUMENTS);
                String[] fetchValues = StrUtl.convertToMulti(fetchParentIds.getValues());
                parentCriteria.add(FIELD_NSD_ID_NAME, Field.Operator.IN, fetchValues);

                Document solrDocument = mSolrDS.fetch(parentCriteria);
                if (Solr.isResponsePopulated(solrDocument))
                    parentTable = Solr.getResponse(solrDocument);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return parentTable;
    }

    private DSCriteria createChildCriteria(DataTable aResponseTable, String aPrimaryKeyFieldName,
                                           String aParentId)
    {
        String primaryKeyId;
        DSCriteria childCriteria = new DSCriteria("Child Criteria");
        childCriteria.add(Solr.FIELD_QUERY_NAME, Field.Operator.EQUAL, Solr.QUERY_ALL_DOCUMENTS);
        childCriteria.add(FIELD_NSD_PARENT_ID_NAME, Field.Operator.EQUAL, aParentId);

        DataField excludeChildIds = new DataTextField("child_ids", "Child Ids");
        excludeChildIds.setMultiValueFlag(true);
        ArrayList<FieldRow> childRows = aResponseTable.findValue(FIELD_NSD_PARENT_ID_NAME, Field.Operator.EQUAL, aParentId);
        for (FieldRow childRow : childRows)
        {
            primaryKeyId = aResponseTable.getValueByName(childRow, aPrimaryKeyFieldName);
            excludeChildIds.addValueUnique(primaryKeyId);
        }
        if (excludeChildIds.valueCount() > 0)
        {
            String[] excludeValues = StrUtl.convertToMulti(excludeChildIds.getValues());
            childCriteria.add(aPrimaryKeyFieldName, Field.Operator.NOT_IN, excludeValues);
        }

        return childCriteria;
    }

    /* This method will ensure that all possible children are in returned table. */

    private DataTable expandChildren(DataTable aResponseTable, DataTable aParentTable,
                                     DataField anExpansionField)
        throws DSException
    {
        String parentId;
        DataBag childBag;
        DataField isParentField;
        DSCriteria childCriteria;
        DataTable childResponseTable;
        int offset, limit, childRowCount;
        Logger appLogger = mAppMgr.getLogger(this, "expandChildren");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataTable childTable = null;

        int resultRowCount = aResponseTable.rowCount();
        if ((anExpansionField != null) && (anExpansionField.valueCount() == 2))
        {
            offset = Integer.parseInt(anExpansionField.getValue(0));
            limit = Integer.parseInt(anExpansionField.getValue(1));
        }
        else
        {
            offset = Solr.QUERY_OFFSET_DEFAULT;
            limit = Solr.QUERY_PAGESIZE_DEFAULT;
        }

// First, we will review the parents of the response table.

        for (int resultRow = 0; resultRow < resultRowCount; resultRow++)
        {
            isParentField = aResponseTable.getFieldByRowName(resultRow, FIELD_NSD_IS_PARENT_NAME);
            if ((isParentField != null) && (isParentField.isValueTrue()))
            {
                parentId = aResponseTable.getValueByName(resultRow, FIELD_NSD_ID_NAME);
                if (StringUtils.isNotEmpty(parentId))
                {
                    childCriteria = createChildCriteria(aResponseTable, FIELD_NSD_ID_NAME, parentId);
                    Document solrDocument = mSolrDS.fetch(childCriteria, offset, limit);
                    if (Solr.isResponsePopulated(solrDocument))
                    {
                        childResponseTable = Solr.getResponse(solrDocument);
                        if (childTable == null)
                        {
                            childTable = new DataTable(childResponseTable.getColumnBag());
                            createTableHashMap(childTable);
                        }
                        childRowCount = childResponseTable.rowCount();
                        for (int childRow = 0; childRow < childRowCount; childRow++)
                        {
                            childBag = childResponseTable.getRowAsBag(childRow);
                            addRowUniqueToTable(childTable, childBag);
                        }
                    }
                }
            }
        }

// Next, we will review the parents of the parent table.

        if (aParentTable != null)
        {
            int parentCount = aParentTable.rowCount();
            for (int parentRow = 0; parentRow < parentCount; parentRow++)
            {
                isParentField = aParentTable.getFieldByRowName(parentRow, FIELD_NSD_IS_PARENT_NAME);
                if ((isParentField != null) && (isParentField.isValueTrue()))
                {
                    parentId = aParentTable.getValueByName(parentRow, FIELD_NSD_ID_NAME);
                    if (StringUtils.isNotEmpty(parentId))
                    {
                        childCriteria = createChildCriteria(aParentTable, FIELD_NSD_ID_NAME, parentId);
                        Document solrDocument = mSolrDS.fetch(childCriteria, offset, limit);
                        if (Solr.isResponsePopulated(solrDocument))
                        {
                            childResponseTable = Solr.getResponse(solrDocument);
                            if (childTable == null)
                            {
                                childTable = new DataTable(childResponseTable.getColumnBag());
                                createTableHashMap(childTable);
                            }
                            childRowCount = childResponseTable.rowCount();
                            for (int childRow = 0; childRow < childRowCount; childRow++)
                            {
                                childBag = childResponseTable.getRowAsBag(childRow);
                                addRowUniqueToTable(childTable, childBag);
                            }
                        }
                    }
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return childTable;
    }

    private DataTable matchChildrenToParents(DataTable aResponseTable)
    {
        String nsdId, parentId;
        DataBag parentBag, childBag;
        Logger appLogger = mAppMgr.getLogger(this, "matchChildrenToParents");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataTable matchTable = new DataTable(aResponseTable.getColumnBag());
        createTableHashMap(matchTable);
        int rowCount = aResponseTable.rowCount();
        for (int parentRow = 0; parentRow < rowCount; parentRow++)
        {
            parentBag = aResponseTable.getRowAsBag(parentRow);
            parentId = parentBag.getValueAsString(FIELD_NSD_PARENT_ID_NAME);
            if (StringUtils.isEmpty(parentId))
            {
                addRowUniqueToTable(matchTable, parentBag);
                nsdId = parentBag.getValueAsString(FIELD_NSD_ID_NAME);
                for (int childRow = 0; childRow < rowCount; childRow++)
                {
                    childBag = aResponseTable.getRowAsBag(childRow);
                    parentId = childBag.getValueAsString(FIELD_NSD_PARENT_ID_NAME);
                    if (StringUtils.equals(nsdId, parentId))
                        addRowUniqueToTable(matchTable, childBag);
                }
            }
        }

        for (int childRow = 0; childRow < rowCount; childRow++)
        {
            childBag = aResponseTable.getRowAsBag(childRow);
            parentId = childBag.getValueAsString(FIELD_NSD_PARENT_ID_NAME);
            if (StringUtils.isNotEmpty(parentId))
                addRowUniqueToTable(matchTable, childBag);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return matchTable;
    }

    private DataBag getParentRowById(String aPrimaryKeyFieldName, String anId,
                                     DataTable aResponseTable, DataTable aParentTable)
    {
        Logger appLogger = mAppMgr.getLogger(this, "getParentRowById");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataBag parentBag = findBagInTableById(aResponseTable, aPrimaryKeyFieldName, anId);
        if (parentBag == null)
        {
            parentBag = findBagInTableById(aParentTable, aPrimaryKeyFieldName, anId);
            if (parentBag != null)
                parentBag.setValueByName(FIELD_NSD_IS_EXPANDED_NAME, true);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return parentBag;
    }

    private ArrayList<DataBag> getChildRowsById(String anId, DataTable aResponseTable,
                                                DataTable aChildTable)
    {
        DataBag childBag;
        Logger appLogger = mAppMgr.getLogger(this, "getChildRowsById");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        ArrayList<DataBag> childBagList = new ArrayList<DataBag>();
        ArrayList<FieldRow> childRows = aResponseTable.findValue(FIELD_NSD_PARENT_ID_NAME, Field.Operator.EQUAL, anId);
        for (FieldRow childRow : childRows)
        {
            childBag = aResponseTable.getRowAsBag(childRow);
            if (childBag != null)
                childBagList.add(childBag);
        }
        childRows = aChildTable.findValue(FIELD_NSD_PARENT_ID_NAME, Field.Operator.EQUAL, anId);
        for (FieldRow childRow : childRows)
        {
            childBag = aResponseTable.getRowAsBag(childRow);
            if (childBag != null)
            {
                childBag.setValueByName(FIELD_NSD_IS_EXPANDED_NAME, true);
                childBagList.add(childBag);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return childBagList;
    }

    private boolean isParentNeeded(DataTable aParentTable, DataBag aRowBag, DataField anExpansionField)
    {
        if ((aParentTable != null) || (! aRowBag.isValueTrue(FIELD_NSD_IS_PARENT_NAME)))
        {
            String parentId = aRowBag.getValueAsString(FIELD_NSD_PARENT_ID_NAME);
            String expansionName = anExpansionField.getName();
            if ((StringUtils.isNotEmpty(parentId)) && (! expansionName.equals(Solr.PC_EXPANSION_NONE)))
                return true;
        }

        return false;
    }

    private boolean isChildrenNeeded(DataTable aChildTable, DataBag aRowBag, DataField anExpansionField)
    {
        if ((aChildTable != null) && (aRowBag.isValueTrue(FIELD_NSD_IS_PARENT_NAME)))
        {
            String parentId = aRowBag.getValueAsString(FIELD_NSD_PARENT_ID_NAME);
            String expansionName = anExpansionField.getName();
            if ((StringUtils.isEmpty(parentId)) &&
                ((expansionName.equals(Solr.PC_EXPANSION_CHILD)) || (expansionName.equals(Solr.PC_EXPANSION_BOTH))))
                return true;
        }

        return false;
    }

    /* The key to this algorithm is the detection of existing rows in the merged table. */

    private DataTable merge(DataTable aResponseTable, DataTable aParentTable,
                            DataTable aChildTable, DataField anExpansionField)
        throws DSException
    {
        int rowCount;
        DataTable mergeTable;
        DataBag responseBag, parentBag;
        Logger appLogger = mAppMgr.getLogger(this, "merge");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

// Match un-expanded children with their parents before we continue.

        DataTable responseTable = matchChildrenToParents(aResponseTable);

        rowCount = responseTable.rowCount();
        if ((aParentTable == null) && (aChildTable == null))
            mergeTable = responseTable;
        else
        {
            mergeTable = new DataTable(responseTable.getColumnBag());
            createTableHashMap(mergeTable);

/* Every row in the response table must be re-added to the merge table. Parent
and child rows that are included as part of the expansion process. */

            for (int row = 0; row < rowCount; row++)
            {
                responseBag = responseTable.getRowAsBag(row);

                if (isParentNeeded(aParentTable, responseBag, anExpansionField))
                {
                    String parentId = responseBag.getValueAsString(FIELD_NSD_PARENT_ID_NAME);
                    parentBag = getParentRowById(FIELD_NSD_ID_NAME, parentId, responseTable, aParentTable);
                    if (parentBag != null)
                    {
                        if (addRowUniqueToTable(mergeTable, parentBag))
                        {
                            appLogger.debug(String.format("Parent expanded (%s): [%s] %s", parentId,
                                                          parentBag.getValueAsString(FIELD_NSD_ID_NAME),
                                                          parentBag.getValueAsString(FIELD_NSD_NAME_NAME)));
                            if (isChildrenNeeded(aChildTable, parentBag, anExpansionField))
                            {
                                String docId = parentBag.getValueAsString(FIELD_NSD_ID_NAME);
                                ArrayList<DataBag> childBagList = getChildRowsById(docId, responseTable, aChildTable);
                                for (DataBag childBag : childBagList)
                                {
                                    if (addRowUniqueToTable(mergeTable, childBag))
                                    {
                                        appLogger.debug(String.format("Child expanded (%s): [%s] %s", docId,
                                                                      childBag.getValueAsString(FIELD_NSD_ID_NAME),
                                                                      childBag.getValueAsString(FIELD_NSD_NAME_NAME)));
                                    }
                                }
                            }
                        }
                    }
                }
                if (isChildrenNeeded(aChildTable, responseBag, anExpansionField))
                {
                    addRowUniqueToTable(mergeTable, responseBag);
                    String docId = responseBag.getValueAsString(FIELD_NSD_ID_NAME);
                    ArrayList<DataBag> childBagList = getChildRowsById(docId, responseTable, aChildTable);
                    for (DataBag childBag : childBagList)
                    {
                        if (addRowUniqueToTable(mergeTable, childBag))
                        {
                            appLogger.debug(String.format("Child expanded (%s): [%s] %s", docId,
                                                          childBag.getValueAsString(FIELD_NSD_ID_NAME),
                                                          childBag.getValueAsString(FIELD_NSD_NAME_NAME)));
                        }
                    }
                }
                else
                    addRowUniqueToTable(mergeTable, responseBag);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return mergeTable;
    }

    public void expand(Document aSolrDocument, DSCriteria aCriteria)
        throws DSException
    {
        int rowCount;
        DataTable responseTable;
        Logger appLogger = mAppMgr.getLogger(this, "expand");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataField expansionField = null;
        if ((aCriteria != null) && (aCriteria.count() > 0))
        {
            String fieldName;

            for (DSCriterionEntry ce : aCriteria.getCriterionEntries())
            {
                fieldName = ce.getName();
                if (StringUtils.equals(fieldName, Solr.FIELD_PC_EXPAND_NAME))
                {
                    expansionField = pcExpansionField(ce.getValue());
                    break;
                }
            }
        }

        if (Solr.isResponsePopulated(aSolrDocument))
        {
            responseTable = Solr.getResponse(aSolrDocument);
            rowCount = responseTable.rowCount();
        }
        else
        {
            rowCount = 0;
            responseTable = null;
        }

        if ((rowCount > 0) && (expansionField != null) &&
            (! expansionField.getName().equals(Solr.PC_EXPANSION_NONE)))
        {
            DataTable parentTable, childTable, mergeTable;

// Populate our parent and child tables.

            String expansionName = expansionField.getName();

// Expand our parent if it is missing.

            parentTable = expandParents(responseTable);
            if ((expansionName.equals(Solr.PC_EXPANSION_CHILD)) || (expansionName.equals(Solr.PC_EXPANSION_BOTH)))
                childTable = expandChildren(responseTable, parentTable, expansionField);
            else
                childTable = null;

// Merge the original response, parent and child tables into one.

            mergeTable = merge(responseTable, parentTable, childTable, expansionField);

// Update the response table in the Solr document.

            Relationship documentRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_DOCUMENT);
            if (documentRelationship != null)
            {
                if (documentRelationship.count() > 0)
                {
                    Document resultDocument = documentRelationship.getDocuments().get(0);
                    resultDocument.setTable(mergeTable);
                }
            }

            logExpansionDetails(aCriteria, mergeTable, rowCount, mergeTable.rowCount());
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
