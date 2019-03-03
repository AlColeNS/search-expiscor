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
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.FieldRow;
import com.nridge.core.base.field.data.*;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * The SolrResponseBuilder provides a collection of methods that can extract
 * a response hierarchy from a Solr {@link QueryResponse} and store it in a
 * {@link Document} instance.  It is a helper class for the {@link SolrDS} class.
 *
 * @author Al Cole
 * @since 1.0
 */
public class SolrResponseBuilder
{
    private DataBag mBag;
    private Document mDocument;
    private final AppMgr mAppMgr;
    private boolean mIsSchemaStatic;
    private String mCfgPropertyPrefix = StringUtils.EMPTY;

    /**
     * Constructor that accepts an application manager instance, but
     * does not specify and existing document with a schema defined.
     *
     * @param anAppMgr Application manager instance.
     */
    public SolrResponseBuilder(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        instantiate(null);
        mIsSchemaStatic = false;
        setCfgPropertyPrefix(Solr.CFG_PROPERTY_PREFIX);
    }

    /**
     * Constructor that accepts an application manager instance and
     * an existing data bag that specifies and existing schema to
     * base the field parsing on.
     *
     * @param anAppMgr Application manager instance.
     * @param aBag Data bag instance.
     */
    public SolrResponseBuilder(final AppMgr anAppMgr, final DataBag aBag)
    {
        mAppMgr = anAppMgr;
        instantiate(aBag);
        setCfgPropertyPrefix(Solr.CFG_PROPERTY_PREFIX);
        mIsSchemaStatic = mBag.count() > 0;
    }

    /**
     * Constructor that accepts an application manager instance and
     * an existing document that specifies and existing schema to
     * base the field parsing on.
     *
     * @param anAppMgr Application manager instance.
     * @param aDocument Document instance.
     */
    public SolrResponseBuilder(final AppMgr anAppMgr, final Document aDocument)
    {
        mAppMgr = anAppMgr;
        instantiate(aDocument.getBag());
        mIsSchemaStatic = aDocument.getBag().count() > 0;
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

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName);
    }

    private DataBag createHeaderBag()
    {
        DataBag headerBag = new DataBag("Header Fields");

// Assigned before query executes.

        headerBag.add(new DataTextField("query_keyword", "Query Keyword"));
        headerBag.add(new DataTextField("request_handler", "Request Handler"));
        headerBag.add(new DataLongField("offset_start", "Offset Start"));
        headerBag.add(new DataIntegerField("page_size", "Page Size"));
        headerBag.add(new DataTextField("base_url", "Base URL"));
        headerBag.add(new DataTextField("collection_name", "Collection Name"));
        headerBag.add(new DataTextField("account_name", "Account Name"));

// Assigned after query executes.

        headerBag.add(new DataBooleanField("is_highlighted", "Is Highlighted", false));
        headerBag.add(new DataIntegerField("status_code", "Status Code"));
        headerBag.add(new DataIntegerField("fetch_count", "Fetch Document Count"));
        headerBag.add(new DataLongField("total_count", "Total Document Count"));
        headerBag.add(new DataIntegerField("query_time", "Query Time"));
        headerBag.add(new DataFloatField("max_score", "Max Score"));
        DataTextField dataTextField = new DataTextField("field_list", "Field List");
        dataTextField.setMultiValueFlag(true);
        headerBag.add(dataTextField);
        headerBag.add(new DataTextField("status_message", "Status Message"));

        return headerBag;
    }

    private DataBag createGroupsBag()
    {
        DataBag groupsBag = new DataBag("Groups");

        groupsBag.add(new DataIntegerField("group_total", "Group Total"));
        groupsBag.add(new DataIntegerField("group_matches", "Group Matches"));
        DataTextField dtfGroupFieldName = new DataTextField("group_name", "Group Name");
        dtfGroupFieldName.setMultiValueFlag(true);
        groupsBag.add(dtfGroupFieldName);

        return groupsBag;
    }

    private Document createGroupField()
    {
        DataBag groupFieldBag = new DataBag("Group Field");

        groupFieldBag.add(new DataTextField("field_name", "Field Name"));
        groupFieldBag.add(new DataIntegerField("total_groups", "Total Groups"));
        groupFieldBag.add(new DataIntegerField("total_matches", "Total Matches"));

        return new Document(Solr.RESPONSE_GROUP_FIELD, groupFieldBag);
    }

    private Document createGroupCollection()
    {
        DataBag groupDocumentBag = new DataBag("Group Collection");

        groupDocumentBag.add(new DataTextField("group_name", "Group Name"));
        groupDocumentBag.add(new DataLongField("offset_start", "Offset Start"));
        groupDocumentBag.add(new DataLongField("total_count", "Total Document Count"));

        return new Document(Solr.RESPONSE_GROUP_COLLECTION, groupDocumentBag);
    }

    private DataBag createMLTBag()
    {
        DataBag mltBag = new DataBag("More Like This");

        DataTextField dataTextField = new DataTextField("mlt_name", "MLT Name");
        dataTextField.setMultiValueFlag(true);
        mltBag.add(dataTextField);
        mltBag.add(new DataIntegerField("mlt_total", "MLT Total"));

        return mltBag;
    }

    private Document createMLTCollection()
    {
        DataBag mltBag = new DataBag("MLT Collection");

        mltBag.add(new DataLongField("total_count", "Total MLT Count"));

        return new Document(Solr.RESPONSE_MLT_COLLECTION, mltBag);
    }

    private DataTable createDocumentTable()
    {
        DataBag resultBag = new DataBag(mBag);
        resultBag.setAssignedFlagAll(false);
        DataTable documentTable = new DataTable(resultBag);
        documentTable.setName("Document Table");

        return documentTable;
    }

    private DataBag createFacetFieldBag()
    {
        DataBag facetBag = new DataBag("Facet Field Bag");

        facetBag.add(new DataTextField("field_name", "Field Name"));
        facetBag.add(new DataTextField("field_title", "Field Title"));
        DataTextField dataTextField = new DataTextField("facet_name_count", "Facet Name & Count");
        dataTextField.setMultiValueFlag(true);
        facetBag.add(dataTextField);

        return facetBag;
    }

    private DataBag createFacetRangeBag()
    {
        DataBag facetBag = new DataBag("Facet Range Bag");

        facetBag.add(new DataTextField("field_name", "Field Name"));
        facetBag.add(new DataTextField("field_title", "Field Title"));
        facetBag.add(new DataTextField("field_type", "Field Type"));
        facetBag.add(new DataTextField("field_start", "Field Start"));
        facetBag.add(new DataTextField("field_finish", "Field Finish"));
        facetBag.add(new DataTextField("field_gap", "Field Gap"));
        facetBag.add(new DataIntegerField("count_after", "Count After Ranges"));
        facetBag.add(new DataIntegerField("count_before", "Count Before Ranges"));
        facetBag.add(new DataIntegerField("count_between", "Count Between Ranges"));
        DataTextField dataTextField = new DataTextField("facet_name_count", "Facet Name & Count");
        dataTextField.setMultiValueFlag(true);
        facetBag.add(dataTextField);

        return facetBag;
    }

    private DataBag createFacetQueryBag()
    {
        DataBag facetBag = new DataBag("Facet Query Bag");

        facetBag.add(new DataTextField("search_term", "Search Term"));
        facetBag.add(new DataTextField("facet_count", "Facet Count"));

        return facetBag;
    }

    private DataBag createFacetPivotBag()
    {
        DataBag facetBag = new DataBag("Facet Pivot Bag");

        facetBag.add(new DataIntegerField("id", "Id"));
        facetBag.add(new DataIntegerField("parent_id", "Parent Id"));
        facetBag.add(new DataTextField("field_name", "Field Name"));

        return facetBag;
    }

    private DataBag createSpellCheckBag()
    {
        DataBag spellCheckBag = new DataBag("Spelling Suggestions Fields");

        spellCheckBag.add(new DataBooleanField("is_spelled_correctly", "Is Spelled Correctly"));
        spellCheckBag.add(new DataTextField("suggestion", "Suggestion"));

        return spellCheckBag;
    }

    private DataBag createStatisticsBag()
    {
        DataBag statisticsBag = new DataBag("Statistics Table");

        statisticsBag.add(new DataTextField("field_name", "Field Name"));
        statisticsBag.add(new DataTextField("field_title", "Field Title"));
        statisticsBag.add(new DataDoubleField("min", "Minimum Value"));
        statisticsBag.add(new DataDoubleField("max", "Maximum Value"));
        statisticsBag.add(new DataIntegerField("count", "Count"));
        statisticsBag.add(new DataIntegerField("missing", "Missing"));
        statisticsBag.add(new DataDoubleField("sum", "Sum"));
        statisticsBag.add(new DataDoubleField("mean", "Mean"));
        statisticsBag.add(new DataDoubleField("standard_deviation", "Standard Deviation"));

        return statisticsBag;
    }

    /**
     * Instantiates the Solr document in its minimal form to start
     * (header and response).  Other search components are added
     * when they are detected in the Solr reply payload.
     *
     * @param aBag Schema bag instance.
     */
    private void instantiate(final DataBag aBag)
    {
        DataBag emptyBag = new DataBag("Solr Response Document");

        if (aBag == null)
            mBag = emptyBag;
        else
            mBag = new DataBag(aBag);

        mDocument = new Document(Solr.DOCUMENT_TYPE, emptyBag);
        mDocument.addRelationship(Solr.RESPONSE_HEADER, createHeaderBag());
        mDocument.addRelationship(Solr.RESPONSE_DOCUMENT, emptyBag);
    }

    public void updateHeader(String aBaseURL, String aQuery, String aRequestHandler, int anOffset, int aLimit)
    {
        Logger appLogger = mAppMgr.getLogger(this, "updateHeader");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Relationship headerRelationship = mDocument.getFirstRelationship(Solr.RESPONSE_HEADER);
        if (headerRelationship != null)
        {
            DataBag headerBag = headerRelationship.getBag();
            if (StringUtils.isNotEmpty(aBaseURL))
            {
                headerBag.setValueByName("base_url", aBaseURL);
                String collectionName = headerBag.getValueAsString("collection_name");
                if (StringUtils.isEmpty(collectionName))
                {
                    collectionName = StringUtils.substringAfterLast(aBaseURL, "/");
                    headerBag.setValueByName("collection_name", collectionName);
                }
            }
            if (StringUtils.isNotEmpty(aQuery))
                headerBag.setValueByName("query_keyword", aQuery);
            if (aLimit > 0)
                headerBag.setValueByName("page_size", aLimit);
            if (anOffset > -1)
                headerBag.setValueByName("offset_start", anOffset);
            if (StringUtils.isNotEmpty(aRequestHandler))
            {
                if (aRequestHandler.charAt(0) == StrUtl.CHAR_FORWARDSLASH)
                    headerBag.setValueByName("request_handler", aRequestHandler);
                else
                    headerBag.setValueByName("request_handler", StrUtl.CHAR_FORWARDSLASH + aRequestHandler);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void populateHeader(QueryResponse aQueryResponse, long anOffset, long aLimit)
    {
        Logger appLogger = mAppMgr.getLogger(this, "populateHeader");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Relationship headerRelationship = mDocument.getFirstRelationship(Solr.RESPONSE_HEADER);
        if (headerRelationship != null)
        {
            DataBag headerBag = headerRelationship.getBag();
            headerBag.resetValues();

            headerBag.setValueByName("page_size", aLimit);
            headerBag.setValueByName("offset_start", anOffset);
            headerBag.setValueByName("query_time", aQueryResponse.getQTime());

            String propertyName = getCfgPropertyPrefix() + ".request_handler";
            String requestHandler = mAppMgr.getString(propertyName);
            if (StringUtils.isNotEmpty(requestHandler))
            {
                if (requestHandler.charAt(0) == StrUtl.CHAR_FORWARDSLASH)
                    headerBag.setValueByName("request_handler", requestHandler);
                else
                    headerBag.setValueByName("request_handler", StrUtl.CHAR_FORWARDSLASH + requestHandler);
            }

            int statusCode = aQueryResponse.getStatus();
            headerBag.setValueByName("status_code", statusCode);
            if (statusCode == Solr.RESPONSE_STATUS_SUCCESS)
                headerBag.setValueByName("status_message", "Success");
            else
                headerBag.setValueByName("status_message", "Failure");

            SolrDocumentList solrDocumentList = aQueryResponse.getResults();
            if (solrDocumentList != null)
            {
                Float maxScore = solrDocumentList.getMaxScore();
                if (maxScore == null)
                    maxScore = (float) 0.0;
                headerBag.setValueByName("max_score", maxScore);
                headerBag.setValueByName("fetch_count", solrDocumentList.size());
                long totalCount = solrDocumentList.getNumFound();
                headerBag.setValueByName("total_count", totalCount);
            }

            NamedList<Object> headerList = aQueryResponse.getHeader();
            if (headerList != null)
            {
                NamedList<Object> paramList = (NamedList<Object>) aQueryResponse.getResponseHeader().get("params");
                if (paramList != null)
                {
                    Object paramObject = paramList.get("fl");
                    if (paramObject != null)
                    {
                        DataField dataField = headerBag.getFieldByName("field_list");
                        if (paramObject instanceof String)
                            dataField.addValue(paramObject.toString());
                        else if (paramObject instanceof List)
                        {
                            List fieldList = (List) paramObject;
                            int fieldCount = fieldList.size();
                            if (fieldCount > 0)
                            {
                                for (int i = 0; i < fieldCount; i++)
                                    dataField.addValue(fieldList.get(i).toString());
                            }
                        }
                    }
                    paramObject = paramList.get("hl");
                    if (paramObject != null)
                    {
                        DataField dataField = headerBag.getFieldByName("is_highlighted");
                        if (paramObject instanceof String)
                        {
                            if (StringUtils.equalsIgnoreCase(paramObject.toString(), "on"))
                                dataField.setValue(true);
                        }
                    }
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    @SuppressWarnings("unchecked")
    private void populateDocument(Document aDocument, SolrDocumentList aSolrDocumentList)
    {
        Date entryDate;
        DataBag resultBag;
        Object entryObject;
        DataField dataField;
        String cellName, cellValue;
        ArrayList<Object> objectArrayList;
        ArrayList<String> stringArrayList;
        Logger appLogger = mAppMgr.getLogger(this, "populateDocument");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aDocument != null) && (aSolrDocumentList != null))
        {
            DataTable resultTable = aDocument.getTable();

            if (mIsSchemaStatic)
                resultTable.emptyRows();
            else
            {
                resultTable.empty();
                resultBag = resultTable.getColumnBag();
                resultBag.setTitle("Document Table");
                for (SolrDocument solrDocument : aSolrDocumentList)
                {
                    for (Map.Entry<String,Object> entry : solrDocument.entrySet())
                    {
                        cellName = entry.getKey();
                        dataField = resultBag.getFieldByName(cellName);
                        if (dataField == null)
                        {
                            entryObject = entry.getValue();
                            dataField = new DataField(Field.getTypeField(entryObject), cellName, Field.nameToTitle(cellName));
                            if (Solr.isSolrReservedFieldName(cellName))
                                dataField.addFeature(Field.FEATURE_IS_VISIBLE, StrUtl.STRING_FALSE);
                            resultBag.add(dataField);
                        }
                    }
                }
            }
            resultBag = resultTable.getColumnBag();
            for (SolrDocument solrDocument : aSolrDocumentList)
            {
                resultTable.newRow();
                for (Map.Entry<String,Object> entry : solrDocument.entrySet())
                {
                    cellName = entry.getKey();
                    entryObject = entry.getValue();
                    if (entryObject instanceof ArrayList)
                    {
                        stringArrayList = new ArrayList<String>();
                        objectArrayList = (ArrayList<Object>) entryObject;
                        for (Object alo : objectArrayList)
                        {
                            cellValue = alo.toString();
                            stringArrayList.add(cellValue);
                        }
                        resultTable.setValuesByName(cellName, stringArrayList);
                    }
                    else
                    {
                        cellValue = entryObject.toString();
                        if (StringUtils.isNotEmpty(cellValue))
                        {
                            dataField = resultBag.getFieldByName(cellName);
                            if ((dataField != null) && (dataField.isTypeDateOrTime()))
                            {
                                entryDate = (Date) entryObject;
                                resultTable.setValueByName(cellName, entryDate);
                            }
                            else
                                resultTable.setValueByName(cellName, cellValue);
                        }
                    }
                }
                resultTable.addRow();
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    @SuppressWarnings("unchecked")
    private void populateResponseDocument(QueryResponse aQueryResponse)
    {
        Logger appLogger = mAppMgr.getLogger(this, "populateResponseDocument");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Relationship documentRelationship = mDocument.getFirstRelationship(Solr.RESPONSE_DOCUMENT);
        if (documentRelationship != null)
        {
            documentRelationship.getDocuments().clear();

/* The following is a key logic feature for unused field collapsing. If we assign
the fields of a schema bag to unassigned, then we can detect which fields can be
dropped from the result table via the collapseUnusedColumns() method. */

            DataBag resultBag = new DataBag(mBag);
            resultBag.setAssignedFlagAll(false);
            DataTable resultTable = new DataTable(resultBag);
            Document responseDocument = new Document(Solr.RESPONSE_DOCUMENT, resultTable);
            populateDocument(responseDocument, aQueryResponse.getResults());
            documentRelationship.add(responseDocument);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void populateFacet(DataTable aTable, FacetField aFacetField)
    {
        FieldRow fieldRow;
        DataField schemaField;
        ArrayList<String> facetValues;
        List<FacetField.Count> facetFieldValues;
        String fieldName, facetName, facetNameCount;
        Logger appLogger = mAppMgr.getLogger(this, "populateFacet");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataBag resultBag = mBag;
        fieldName = aFacetField.getName();
        facetFieldValues = aFacetField.getValues();
        for (FacetField.Count ffc : facetFieldValues)
        {
            facetName = ffc.getName();
            schemaField = resultBag.getFieldByName(fieldName);
            if (schemaField == null)
                facetNameCount = String.format("%s (%s)", Field.nameToTitle(facetName), ffc.getCount());
            else
                facetNameCount = String.format("%s (%s)", facetName, ffc.getCount());
            fieldRow = Field.firstFieldRow(aTable.findValue("field_name", Field.Operator.EQUAL, fieldName));
            if (fieldRow == null)
            {
                aTable.newRow();
                aTable.setValueByName("field_name", fieldName);
                if (schemaField == null)
                    aTable.setValueByName("field_title", Field.nameToTitle(fieldName));
                else
                    aTable.setValueByName("field_title", schemaField.getTitle());
                aTable.setValueByName("facet_name_count", facetNameCount);
                aTable.addRow();
            }
            else
            {
                facetValues = aTable.getValuesByName(fieldRow, "facet_name_count");
                facetValues.add(facetNameCount);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void populateFacetField(QueryResponse aQueryResponse)
    {
        Logger appLogger = mAppMgr.getLogger(this, "populateFacetField");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        List<FacetField> facetFields = aQueryResponse.getFacetFields();
        List<FacetField> facetDateFields = aQueryResponse.getFacetDates();
        if ((facetFields != null) || (facetDateFields != null))
        {
            mDocument.addRelationship(Solr.RESPONSE_FACET_FIELD, createFacetFieldBag());
            Relationship facetRelationship = mDocument.getFirstRelationship(Solr.RESPONSE_FACET_FIELD);
            if (facetRelationship != null)
            {
                DataBag facetBag = new DataBag(facetRelationship.getBag());
                facetBag.setAssignedFlagAll(false);
                DataTable facetTable = new DataTable(facetBag);
                if (facetFields != null)
                {
                    for (FacetField facetField : facetFields)
                        populateFacet(facetTable, facetField);
                }
                if (facetDateFields != null)
                {
                    for (FacetField facetField : facetDateFields)
                        populateFacet(facetTable, facetField);
                }
                Document facetDocument = new Document(Solr.RESPONSE_FACET_FIELD, facetTable);
                facetRelationship.add(facetDocument);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void populateFacetQuery(QueryResponse aQueryResponse)
    {
        Logger appLogger = mAppMgr.getLogger(this, "populateFacetQuery");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Map<String,Integer> facetQueries = aQueryResponse.getFacetQuery();
        if ((facetQueries != null) && (facetQueries.size() > 0))
        {
            mDocument.addRelationship(Solr.RESPONSE_FACET_QUERY, createFacetQueryBag());
            Relationship facetRelationship = mDocument.getFirstRelationship(Solr.RESPONSE_FACET_QUERY);
            if (facetRelationship != null)
            {
                DataBag facetBag = new DataBag(facetRelationship.getBag());
                facetBag.setAssignedFlagAll(false);
                DataTable facetTable = new DataTable(facetBag);
                for (Map.Entry<String,Integer> entry : facetQueries.entrySet())
                {
                    facetTable.newRow();

                    facetTable.setValueByName("search_term", entry.getKey());
                    facetTable.setValueByName("facet_count", entry.getValue());

                    facetTable.addRow();
                }
                Document facetDocument = new Document(Solr.RESPONSE_FACET_QUERY, facetTable);
                facetRelationship.add(facetDocument);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private int populateFacetPivot(DataTable aTable, String[] aFacetNames,
                                   int aRowId, int aParentId,
                                   PivotField aPivotField)
    {
        Object facetValue;
        FieldRow fieldRow;
        String fieldName, facetName, facetNameCount;
        Logger appLogger = mAppMgr.getLogger(this, "populateFacetPivot");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aPivotField != null)
        {
            fieldName = aPivotField.getField();
            facetValue = aPivotField.getValue();
            if (facetValue == null)
                facetNameCount = String.format("Unassigned (%s)", aPivotField.getCount());
            else
            {
                facetName = aPivotField.getValue().toString();
                facetNameCount = String.format("%s (%s)", facetName, aPivotField.getCount());
            }

            ArrayList<String> facetValues = null;
            if (StringUtils.equals(fieldName, aFacetNames[0]))
                aParentId = 0;
            else
            {
                int rowCount = aTable.rowCount();
                if (rowCount > 0)
                {
                    fieldRow = aTable.getRow(rowCount-1);
                    facetValues = aTable.getValuesByName(fieldRow, fieldName);
                    if (facetValues != null)
                    {
                        if (facetValues.size() == 0)
                            facetValues = null;
                        else
                            facetValues.add(facetNameCount);
                    }
                }
            }
            if (facetValues == null)
            {
                fieldRow = aTable.newRow();
                aTable.setValueByName(fieldRow, "id", aRowId+1);
                aTable.setValueByName(fieldRow, "parent_id", aParentId);
                facetValues = aTable.getValuesByName(fieldRow, fieldName);
                if (facetValues != null)
                {
                    facetValues.add(facetNameCount);
                    if (facetValues.size() == 1)
                    {
                        aTable.addRow(fieldRow);
                        aRowId = aTable.rowCount();
                        aParentId = aRowId;
                    }
                }
            }

            if (aPivotField.getPivot() != null)
            {
                for (PivotField pivotField : aPivotField.getPivot())
                    aRowId = populateFacetPivot(aTable, aFacetNames, aRowId, aParentId, pivotField);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return aRowId;
    }

    private void populateFacetPivot(QueryResponse aQueryResponse)
    {
        DataField schemaField, dataField;
        Logger appLogger = mAppMgr.getLogger(this, "populateFacetPivot");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        NamedList<List<PivotField>> facetPivotFields = aQueryResponse.getFacetPivot();
        if (facetPivotFields != null)
        {
            mDocument.addRelationship(Solr.RESPONSE_FACET_PIVOT, createFacetPivotBag());
            Relationship facetRelationship = mDocument.getFirstRelationship(Solr.RESPONSE_FACET_PIVOT);
            if (facetRelationship != null)
            {
                facetRelationship.getDocuments().clear();
                DataBag facetBag = new DataBag(facetRelationship.getBag());
                facetBag.setAssignedFlagAll(false);
                DataTable facetTable = new DataTable(facetBag);
                facetTable.add(new DataIntegerField("id", "Id"));
                facetTable.add(new DataIntegerField("parent_id", "Parent Id"));
                facetBag = facetTable.getColumnBag();
                facetBag.setTitle("Facet Pivot Table");
                DataBag resultBag = mBag;

                String[] facetNames = null;
                for (Map.Entry<String, List<PivotField>> entry : facetPivotFields)
                {
                    facetNames = entry.getKey().split(",");
                    for (String facetName : facetNames)
                    {
                        schemaField = resultBag.getFieldByName(facetName);
                        if (schemaField == null)
                            dataField = new DataTextField(facetName, Field.nameToTitle(facetName));
                        else
                            dataField = new DataTextField(facetName, schemaField.getTitle());
                        dataField.setMultiValueFlag(true);
                        facetBag.add(dataField);
                    }
                }
                if ((facetNames != null) && (facetNames.length > 0))
                {
                    int rowId = 0;
                    int parentId = rowId;
                    for (Map.Entry<String, List<PivotField>> entry : facetPivotFields)
                    {
                        for (PivotField pivotField : entry.getValue())
                        {
                            rowId = populateFacetPivot(facetTable, facetNames, rowId, parentId, pivotField);
                            parentId = rowId;
                        }
                    }
                }

                Document facetDocument = new Document(Solr.RESPONSE_FACET_PIVOT, facetTable);
                facetRelationship.add(facetDocument);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    @SuppressWarnings({"rawtypes"})
    private Field.Type facetRangeToFieldType(RangeFacet aRangeFacet, DataField aField)
    {
        if (aField == null)
        {
            Object startObject = aRangeFacet.getStart();
            if (startObject != null)
            {
                if (startObject instanceof Date)
                    return Field.Type.DateTime;
                else if (startObject instanceof Integer)
                    return Field.Type.Integer;
                else if (startObject instanceof Long)
                    return Field.Type.Long;
                else if (startObject instanceof Float)
                    return Field.Type.Float;
                else if (startObject instanceof Double)
                    return Field.Type.Double;
            }
        }
        else
            return aField.getType();

        return Field.Type.Text;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void populateFacet(DataTable aTable, RangeFacet aRangeFacet)
    {
        DataField schemaField;
        Logger appLogger = mAppMgr.getLogger(this, "populateFacet");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataBag resultBag = mBag;
        String fieldName = aRangeFacet.getName();
        schemaField = resultBag.getFieldByName(fieldName);
        Field.Type fieldType = facetRangeToFieldType(aRangeFacet, schemaField);
        if (Field.isDateOrTime(fieldType))
        {
            RangeFacet<Date,Date> facetRangeDate = (RangeFacet<Date,Date>) aRangeFacet;

            aTable.newRow();
            aTable.setValueByName("field_name", fieldName);
            if ((schemaField != null) && (StringUtils.isNotEmpty(schemaField.getTitle())))
                aTable.setValueByName("field_title", schemaField.getTitle());
            else
                aTable.setValueByName("field_title", Field.nameToTitle(fieldName));
            aTable.setValueByName("field_type", Field.typeToString(fieldType));
            Object objectValue = facetRangeDate.getStart();
            if (objectValue != null)
                aTable.setValueByName("field_start", facetRangeDate.getStart());
            objectValue = facetRangeDate.getEnd();
            if (objectValue != null)
                aTable.setValueByName("field_finish", facetRangeDate.getEnd());
            objectValue = facetRangeDate.getGap();
            if (objectValue != null)
                aTable.setValueByName("field_gap", objectValue.toString());
            objectValue = facetRangeDate.getAfter();
            if (objectValue != null)
                aTable.setValueByName("count_after", objectValue.toString());
            objectValue = facetRangeDate.getBefore();
            if (objectValue != null)
                aTable.setValueByName("count_before", objectValue.toString());
            objectValue = facetRangeDate.getBetween();
            if (objectValue != null)
                aTable.setValueByName("count_between", objectValue.toString());
            ArrayList<String> fieldValueList = new ArrayList<>();
            for (RangeFacet.Count rfCount : facetRangeDate.getCounts())
                fieldValueList.add(String.format("%s (%d)", rfCount.getValue(), rfCount.getCount()));
            aTable.setValuesByName("facet_name_count", fieldValueList);
            aTable.addRow();
        }
        else
        {
            RangeFacet.Numeric facetRangeNumber = (RangeFacet.Numeric) aRangeFacet;

            aTable.newRow();
            aTable.setValueByName("field_name", fieldName);
            if ((schemaField != null) && (StringUtils.isNotEmpty(schemaField.getTitle())))
                aTable.setValueByName("field_title", schemaField.getTitle());
            else
                aTable.setValueByName("field_title", Field.nameToTitle(fieldName));
            aTable.setValueByName("field_type", Field.typeToString(fieldType));
            Object objectValue = facetRangeNumber.getStart();
            if (objectValue != null)
                aTable.setValueByName("field_start", objectValue.toString());
            objectValue = facetRangeNumber.getEnd();
            if (objectValue != null)
                aTable.setValueByName("field_finish", objectValue.toString());
            objectValue = facetRangeNumber.getGap();
            if (objectValue != null)
                aTable.setValueByName("field_gap", objectValue.toString());
            objectValue = facetRangeNumber.getAfter();
            if (objectValue != null)
                aTable.setValueByName("count_after", objectValue.toString());
            objectValue = facetRangeNumber.getBefore();
            if (objectValue != null)
                aTable.setValueByName("count_before", objectValue.toString());
            objectValue = facetRangeNumber.getBetween();
            if (objectValue != null)
                aTable.setValueByName("count_between", objectValue.toString());
            ArrayList<String> fieldValueList = new ArrayList<>();
            for (RangeFacet.Count rfCount : facetRangeNumber.getCounts())
                fieldValueList.add(String.format("%s (%d)", rfCount.getValue(), rfCount.getCount()));
            aTable.setValuesByName("facet_name_count", fieldValueList);
            aTable.addRow();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

// solr/src/solr-7.5.0/solr/solrj/src/test/org/apache/solr/client/ls/response/QueryResponseTest.java

    @SuppressWarnings({"rawtypes"})
    private void populateFacetRange(QueryResponse aQueryResponse)
    {
        Logger appLogger = mAppMgr.getLogger(this, "populateFacetRange");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        List<RangeFacet> facetRangeList = aQueryResponse.getFacetRanges();
        if ((facetRangeList != null) && (facetRangeList.size() > 0))
        {
            mDocument.addRelationship(Solr.RESPONSE_FACET_RANGE, createFacetRangeBag());
            Relationship facetRelationship = mDocument.getFirstRelationship(Solr.RESPONSE_FACET_RANGE);
            if (facetRelationship != null)
            {
                DataBag facetBag = new DataBag(facetRelationship.getBag());
                facetBag.setAssignedFlagAll(false);
                DataTable facetTable = new DataTable(facetBag);
                for (RangeFacet rangeFacet : facetRangeList)
                    populateFacet(facetTable, rangeFacet);

                Document facetDocument = new Document(Solr.RESPONSE_FACET_RANGE, facetTable);
                facetRelationship.add(facetDocument);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void populateMoreLikeThis(QueryResponse aQueryResponse)
    {
        String docName;
        Document mltDocument;
        DataField docNameField;
        SolrDocumentList solrDocumentList;
        Logger appLogger = mAppMgr.getLogger(this, "populateMoreLikeThis");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        NamedList<SolrDocumentList> mltDocuments = aQueryResponse.getMoreLikeThis();
        if (mltDocuments != null)
        {
            mDocument.addRelationship(Solr.RESPONSE_MORE_LIKE_THIS, createMLTBag());
            Relationship mltRelationship = mDocument.getFirstRelationship(Solr.RESPONSE_MORE_LIKE_THIS);
            if (mltRelationship != null)
            {
                int mltCount = mltDocuments.size();
                DataBag mltBag = mltRelationship.getBag();
                docNameField = mltBag.getFieldByName("mlt_name");
                mltBag.setValueByName("mlt_total", mltCount);
                for (int i = 0; i < mltCount; i++)
                {
                    docName = mltDocuments.getName(i);
                    docNameField.addValue(docName);
                    solrDocumentList = mltDocuments.getVal(i);
                    if (solrDocumentList.getNumFound() > 0L)
                    {
                        mltDocument = new Document(Solr.RESPONSE_MLT_DOCUMENT, createDocumentTable());
                        populateDocument(mltDocument, solrDocumentList);
                        mltRelationship.add(mltDocument);
                    }
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    @SuppressWarnings("unchecked")
    private Document createGroupCollectionDocument(Group aGroup)
    {
        Logger appLogger = mAppMgr.getLogger(this, "createGroupCollectionDocument");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Document groupCollectionDocument = createGroupCollection();
        DataBag groupCollectionBag = groupCollectionDocument.getBag();

        String groupName = aGroup.getGroupValue();
        if (StringUtils.isNotEmpty(groupName))
            groupCollectionBag.setValueByName("group_name", groupName);
        SolrDocumentList solrDocumentList = aGroup.getResult();
        if (solrDocumentList != null)
        {
            groupCollectionBag.setValueByName("offset_start", solrDocumentList.getStart());
            groupCollectionBag.setValueByName("total_count", solrDocumentList.getNumFound());
            Document groupDocument = new Document(Solr.RESPONSE_GROUP_DOCUMENT, createDocumentTable());
            populateDocument(groupDocument, solrDocumentList);
            groupCollectionDocument.addRelationship(groupDocument.getType(), groupDocument);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return groupCollectionDocument;
    }

    private void populateGroupResponse(QueryResponse aQueryResponse)
    {
        Logger appLogger = mAppMgr.getLogger(this, "populateGroupResponse");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        GroupResponse groupResponse = aQueryResponse.getGroupResponse();
        if (groupResponse != null)
        {
            mDocument.addRelationship(Solr.RESPONSE_GROUP, createGroupsBag());
            Relationship groupRelationship = mDocument.getFirstRelationship(Solr.RESPONSE_GROUP);
            if (groupRelationship != null)
            {
                DataBag groupingBag = groupRelationship.getBag();

                List<GroupCommand> groupCommandList = groupResponse.getValues();
                if (groupCommandList != null)
                {
                    String groupName;

                    DataField dfGroupName = groupingBag.getFieldByName("group_name");
                    groupingBag.setValueByName("group_total", groupCommandList.size());

                    for (GroupCommand groupCommand : groupCommandList)
                    {
                        groupName = groupCommand.getName();
                        if (StringUtils.isNotEmpty(groupName))
                            dfGroupName.addValue(groupName);

                        groupingBag.setValueByName("group_matches", groupCommand.getMatches());

                        List<Group> groupList = groupCommand.getValues();
                        if (groupList != null)
                        {
                            for (Group groupMember : groupList)
                                groupRelationship.add(createGroupCollectionDocument(groupMember));
                        }
                    }
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void populateSpelling(QueryResponse aQueryResponse)
    {
        Logger appLogger = mAppMgr.getLogger(this, "populateSpelling");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        SpellCheckResponse spellCheckResponse = aQueryResponse.getSpellCheckResponse();
        if (spellCheckResponse != null)
        {
            mDocument.addRelationship(Solr.RESPONSE_SPELLING, createSpellCheckBag());
            Relationship spellingRelationship = mDocument.getFirstRelationship(Solr.RESPONSE_SPELLING);
            if (spellingRelationship != null)
            {
                DataBag spellingBag = spellingRelationship.getBag();
                spellingBag.setValueByName("suggestion", spellCheckResponse.getCollatedResult());
                spellingBag.setValueByName("is_spelled_correctly", spellCheckResponse.isCorrectlySpelled());
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void populateStatistic(QueryResponse aQueryResponse)
    {
        Object statObject;
        DataField schemaField;
        FieldStatsInfo fieldStatsInfo;
        String fieldName, fieldTitle, fieldValue;
        Logger appLogger = mAppMgr.getLogger(this, "populateStatistic");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Map<String,FieldStatsInfo> mapFieldStatsInfo = aQueryResponse.getFieldStatsInfo();
        if (mapFieldStatsInfo != null)
        {
            mDocument.addRelationship(Solr.RESPONSE_STATISTIC, createStatisticsBag());
            Relationship statisticRelationship = mDocument.getFirstRelationship(Solr.RESPONSE_STATISTIC);
            if (statisticRelationship != null)
            {
                DataBag statisticsBag = new DataBag(statisticRelationship.getBag());
                statisticsBag.setAssignedFlagAll(false);
                DataTable statisticTable = new DataTable(statisticsBag);
                DataBag resultBag = mBag;

                for (Map.Entry<String,FieldStatsInfo> entry : mapFieldStatsInfo.entrySet())
                {
                    statisticTable.newRow();
                    fieldName = entry.getKey();
                    fieldStatsInfo = entry.getValue();
                    statisticTable.setValueByName("field_name", fieldName);
                    schemaField = resultBag.getFieldByName(fieldName);
                    if (schemaField == null)
                        fieldTitle = Field.nameToTitle(fieldName);
                    else
                        fieldTitle = schemaField.getTitle();
                    statisticTable.setValueByName("field_title", fieldTitle);
                    statObject = fieldStatsInfo.getMin();
                    if (statObject == null)
                        fieldValue = StringUtils.EMPTY;
                    else
                        fieldValue = statObject.toString();
                    statisticTable.setValueByName("min", fieldValue);
                    statObject = fieldStatsInfo.getMax();
                    if (statObject == null)
                        fieldValue = StringUtils.EMPTY;
                    else
                        fieldValue = statObject.toString();
                    statisticTable.setValueByName("max", fieldValue);
                    statObject = fieldStatsInfo.getCount();
                    if (statObject == null)
                        fieldValue = StringUtils.EMPTY;
                    else
                        fieldValue = statObject.toString();
                    statisticTable.setValueByName("count", fieldValue);
                    statObject = fieldStatsInfo.getMissing();
                    if (statObject == null)
                        fieldValue = StringUtils.EMPTY;
                    else
                        fieldValue = statObject.toString();
                    statisticTable.setValueByName("missing", fieldValue);
                    statObject = fieldStatsInfo.getSum();
                    if (statObject == null)
                        fieldValue = StringUtils.EMPTY;
                    else
                        fieldValue = statObject.toString();
                    statisticTable.setValueByName("sum", fieldValue);
                    statObject = fieldStatsInfo.getMean();
                    if (statObject == null)
                        fieldValue = StringUtils.EMPTY;
                    else
                        fieldValue = statObject.toString();
                    statisticTable.setValueByName("mean", fieldValue);
                    statObject = fieldStatsInfo.getStddev();
                    if (statObject == null)
                        fieldValue = StringUtils.EMPTY;
                    else
                        fieldValue = statObject.toString();
                    statisticTable.setValueByName("standard_deviation", fieldValue);
                    statisticTable.addRow();
                }

                Document facetDocument = new Document(Solr.RESPONSE_STATISTIC, statisticTable);
                statisticRelationship.add(facetDocument);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void assignHighlightingToResults(DataTable aResultTable, DataField aPrimaryKeyField,
                                             String aDocId, String aFieldName, ArrayList<String> aHighlight)
    {
        Logger appLogger = mAppMgr.getLogger(this, "populateHighlighting");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        FieldRow fieldRow = Field.firstFieldRow(aResultTable.findValue(aPrimaryKeyField.getName(), Field.Operator.EQUAL, aDocId));
        if (fieldRow != null)
            aResultTable.setValuesByName(fieldRow, aFieldName, aHighlight);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void populateHighlighting(QueryResponse aQueryResponse)
    {
        String docId, fieldName;
        List<String> entryValues;
        ArrayList<String> fieldValues;
        Map<String,List<String>> mapHighlightList;
        Logger appLogger = mAppMgr.getLogger(this, "populateHighlighting");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Relationship documentRelationship = mDocument.getFirstRelationship(Solr.RESPONSE_DOCUMENT);
        if ((documentRelationship != null) && (documentRelationship.count() > 0))
        {
            boolean isHighlighted = false;

            Document responseDocument = documentRelationship.getFirstDocument();
            DataTable resultTable = responseDocument.getTable();
            DataBag resultBag = resultTable.getColumnBag();
            DataField dfPrimaryKey = resultBag.getPrimaryKeyField();
            if ((mIsSchemaStatic) && (dfPrimaryKey != null))
            {
                Map<String,Map<String,List<String>>> mapHighlighting = aQueryResponse.getHighlighting();
                if (mapHighlighting != null)
                {
                    for (Map.Entry<String,Map<String,List<String>>> entry1 : mapHighlighting.entrySet())
                    {
                        docId = entry1.getKey();
                        mapHighlightList = entry1.getValue();
                        if (mapHighlightList != null)
                        {
                            for (Map.Entry<String,List<String>> entry2 : mapHighlightList.entrySet())
                            {
                                fieldName = entry2.getKey();
                                entryValues = entry2.getValue();
                                if (entryValues != null)
                                {
                                    fieldValues = new ArrayList<>(entryValues);
                                    assignHighlightingToResults(resultTable, dfPrimaryKey, docId, fieldName, fieldValues);
                                    if (! isHighlighted)
                                        isHighlighted = true;
                                }
                            }
                        }
                    }
                }

                if (isHighlighted)
                {
                    DataBag headerBag = Solr.getHeader(mDocument);
                    headerBag.setValueByName("is_highlighted", isHighlighted);
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Extracts the query response message from Solr into a normalized
     * NS Document representation.
     *
     * @param aQueryResponse Solr query response instance.
     * @param anOffset Starting offset into the Solr result set.
     * @param aLimit Limit on the total number of rows to fetch from
     *               the Solr index.
     *
     * @return NS Document instance.
     */
    public Document extract(QueryResponse aQueryResponse, int anOffset, int aLimit)
    {
        Logger appLogger = mAppMgr.getLogger(this, "extract");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

// solr/src/solr-7.5.0/solr/solrj/src/test/org/apache/solr/client/ls/response/QueryResponseTest.java

        populateHeader(aQueryResponse, anOffset, aLimit);
        populateResponseDocument(aQueryResponse);
        populateGroupResponse(aQueryResponse);
        populateFacetField(aQueryResponse);
        populateFacetQuery(aQueryResponse);
        populateFacetPivot(aQueryResponse);
        populateFacetRange(aQueryResponse);
        populateSpelling(aQueryResponse);
        populateStatistic(aQueryResponse);
        populateHighlighting(aQueryResponse);
        populateMoreLikeThis(aQueryResponse);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return mDocument;
    }

    /**
     * Extracts the query response message from Solr into a normalized
     * NS Document representation.
     *
     * @param aQueryResponse Solr query response instance.
     *
     * @return NS Document instance.
     */
    public Document extract(QueryResponse aQueryResponse)
    {
        Logger appLogger = mAppMgr.getLogger(this, "extract");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        extract(aQueryResponse, Solr.QUERY_OFFSET_DEFAULT, Solr.QUERY_PAGESIZE_DEFAULT);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return mDocument;
    }
}
