/*
 * NorthRidge Software, LLC - Copyright (c) 2016.
 *
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains
 * the property of NorthRidge Software, LLC and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to NorthRidge Software, LLC and its
 * suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or
 * copyright law.  Dissemination of this information or
 * reproduction of this material is strictly forbidden unless
 * prior written permission is obtained from NorthRidge
 * Software, LLC.
 */

package com.nridge.ds.solr;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.google.gson.stream.JsonWriter;
import com.nridge.core.io.gson.IOJSON;
import org.slf4j.Logger;

import java.io.*;
import java.util.ArrayList;

/**
 * The SolrDocumentJSON provides a collection of methods to generate
 * a JSON Solr document feed file suitable for uploading to the index
 * server.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/solr/Transforming+and+Indexing+Custom+JSON">Apache Solr JSON Messages</a>
 * @see <a href="https://cwiki.apache.org/confluence/display/solr/Updating+Parts+of+Documents">Apache Solr JSON Atomic Updates</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class SolrDocumentJSON
{
    private AppMgr mAppMgr;
    private boolean mIncludeChildren;

    public SolrDocumentJSON(AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
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

    public void saveContent(JsonWriter aWriter, DataField aField)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "saveContent");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aWriter != null) && (aField != null))
        {
            if (aField.isMultiValue())
                IOJSON.writeNameValue(aWriter, aField.getName(), aField.getValues());
            else
            {
                String fieldValueString;

                if (Field.isDateOrTime(aField.getType()))
                    fieldValueString = aField.getValueFormatted(Solr.DOC_DATETIME_FORMAT);
                else
                    fieldValueString = aField.getValue();
                IOJSON.writeNameValue(aWriter, aField.getName(), fieldValueString);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void saveContent(JsonWriter aWriter, DataBag aBag)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "saveContent");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aWriter != null) && (aBag != null) && (aBag.count() > 0))
        {
            for (DataField dataField : aBag.getFields())
                saveContent(aWriter, dataField);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void saveContent(JsonWriter aWriter, DataTable aTable)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "saveContent");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aWriter != null) && (aTable != null))
        {
            aWriter.beginArray();
            int rowCount = aTable.rowCount();
            for (int row = 0; row < rowCount; row++)
                saveContent(aWriter, aTable.getRowAsBag(row));
            aWriter.endArray();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void save(String aPathFileName, DataTable aTable)
        throws IOException
    {
        StringWriter stringWriter = new StringWriter();
        try (PrintWriter printWriter = new PrintWriter(stringWriter))
        {
            try (JsonWriter jsonWriter = new JsonWriter(printWriter))
            {
                jsonWriter.setIndent(" ");
                saveContent(jsonWriter, aTable);
            }
        }
    }

    public void saveContent(JsonWriter aWriter, Document aDocument)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "saveContent");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aWriter != null) && (aDocument != null))
        {
            DataBag docBag = aDocument.getBag();
            saveContent(aWriter, docBag);
            if (mIncludeChildren)
            {
                DataBag relBag;

                aWriter.name("_childDocuments_").beginArray();
                ArrayList<Relationship> relationshipList = aDocument.getRelationships();
                for (Relationship relationship : relationshipList)
                {
                    relBag = relationship.getBag();
                    saveContent(aWriter, relBag);
                }
                aWriter.endArray();
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void saveContent(JsonWriter aWriter, ArrayList<Document> aList)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "saveContent");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aList != null)
        {
            aWriter.beginArray();
            for (Document document : aList)
                saveContent(aWriter, document);
            aWriter.endArray();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void save(String aPathFileName, ArrayList<Document> aList)
        throws IOException
    {
        StringWriter stringWriter = new StringWriter();
        try (PrintWriter printWriter = new PrintWriter(stringWriter))
        {
            try (JsonWriter jsonWriter = new JsonWriter(printWriter))
            {
                jsonWriter.setIndent(" ");
                saveContent(jsonWriter, aList);
            }
        }
    }

    public String saveAsString(String aPathFileName, DataTable aTable)
        throws IOException
    {
        StringWriter stringWriter = new StringWriter();
        try (PrintWriter printWriter = new PrintWriter(stringWriter))
        {
            try (JsonWriter jsonWriter = new JsonWriter(printWriter))
            {
                jsonWriter.setIndent(" ");
                saveContent(jsonWriter, aTable);
            }
        }

        return stringWriter.toString();
    }

    public String saveAsString(String aPathFileName, ArrayList<Document> aList)
        throws IOException
    {
        StringWriter stringWriter = new StringWriter();
        try (PrintWriter printWriter = new PrintWriter(stringWriter))
        {
            try (JsonWriter jsonWriter = new JsonWriter(printWriter))
            {
                jsonWriter.setIndent(" ");
                saveContent(jsonWriter, aList);
            }
        }

        return stringWriter.toString();
    }
}
