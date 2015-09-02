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
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.io.xml.IOXML;
import com.nridge.core.base.std.XMLUtl;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.PrintWriter;
import java.util.ArrayList;

/**
 * The SolrDocumentXML provides a collection of methods to generate
 * an XML Solr document feed file suitable for uploading to the index
 * server.
 *
 * @see <a href="http://wiki.apache.org/solr/UpdateXmlMessages">Apache Solr XML Messages</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class SolrDocumentXML
{
    private final boolean FIELD_UPDATE_DISABLED = false;

    private AppMgr mAppMgr;
    private String mOperation;
    private boolean mIncludeChildren;
    private PrintWriter mPrintWriter;

    public SolrDocumentXML(AppMgr anAppMgr, String anOperation, PrintWriter aPrintWriter)
    {
        mAppMgr = anAppMgr;
        mOperation = anOperation;
        mPrintWriter = aPrintWriter;
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

    public void writeHeader()
    {
        Logger appLogger = mAppMgr.getLogger(this, "writeHeader");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mPrintWriter != null)
        {
            mPrintWriter.printf("<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n");
            mPrintWriter.printf("<%s>%n", mOperation);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void writeField(DataField aField, boolean anIsUpdate, int anIndentAmount)
    {
        String fieldValueString;

        if ((aField.isAssigned()) && (aField.isFeatureFalse(Field.FEATURE_IS_HIDDEN)))
        {
            if (Field.isDateOrTime(aField.getType()))
                fieldValueString = aField.getValueFormatted(Solr.DOC_DATETIME_FORMAT);
            else
                fieldValueString = aField.getValue();

            if (anIsUpdate)
            {
                if (StringUtils.isNotEmpty(fieldValueString))
                {
                    if (aField.isMultiValue())
                    {
                        ArrayList<String> fieldValueStrings = aField.getValues();
                        for (String fvs : fieldValueStrings)
                        {
                            IOXML.indentLine(mPrintWriter, anIndentAmount);
                            if (aField.isFeatureTrue(Solr.FEATURE_IS_CONTENT))
                                mPrintWriter.printf("<field name=\"%s\" update=\"set\">%s</field>%n",
                                                    aField.getName(), XMLUtl.escapeElemStrValue(fvs));
                            else
                                mPrintWriter.printf("<field name=\"%s\" update=\"set\">%s</field>%n",
                                                    aField.getName(), StringEscapeUtils.escapeXml10(fvs));
                        }
                    }
                    else
                    {
                        IOXML.indentLine(mPrintWriter, anIndentAmount);
                        if (aField.isFeatureTrue(Solr.FEATURE_IS_CONTENT))
                            mPrintWriter.printf("<field name=\"%s\" update=\"set\">%s</field>%n",
                                                aField.getName(), XMLUtl.escapeElemStrValue(fieldValueString));
                        else
                            mPrintWriter.printf("<field name=\"%s\" update=\"set\">%s</field>%n",
                                                aField.getName(), StringEscapeUtils.escapeXml10(fieldValueString));
                    }
                }
            }
            else
            {
                if (StringUtils.isNotEmpty(fieldValueString))
                {
                    if (aField.isMultiValue())
                    {
                        ArrayList<String> fieldValueStrings = aField.getValues();
                        for (String fvs : fieldValueStrings)
                        {
                            IOXML.indentLine(mPrintWriter, anIndentAmount);
                            if (aField.isFeatureTrue(Solr.FEATURE_IS_CONTENT))
                                mPrintWriter.printf("<field name=\"%s\">%s</field>%n",
                                                    aField.getName(), XMLUtl.escapeElemStrValue(fvs));
                            else
                                mPrintWriter.printf("<field name=\"%s\">%s</field>%n",
                                                    aField.getName(), StringEscapeUtils.escapeXml10(fvs));
                        }
                    }
                    else
                    {
                        IOXML.indentLine(mPrintWriter, anIndentAmount);
                        if (aField.isFeatureTrue(Solr.FEATURE_IS_CONTENT))
                            mPrintWriter.printf("<field name=\"%s\">%s</field>%n",
                                                aField.getName(), XMLUtl.escapeElemStrValue(fieldValueString));
                        else
                            mPrintWriter.printf("<field name=\"%s\">%s</field>%n",
                                                aField.getName(), StringEscapeUtils.escapeXml10(fieldValueString));
                    }
                }
            }
        }
    }

    private void writeContent(DataBag aBag, boolean anIsUpdate, int anIndentAmount)
    {
        Logger appLogger = mAppMgr.getLogger(this, "writeContent");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((mPrintWriter != null) && (aBag != null))
        {
            IOXML.indentLine(mPrintWriter, anIndentAmount);
            mPrintWriter.printf("<doc>%n");
            if (mOperation.equals(Solr.DOC_OPERATION_DELETE))
            {
                DataField primaryKeyField = aBag.getPrimaryKeyField();
                if (primaryKeyField != null)
                {
                    DataField dataField = aBag.getFieldByName(primaryKeyField.getName());
                    if (dataField != null)
                        writeField(dataField, FIELD_UPDATE_DISABLED, anIndentAmount+1);
                }
            }
            else
            {
                for (DataField dataField : aBag.getFields())
                {
                    if (dataField.isFeatureTrue(Field.FEATURE_IS_PRIMARY_KEY))
                        writeField(dataField, FIELD_UPDATE_DISABLED, anIndentAmount+1);
                    else
                        writeField(dataField, anIsUpdate, anIndentAmount+1);
                }
            }
            IOXML.indentLine(mPrintWriter, anIndentAmount);
            mPrintWriter.printf("</doc>%n");
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void writeContent(DataBag aBag, int anIndentAmount)
    {
        Logger appLogger = mAppMgr.getLogger(this, "writeContent");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((mPrintWriter != null) && (aBag != null))
        {
            IOXML.indentLine(mPrintWriter, anIndentAmount);
            mPrintWriter.printf("<doc>%n");
            if (mOperation.equals(Solr.DOC_OPERATION_DELETE))
            {
                DataField primaryKeyField = aBag.getPrimaryKeyField();
                if (primaryKeyField != null)
                {
                    DataField dataField = aBag.getFieldByName(primaryKeyField.getName());
                    if (dataField != null)
                        writeField(dataField, FIELD_UPDATE_DISABLED, anIndentAmount+1);
                }
            }
            else
            {
                for (DataField dataField : aBag.getFields())
                    writeField(dataField, FIELD_UPDATE_DISABLED, anIndentAmount+1);
            }
            IOXML.indentLine(mPrintWriter, anIndentAmount);
            mPrintWriter.printf("</doc>%n");
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void writeContent(Document aDocument, boolean anIsUpdate, int anIndentAmount)
    {
        Logger appLogger = mAppMgr.getLogger(this, "writeContent");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((mPrintWriter != null) && (aDocument != null))
        {
            DataBag docBag = aDocument.getBag();
            if (docBag.count() > 0)
            {
                IOXML.indentLine(mPrintWriter, anIndentAmount);
                mPrintWriter.printf("<doc>%n");
                if (! docBag.isValid())
                {
                    IOXML.indentLine(mPrintWriter, anIndentAmount+1);
                    mPrintWriter.printf("<!-- Invalid Data -->%n");
                }
                if (mOperation.equals(Solr.DOC_OPERATION_DELETE))
                {
                    DataField primaryKeyField = docBag.getPrimaryKeyField();
                    if (primaryKeyField != null)
                    {
                        DataField dataField = docBag.getFieldByName(primaryKeyField.getName());
                        if (dataField != null)
                            writeField(dataField, anIsUpdate, anIndentAmount+1);
                    }
                }
                else
                {
                    for (DataField dataField : docBag.getFields())
                        writeField(dataField, anIsUpdate, anIndentAmount+1);
                }
                if (mIncludeChildren)
                {
                    DataBag relBag;

                    ArrayList<Relationship> relationshipList = aDocument.getRelationships();
                    for (Relationship relationship : relationshipList)
                    {
                        relBag = relationship.getBag();
                        if (relBag.count() > 0)
                        {
                            IOXML.indentLine(mPrintWriter, anIndentAmount+1);
                            mPrintWriter.printf("<doc>%n");
                            if (! relBag.isValid())
                            {
                                IOXML.indentLine(mPrintWriter, anIndentAmount+1);
                                mPrintWriter.printf("<!-- Invalid Data -->%n");
                            }
                            for (DataField dataField : relBag.getFields())
                                writeField(dataField, anIsUpdate, anIndentAmount+2);
                            IOXML.indentLine(mPrintWriter, anIndentAmount+1);
                            mPrintWriter.printf("</doc>%n");
                        }
                    }
                }
                IOXML.indentLine(mPrintWriter, anIndentAmount);
                mPrintWriter.printf("</doc>%n");
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void writeContent(ArrayList<Document> aList, boolean anIsUpdate, int anIndentAmount)
    {
        Logger appLogger = mAppMgr.getLogger(this, "writeContent");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        for (Document document : aList)
            writeContent(document, anIsUpdate, anIndentAmount);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void writeContent(ArrayList<Document> aList, int anIndentAmount)
    {
        Logger appLogger = mAppMgr.getLogger(this, "writeContent");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        writeContent(aList, FIELD_UPDATE_DISABLED, anIndentAmount);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void writeContent(DataTable aTable, boolean anIsUpdate, int anIndentAmount)
    {
        Logger appLogger = mAppMgr.getLogger(this, "writeContent");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((mPrintWriter != null) && (aTable != null))
        {
            int rowCount = aTable.rowCount();
            for (int row = 0; row < rowCount; row++)
                writeContent(aTable.getRowAsBag(row), anIsUpdate, anIndentAmount);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void writeContent(DataTable aTable, int anIndentAmount)
    {
        Logger appLogger = mAppMgr.getLogger(this, "writeContent");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((mPrintWriter != null) && (aTable != null))
        {
            int rowCount = aTable.rowCount();
            for (int row = 0; row < rowCount; row++)
                writeContent(aTable.getRowAsBag(row), anIndentAmount);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void writeCommit()
    {
        Logger appLogger = mAppMgr.getLogger(this, "writeCommit");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mPrintWriter != null)
            mPrintWriter.printf("<commit/>%n");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void writeOptimize()
    {
        Logger appLogger = mAppMgr.getLogger(this, "writeOptimize");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mPrintWriter != null)
            mPrintWriter.printf("<optimize/>%n");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void writeTrailer()
    {
        Logger appLogger = mAppMgr.getLogger(this, "writeTrailer");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mPrintWriter != null)
        {
            mPrintWriter.printf("</%s>%n", mOperation);
            mPrintWriter.flush();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    public void writeTrailerAndClose()
    {
        Logger appLogger = mAppMgr.getLogger(this, "writeTrailerAndClose");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mPrintWriter != null)
        {
            writeTrailer();
            mPrintWriter.close();
            mPrintWriter = null;
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
