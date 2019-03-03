/*
 * NorthRidge Software, LLC - Copyright (c) 2019.
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

package com.nridge.core.ds.io.xml;

import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.FieldRange;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.io.xml.IOXML;
import com.nridge.core.base.std.StrUtl;
import com.nridge.core.ds.DSTable;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;

/**
 * The SmartGWTXML provides a collection of methods that can generate/load
 * an XML representation of a DSTable object.  In general, the
 * developer should use the data source I/O methods instead of this helper
 * implementation.
 *
 * @author Al Cole
 * @since 1.0
 */
public class SmartGWTXML
{
    public SmartGWTXML()
    {
    }

    private void saveRecord(PrintWriter aPW, DataBag aBag)
    {
        aPW.printf("<record>%n");
        for (DataField dataField : aBag.getFields())
        {
            aPW.printf("<%s>%s</%s>", dataField.getName(),
                StringEscapeUtils.escapeXml10(dataField.collapse()),
                dataField.getName());
        }
        aPW.printf("</record>%n");
    }

    public void saveTableResponse(PrintWriter aPW, DataTable aTable,
                                  int aStartRow, int anEndRow)
        throws IOException
    {
        if (aPW == null)
            throw new IOException("PrintWriter output stream is null.");

        aPW.printf("<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n");
        aPW.printf("<response>%n");
        aPW.printf("<status>0</status>%n");
        aPW.printf("<startRow>%d</startRow>%n", aStartRow);
        aPW.printf("<endRow>%d</endRow>%n", anEndRow);

        aPW.printf("<totalRows>%d</totalRows>%n", aTable.rowCount());
        aPW.printf("<data>%n");
        int rowCount = aTable.rowCount();
        for (int row = 0; row < rowCount; row++)
            saveRecord(aPW, aTable.getRowAsBag(row));
        aPW.printf("</data>%n");

        aPW.printf("</response>%n");
    }

    public void saveBagResponse(PrintWriter aPW, DataBag aBag)
        throws IOException
    {
        if (aPW == null)
            throw new IOException("PrintWriter output stream is null.");

        aPW.printf("<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n");
        aPW.printf("<response>%n");
        aPW.printf("<status>0</status>%n");

        aPW.printf("<startRow>0</startRow>%n");
        aPW.printf("<endRow>1</endRow>%n");
        aPW.printf("<totalRows>1</totalRows>%n");
        aPW.printf("<data>%n");
        saveRecord(aPW, aBag);
        aPW.printf("</data>%n");
    }

// http://www.smartclient.com/releases/SmartGWT_Quick_Start_Guide.pdf (Data Sources Page 22)

    private String dsTypeToSGWTString(DataField aField)
    {
        switch (aField.getType())
        {
            case Integer:
            case Long:
                if (aField.isFeatureEqual(Field.FEATURE_SEQUENCE_MANAGEMENT, Field.SQL_INDEX_MANAGEMENT_IMPLICIT))
                    return "sequence";
                else
                    return "integer";
            case Float:
            case Double:
                return "float";
            case Boolean:
                return "boolean";
            case Date:
            case Time:
            case DateTime:
                return "date";
            default:
                return "text";
        }
    }

    private Field.Type dsSGWTStringToType(String aTypeString)
    {
        Field.Type fieldType = Field.Type.Text;

        if (StringUtils.equalsIgnoreCase(aTypeString, "sequence"))
            fieldType = Field.Type.Integer;
        else if (StringUtils.equalsIgnoreCase(aTypeString, "integer"))
            fieldType = Field.Type.Integer;
        else if (StringUtils.equalsIgnoreCase(aTypeString, "float"))
            fieldType = Field.Type.Double;
        else if (StringUtils.equalsIgnoreCase(aTypeString, "boolean"))
            fieldType = Field.Type.Boolean;
        else if (StringUtils.equalsIgnoreCase(aTypeString, "date"))
            fieldType = Field.Type.DateTime;

        return fieldType;
    }

    public void saveRange(PrintWriter aPW, int anIndentAmount, FieldRange aRange)
        throws IOException
    {
        if (aRange.getType() == Field.Type.Text)
        {
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<valueMap>%n");
            IOXML.writeAttrNameValue(aPW, "type", Field.typeToString(aRange.getType()));
            ArrayList<String> rangeItems = aRange.getItems();
            for (String rangeItem : rangeItems)
                IOXML.writeNodeNameValue(aPW, anIndentAmount+1, "value", rangeItem);
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("</valueMap>%n");
        }
    }

    public void save(PrintWriter aPW, int anIndentAmount, DataBag aBag)
        throws IOException
    {
        int fieldCount = aBag.count();
        if (fieldCount > 0)
        {
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<fields>%n");
            DataField dataField;
            for (int i = 0; i < fieldCount; i++)
            {
                dataField = aBag.getByOffset(i);
                IOXML.indentLine(aPW, anIndentAmount+1);
                aPW.printf("<field");
                IOXML.writeAttrNameValue(aPW, "name", dataField.getName());
                IOXML.writeAttrNameValue(aPW, "title", dataField.getTitle());
                IOXML.writeAttrNameValue(aPW, "type", dsTypeToSGWTString(dataField));
                if (dataField.isFeatureTrue(Field.FEATURE_IS_REQUIRED))
                    IOXML.writeAttrNameValue(aPW, "required", StrUtl.STRING_TRUE);
                if (dataField.isFeatureTrue(Field.FEATURE_IS_HIDDEN))
                    IOXML.writeAttrNameValue(aPW, "hidden", true);
                else
                    IOXML.writeAttrNameValue(aPW, "detail", true);

                if (dataField.isFeatureAssigned(Field.FEATURE_STORED_SIZE))
                    IOXML.writeAttrNameValue(aPW, "length", dataField.getFeatureAsInt(Field.FEATURE_STORED_SIZE));
                if (dataField.isFeatureTrue(Field.FEATURE_IS_PRIMARY_KEY))
                    IOXML.writeAttrNameValue(aPW, "primaryKey", StrUtl.STRING_TRUE);
                if (dataField.isRangeAssigned())
                {
                    aPW.printf(">%n");
                    saveRange(aPW, anIndentAmount+2, dataField.getRange());
                }
                else
                    aPW.printf("/>%n");

                if (dataField.isRangeAssigned())
                {
                    IOXML.indentLine(aPW, anIndentAmount+1);
                    aPW.printf("</field>%n");
                }
            }
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("</fields>%n");
        }
    }

// Refer to com.isomorphic.datasource.DSField (Quick Start Guide - Data Sources page 20)

    public void save(String aPathFileName, DSTable aDSTable)
        throws IOException, DSException
    {
        try (PrintWriter printWriter = new PrintWriter(aPathFileName, "UTF-8"))
        {
            IOXML.indentLine(printWriter, 0);
            printWriter.printf("<DataSource");
            IOXML.writeAttrNameValue(printWriter, "ID", aDSTable.getName());
            IOXML.writeAttrNameValue(printWriter, "dataSourceVersion", "1");
            IOXML.writeAttrNameValue(printWriter, "serverType", "generic");
            String serverConstructor = (String) aDSTable.getProperty("serverConstructor");
            if (StringUtils.isNotEmpty(serverConstructor))
                IOXML.writeAttrNameValue(printWriter, "serverConstructor", serverConstructor);
            printWriter.printf(">%n");
            String asDisable = (String) aDSTable.getProperty("nsd_advanced_search_disabled");
            if (StringUtils.isNotEmpty(asDisable))
                printWriter.printf("  <!-- <field name=\"nsd_advanced_search_disabled\" title=\"Disable Advanced Search\" type=\"boolean\" hidden=\"true\"/> -->%n");
            save(printWriter, 1, aDSTable.getCacheBag());
            DataField dataField = new DataField("date_today", StringUtils.EMPTY, new Date());
            IOXML.indentLine(printWriter, 1);
            printWriter.printf("<generatedBy>NS DataSource Generator %s</generatedBy>%n",
                dataField.getValueFormatted(Field.FORMAT_DATE_DEFAULT));
            IOXML.indentLine(printWriter, 0);
            printWriter.printf("</DataSource>%n");
        }
        catch (Exception e)
        {
            throw new IOException("PrintWriter Error: " + e.getMessage());
        }
    }

    public void save(String aPathFileName, DataBag aBag)
        throws IOException, DSException
    {
        try (PrintWriter printWriter = new PrintWriter(aPathFileName, "UTF-8"))
        {
            IOXML.indentLine(printWriter, 0);
            printWriter.printf("<DataSource");
            IOXML.writeAttrNameValue(printWriter, "ID", aBag.getName());
            IOXML.writeAttrNameValue(printWriter, "dataSourceVersion", "1");
            IOXML.writeAttrNameValue(printWriter, "serverType", "generic");
            String serverConstructor = (String) aBag.getProperty("serverConstructor");
            if (StringUtils.isNotEmpty(serverConstructor))
                IOXML.writeAttrNameValue(printWriter, "serverConstructor", serverConstructor);
            printWriter.printf(">%n");
            save(printWriter, 1, aBag);
            DataField dataField = new DataField("date_today", StringUtils.EMPTY, new Date());
            IOXML.indentLine(printWriter, 1);
            printWriter.printf("<generatedBy>NS DataSource Generator %s</generatedBy>%n",
                dataField.getValueFormatted(Field.FORMAT_DATE_DEFAULT));
            IOXML.indentLine(printWriter, 0);
            printWriter.printf("</DataSource>%n");
        }
        catch (Exception e)
        {
            throw new IOException("PrintWriter Error: " + e.getMessage());
        }
    }
}
