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

package com.nridge.core.base.io.xml;

import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.FieldRow;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;

import com.nridge.core.base.std.StrUtl;
import com.nridge.core.base.std.XMLUtl;
import com.nridge.core.base.io.IO;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.*;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Map;

/**
 * The DataTableXML provides a collection of methods that can generate/load
 * an XML representation of a {@link DataTable} object.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataTableXML implements DOMInterface
{
    private int mContextTotal;
    private int mContextStart;
    private int mContextLimit;
    private DataTable mDataTable;
    private boolean mSaveFieldsWithoutValues;

    public DataTableXML()
    {
        mDataTable = new DataTable();
    }

    public DataTableXML(DataTable aDataTable)
    {
        mDataTable = aDataTable;
    }

    public DataTableXML(DataTable aDataTable, int aStart, int aLimit, int aTotal)
    {
        mContextStart = aStart;
        mContextLimit = aLimit;
        mContextTotal = aTotal;
        mDataTable = aDataTable;
    }

    public DataTable getTable()
    {
        return mDataTable;
    }

    public void setContext(int aStart, int aCount, int aSize)
    {
        mContextTotal = aSize;
        mContextStart = aStart;
        mContextLimit = aCount;
    }

    public int getContextTotal()
    {
        return mContextTotal;
    }

    public int getContextStart()
    {
        return mContextStart;
    }

    public int getContextLimit()
    {
        return mContextLimit;
    }

    /**
     * Assigning this to <i>true</i> will ensure all fields are written
     * (regardless of them having values assigned).  Use this method
     * to generate schema files.
     *
     * @param aFlag <i>true</i> or <i>false</i>
     */
    public void setSaveFieldsWithoutValues(boolean aFlag)
    {
        mSaveFieldsWithoutValues = aFlag;
    }

    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the print writer stream wrapped in a tag name specified in the parameter.
     *
     * @param aPW            PrintWriter stream instance.
     * @param aTagName       Tag name.
     * @param anIndentAmount Indentation count.
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void save(PrintWriter aPW, String aTagName, int anIndentAmount)
        throws IOException
    {
        String cellValue;
        DataField dataField;
        int columnCount, rowCount;

        rowCount = mDataTable.rowCount();
        columnCount = mDataTable.columnCount();
        String tagName = StringUtils.remove(aTagName, StrUtl.CHAR_SPACE);
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<%s", tagName);
        IOXML.writeAttrNameValue(aPW, "type", IO.extractType(mDataTable.getClass().getName()));
        IOXML.writeAttrNameValue(aPW, "name", mDataTable.getName());
        IOXML.writeAttrNameValue(aPW, "dimensions", String.format("%d cols x %d rows", columnCount, rowCount));
        IOXML.writeAttrNameValue(aPW, "version", IO.DATATABLE_XML_FORMAT_VERSION);
        for (Map.Entry<String, String> featureEntry : mDataTable.getFeatures().entrySet())
            IOXML.writeAttrNameValue(aPW, featureEntry.getKey(), featureEntry.getValue());
        aPW.printf(">%n");

        if ((mContextTotal != 0) || (mContextLimit != 0))
        {
            IOXML.indentLine(aPW, anIndentAmount + 2);
            aPW.printf("<Context");
            IOXML.writeAttrNameValue(aPW, "start", mContextStart);
            IOXML.writeAttrNameValue(aPW, "limit", mContextLimit);
            IOXML.writeAttrNameValue(aPW, "total", mContextTotal);
            aPW.printf("/>%n");
        }
        DataBag dataBag = new DataBag(mDataTable.getColumnBag());
        if (mSaveFieldsWithoutValues)
            dataBag.setAssignedFlagAll(true);
        DataBagXML dataBagXML = new DataBagXML(dataBag);
        dataBagXML.save(aPW, "Columns", anIndentAmount + 2);
        if (rowCount > 0)
        {
            IOXML.indentLine(aPW, anIndentAmount + 2);
            aPW.printf("<Rows");
            IOXML.writeAttrNameValue(aPW, "count", rowCount);
            aPW.printf(">%n");
            for (int row = 0; row < rowCount; row++)
            {
                IOXML.indentLine(aPW, anIndentAmount + 3);
                aPW.printf("<Row>%n");
                IOXML.indentLine(aPW, anIndentAmount + 4);
                for (int col = 0; col < columnCount; col++)
                {
                    dataField = mDataTable.getFieldByRowCol(row, col);
                    cellValue = dataField.collapse();
                    if (StringUtils.isEmpty(cellValue))
                        aPW.printf("<C/>");
                    else
                        aPW.printf("<C>%s</C>", StringEscapeUtils.escapeXml10(cellValue));
                }
                aPW.printf("%n");
                IOXML.indentLine(aPW, anIndentAmount + 3);
                aPW.printf("</Row>%n");
            }
            IOXML.indentLine(aPW, anIndentAmount + 2);
            aPW.printf("</Rows>%n");
        }
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("</%s>%n", tagName);
    }

    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the print writer stream specified as a parameter.
     *
     * @param aPW PrintWriter stream instance.
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void save(PrintWriter aPW)
        throws IOException
    {
        save(aPW, "DataTable", 0);
    }

    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the path/file name specified as a parameter.
     *
     * @param aPathFileName Absolute file name.
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void save(String aPathFileName)
        throws IOException
    {
        try (PrintWriter printWriter = new PrintWriter(aPathFileName, StrUtl.CHARSET_UTF_8))
        {
            save(printWriter);
        }
    }

    private void loadRow(Element anElement)
        throws IOException
    {
        Node nodeItem;
        String mvDelimiter;
        DataField dataField;
        String nodeName, nodeValue;

        int columnOffset = 0;
        FieldRow fieldRow = mDataTable.newRow();
        int columnCount = mDataTable.columnCount();

        NodeList nodeList = anElement.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            nodeItem = nodeList.item(i);

            if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
                continue;

            nodeName = nodeItem.getNodeName();
            if (nodeName.equalsIgnoreCase("C"))
            {
                nodeValue = XMLUtl.getNodeStrValue(nodeItem);
                if ((StringUtils.isNotEmpty(nodeValue)) && (columnOffset < columnCount))
                {
                    dataField = mDataTable.getColumn(columnOffset);
                    if (dataField.isMultiValue())
                    {
                        mvDelimiter = dataField.getFeature(Field.FEATURE_MV_DELIMITER);
                        if (StringUtils.isNotEmpty(mvDelimiter))
                            fieldRow.setValues(columnOffset, StrUtl.expandToList(nodeValue, mvDelimiter.charAt(0)));
                        else
                            fieldRow.setValues(columnOffset, StrUtl.expandToList(nodeValue, StrUtl.CHAR_PIPE));
                    }
                    else
                        fieldRow.setValue(columnOffset, nodeValue);
                }
                columnOffset++;
            }
        }
        mDataTable.addRow(fieldRow);
    }

    private void loadRows(Element anElement)
        throws IOException
    {
        Node nodeItem;
        String nodeName;
        Element nodeElement;

        NodeList nodeList = anElement.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            nodeItem = nodeList.item(i);

            if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
                continue;

            nodeName = nodeItem.getNodeName();
            if (nodeName.equalsIgnoreCase("Row"))
            {
                nodeElement = (Element) nodeItem;
                loadRow(nodeElement);
            }
        }
    }

    /**
     * Parses an XML DOM element and loads it into a bag/table.
     *
     * @param anElement DOM element.
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void load(Element anElement)
        throws IOException
    {
        Node nodeItem;
        Attr nodeAttr;
        Element nodeElement;
        String nodeName, nodeValue, attrValue;

        attrValue = anElement.getAttribute("name");
        if (StringUtils.isNotEmpty(attrValue))
            mDataTable.setName(attrValue);

        NamedNodeMap namedNodeMap = anElement.getAttributes();
        int attrCount = namedNodeMap.getLength();
        for (int attrOffset = 0; attrOffset < attrCount; attrOffset++)
        {
            nodeAttr = (Attr) namedNodeMap.item(attrOffset);
            nodeName = nodeAttr.getNodeName();
            nodeValue = nodeAttr.getNodeValue();

            if (StringUtils.isNotEmpty(nodeValue))
            {
                if (! StringUtils.equalsIgnoreCase(nodeName, "name"))
                    mDataTable.addFeature(nodeName, nodeValue);
            }
        }

        NodeList nodeList = anElement.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            nodeItem = nodeList.item(i);

            if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
                continue;

            nodeName = nodeItem.getNodeName();
            if (nodeName.equalsIgnoreCase("Context"))
            {
                nodeElement = (Element) nodeItem;
                attrValue = nodeElement.getAttribute("start");
                if (StringUtils.isNumeric(attrValue))
                    mContextStart = Integer.parseInt(attrValue);
                attrValue = nodeElement.getAttribute("limit");
                if (StringUtils.isNumeric(attrValue))
                    mContextLimit = Integer.parseInt(attrValue);
                attrValue = nodeElement.getAttribute("total");
                if (StringUtils.isNumeric(attrValue))
                    mContextTotal = Integer.parseInt(attrValue);
            }
            else if (nodeName.equalsIgnoreCase("Columns"))
            {
                nodeElement = (Element) nodeItem;
                DataBagXML dataBagXML = new DataBagXML();
                dataBagXML.load(nodeElement);
                DataBag dataBag = dataBagXML.getBag();
                dataBag.setName(mDataTable.getName());
                mDataTable = new DataTable(dataBag);
            }
            else if (nodeName.equalsIgnoreCase("Rows"))
            {
                nodeElement = (Element) nodeItem;
                loadRows(nodeElement);
            }
        }
    }

    /**
     * Parses an XML DOM element and loads it into a bag/table.
     *
     * @param anIS Input stream.
     * @throws java.io.IOException                            I/O related exception.
     * @throws javax.xml.parsers.ParserConfigurationException XML parser related exception.
     * @throws org.xml.sax.SAXException                       XML parser related exception.
     */
    @Override
    public void load(InputStream anIS)
        throws ParserConfigurationException, IOException, SAXException, TransformerException
    {
        DocumentBuilderFactory docBldFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docBldFactory.newDocumentBuilder();
        InputSource inputSource = new InputSource(anIS);
        Document xmlDocument = docBuilder.parse(inputSource);
        xmlDocument.getDocumentElement().normalize();

        load(xmlDocument.getDocumentElement());
    }

    /**
     * Parses an XML file identified by the path/file name parameter
     * and loads it into a bag/table.
     *
     * @param aPathFileName Absolute file name.
     * @throws java.io.IOException                            I/O related exception.
     * @throws javax.xml.parsers.ParserConfigurationException XML parser related exception.
     * @throws org.xml.sax.SAXException                       XML parser related exception.
     */
    @Override
    public void load(String aPathFileName)
        throws IOException, ParserConfigurationException, SAXException
    {
        File xmlFile = new File(aPathFileName);
        if (! xmlFile.exists())
            throw new IOException(aPathFileName + ": Does not exist.");

        DocumentBuilderFactory docBldFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docBldFactory.newDocumentBuilder();
        Document xmlDocument = docBuilder.parse(new File(aPathFileName));
        xmlDocument.getDocumentElement().normalize();

        load(xmlDocument.getDocumentElement());
    }
}
