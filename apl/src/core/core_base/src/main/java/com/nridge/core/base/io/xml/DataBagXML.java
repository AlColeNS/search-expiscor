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

import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.io.IO;
import com.nridge.core.base.std.StrUtl;
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
 * The DataBagXML class provides XML helper methods.
 */
public class DataBagXML implements DOMInterface
{
    private DataBag mBag;
    private boolean mIsSimple;
    private DataFieldXML mDataFieldXML;

    /**
     * Default constructor.
     */
    public DataBagXML()
    {
        mBag = new DataBag();
        mDataFieldXML = new DataFieldXML();
    }

    /**
     * Constructor accepts a bag as a parameter.
     *
     * @param aBag Bag instance.
     */
    public DataBagXML(DataBag aBag)
    {
        setBag(aBag);
        mDataFieldXML = new DataFieldXML();
    }

    /**
     * Assigns the bag parameter to the internally managed bag instance.
     *
     * @param aBag Bag instance.
     */
    public void setBag(DataBag aBag)
    {
        mBag = aBag;
    }

    /**
     * Returns a reference to the internally managed bag instance.
     *
     * @return Bag instance.
     */
    public DataBag getBag()
    {
        return mBag;
    }

    /**
     * Assigns a simple format flag.
     *
     * @param anIsSimple Is output format simple?
     */
    public void setIsSimpleFlag(boolean anIsSimple)
    {
        mIsSimple = anIsSimple;
    }

    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the print writer stream wrapped in a tag name specified in the parameter.
     *
     * @param aPW PrintWriter stream instance.
     * @param aTagName Tag name.
     * @param anIndentAmount Indentation count.
     *
     * @throws IOException I/O related exception.
     */
    public void save(PrintWriter aPW, String aTagName, int anIndentAmount)
        throws IOException
    {
        IOXML.indentLine(aPW, anIndentAmount);
        String tagName = StringUtils.remove(aTagName, StrUtl.CHAR_SPACE);
        aPW.printf("<%s", tagName);
        if (! mIsSimple)
        {
            IOXML.writeAttrNameValue(aPW, "type", IO.extractType(mBag.getClass().getName()));
            IOXML.writeAttrNameValue(aPW, "name", mBag.getName());
            IOXML.writeAttrNameValue(aPW, "title", mBag.getTitle());
            IOXML.writeAttrNameValue(aPW, "count", mBag.count());
            IOXML.writeAttrNameValue(aPW, "version", IO.DATABAG_XML_FORMAT_VERSION);
        }
        for (Map.Entry<String, String> featureEntry : mBag.getFeatures().entrySet())
            IOXML.writeAttrNameValue(aPW, featureEntry.getKey(), featureEntry.getValue());
        aPW.printf(">%n");

        for (DataField dataField : mBag.getFields())
        {
            if (dataField.isAssigned())
            {
                mDataFieldXML.saveNode(aPW, anIndentAmount + 1);
                mDataFieldXML.saveAttr(aPW, dataField);
                mDataFieldXML.saveValue(aPW, dataField, anIndentAmount + 1);
            }
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
        save(aPW, "DataBag", 0);
    }

    /**
     * Saves the previous assigned bag/table (e.g. via constructor or set method)
     * to the path/file name specified as a parameter.
     *
     * @param aPathFileName Absolute file name.
     * @param aTagName Tag name.
     * @throws java.io.IOException I/O related exception.
     */
    public void save(String aPathFileName, String aTagName)
        throws IOException
    {
        try (PrintWriter printWriter = new PrintWriter(aPathFileName, StrUtl.CHARSET_UTF_8))
        {
            save(printWriter, aTagName, 0);
        }
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
        DataField dataField;
        String nodeName, nodeValue;

        String className = mBag.getClass().getName();
        String attrValue = anElement.getAttribute("type");
        if ((StringUtils.isNotEmpty(attrValue)) &&
            (! IO.isTypesEqual(attrValue, className)))
            throw new IOException("Unsupported type: " + attrValue);

        attrValue = anElement.getAttribute("name");
        if (StringUtils.isNotEmpty(attrValue))
            mBag.setName(attrValue);
        attrValue = anElement.getAttribute("title");
        if (StringUtils.isNotEmpty(attrValue))
            mBag.setTitle(attrValue);

        NamedNodeMap namedNodeMap = anElement.getAttributes();
        int attrCount = namedNodeMap.getLength();
        for (int attrOffset = 0; attrOffset < attrCount; attrOffset++)
        {
            nodeAttr = (Attr) namedNodeMap.item(attrOffset);
            nodeName = nodeAttr.getNodeName();
            nodeValue = nodeAttr.getNodeValue();

            if (StringUtils.isNotEmpty(nodeValue))
            {
                if ((StringUtils.equalsIgnoreCase(nodeName, "name")) ||
                    (StringUtils.equalsIgnoreCase(nodeName, "type")) ||
                    (StringUtils.equalsIgnoreCase(nodeName, "count")) ||
                    (StringUtils.equalsIgnoreCase(nodeName, "title")) ||
                    (StringUtils.equalsIgnoreCase(nodeName, "version")))
                    continue;
                else
                    mBag.addFeature(nodeName, nodeValue);
            }
        }

        NodeList nodeList = anElement.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            nodeItem = nodeList.item(i);

            if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
                continue;

            nodeName = nodeItem.getNodeName();
            if (nodeName.equalsIgnoreCase(IO.XML_FIELD_NODE_NAME))
            {
                nodeElement = (Element) nodeItem;
                dataField = mDataFieldXML.load(nodeElement);
                if (dataField != null)
                    mBag.add(dataField);
            }
        }
    }

    /**
     * Parses an XML DOM element and loads it into a bag/table.
     *
     * @param anIS Input stream.
     *
     * @throws java.io.IOException I/O related exception.
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
