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

import com.nridge.core.base.ds.DSCriteria;

import com.nridge.core.base.ds.DSCriterion;
import com.nridge.core.base.ds.DSCriterionEntry;
import com.nridge.core.base.field.Field;
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
import java.util.ArrayList;
import java.util.Map;

/**
 * The DSCriteriaXML provides a collection of methods that can generate/load
 * an XML representation of a {@link DSCriteria} object.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DSCriteriaXML implements DOMInterface
{
    private DSCriteria mDSCriteria;
    private DataFieldXML mDataFieldXML;

    /**
     * Default constructor.
     */
    public DSCriteriaXML()
    {
        mDSCriteria = new DSCriteria();
        mDataFieldXML = new DataFieldXML();
    }

    /**
     * Constructor that identifies a criteria prior to a save operation.
     *
     * @param aCriteria Data source criteria.
     */
    public DSCriteriaXML(DSCriteria aCriteria)
    {
        mDSCriteria = aCriteria;
        mDataFieldXML = new DataFieldXML();
    }

    /**
     * Returns a reference to the {@link DSCriteria} being managed by
     * this class.
     *
     * @return Data source criteria.
     */
    public DSCriteria getCriteria()
    {
        return mDSCriteria;
    }

    /**
     * Saves the previous assigned criteria (e.g. via constructor or set method)
     * to the print writer stream wrapped in a tag name specified in the parameter.
     *
     * @param aPW            PrintWriter stream instance.
     * @param aTagName       Tag name.
     * @param anIndentAmount Indentation count.
     *
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void save(PrintWriter aPW, String aTagName, int anIndentAmount)
        throws IOException
    {
        DataField dataField;
        DSCriterion dsCriterion;

        String tagName = StringUtils.remove(aTagName, StrUtl.CHAR_SPACE);
        ArrayList<DSCriterionEntry> dsCriterionEntries = mDSCriteria.getCriterionEntries();
        int ceCount = dsCriterionEntries.size();

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<%s", tagName);
        IOXML.writeAttrNameValue(aPW, "type", IO.extractType(mDSCriteria.getClass().getName()));
        IOXML.writeAttrNameValue(aPW, "name", mDSCriteria.getName());
        IOXML.writeAttrNameValue(aPW, "version", IO.CRITERIA_XML_FORMAT_VERSION);
        IOXML.writeAttrNameValue(aPW, "operator", Field.operatorToString(Field.Operator.AND));
        IOXML.writeAttrNameValue(aPW, "count", ceCount);
        for (Map.Entry<String, String> featureEntry : mDSCriteria.getFeatures().entrySet())
            IOXML.writeAttrNameValue(aPW, featureEntry.getKey(), featureEntry.getValue());
        aPW.printf(">%n");

        if (ceCount > 0)
        {
            for (DSCriterionEntry ce : dsCriterionEntries)
            {
                dsCriterion = ce.getCriterion();

                dataField = new DataField(dsCriterion.getField());
                dataField.addFeature("operator", Field.operatorToString(ce.getLogicalOperator()));
                mDataFieldXML.saveNode(aPW, anIndentAmount+1);
                mDataFieldXML.saveAttr(aPW, dataField);
                mDataFieldXML.saveValue(aPW, dataField, anIndentAmount+1);
            }
        }

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("</%s>%n", tagName);
    }

    /**
     * Saves the previous assigned criteria (e.g. via constructor or set method)
     * to the print writer stream specified as a parameter.
     *
     * @param aPW PrintWriter stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void save(PrintWriter aPW)
        throws IOException
    {
        save(aPW, IO.XML_CRITERIA_NODE_NAME, 0);
    }

    /**
     * Saves the previous assigned criteria (e.g. via constructor or set method)
     * to the path/file name specified as a parameter.
     *
     * @param aPathFileName Absolute file name.
     *
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
     * Parses an XML DOM element and loads it into a criteria.
     *
     * @param anElement DOM element.
     *
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
        String nodeName, nodeValue, logicalOperator;

        String className = mDSCriteria.getClass().getName();
        String attrValue = anElement.getAttribute("type");
        if ((StringUtils.isNotEmpty(attrValue)) &&
            (! IO.isTypesEqual(attrValue, className)))
            throw new IOException("Unsupported type: " + attrValue);

        attrValue = anElement.getAttribute("name");
        if (StringUtils.isNotEmpty(attrValue))
            mDSCriteria.setName(attrValue);

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
                    (StringUtils.equalsIgnoreCase(nodeName, "type")))
                    continue;
                else
                    mDSCriteria.addFeature(nodeName, nodeValue);
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
                {
                    logicalOperator = dataField.getFeature("operator");
                    if (StringUtils.isEmpty(logicalOperator))
                        logicalOperator = Field.operatorToString(Field.Operator.EQUAL);
                    mDSCriteria.add(dataField, Field.stringToOperator(logicalOperator));
                }
            }
        }
    }

    /**
     * Parses an XML DOM element and loads it into a bag/table.
     *
     * @param anIS Input stream.
     *
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
     *
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
