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

import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.field.data.DataBag;
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
 * The RelationshipXML provides a collection of methods that can generate/load
 * an XML representation of a {@link Relationship} object.
 *
 * @author Al Cole
 * @since 1.0
 */
public class RelationshipXML implements DOMInterface
{
    private Relationship mRelationship;
    private boolean mSaveFieldsWithoutValues;

    /**
     * Default constructor.
     */
    public RelationshipXML()
    {
        mRelationship = new Relationship();
    }

    /**
     * Constructor that identifies a relationship prior to a save operation.
     *
     * @param aRelationship Relationship instance.
     */
    public RelationshipXML(Relationship aRelationship)
    {
        mRelationship = aRelationship;
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
     * Returns a reference to the {@link Relationship} being managed by
     * this class.
     *
     * @return Relationship instance.
     */
    public Relationship getRelationship()
    {
        return mRelationship;
    }

    /**
     * Saves the previous assigned relationship (e.g. via constructor or set method)
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
        DocumentXML documentXML;

        IOXML.indentLine(aPW, anIndentAmount);
        String typeName = mRelationship.getType();
        String tagName = StringUtils.remove(aTagName, StrUtl.CHAR_SPACE);
        aPW.printf("<%s", tagName);
        IOXML.writeAttrNameValue(aPW, "type", typeName);
        for (Map.Entry<String, String> featureEntry : mRelationship.getFeatures().entrySet())
            IOXML.writeAttrNameValue(aPW, featureEntry.getKey(), featureEntry.getValue());
        aPW.printf(">%n");
        DataBag dataBag = mRelationship.getBag();
        if (dataBag.assignedCount() > 0)
        {
            DataBagXML dataBagXML = new DataBagXML(dataBag);
            dataBagXML.setBag(dataBag);
            dataBagXML.save(aPW, IO.XML_PROPERTIES_NODE_NAME, anIndentAmount+1);
        }
        for (com.nridge.core.base.doc.Document document : mRelationship.getDocuments())
        {
            documentXML = new DocumentXML(document);
            documentXML.setSaveFieldsWithoutValues(mSaveFieldsWithoutValues);
            documentXML.save(aPW, IO.XML_DOCUMENT_NODE_NAME, document, anIndentAmount+1);
        }
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("</%s>%n", tagName);
    }

    /**
     * Saves the previous assigned relationship (e.g. via constructor or set method)
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
        save(aPW, IO.XML_RELATIONSHIP_NODE_NAME, 0);
    }

    /**
     * Saves the previous assigned relationship (e.g. via constructor or set method)
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
     * Parses an XML DOM element and loads it into a relationship.
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
        DataBagXML dataBagXML;
        DocumentXML documentXML;
        String nodeName, nodeValue;

        mRelationship.reset();
        String attrValue = anElement.getAttribute("type");
        if (StringUtils.isEmpty(attrValue))
            throw new IOException("Relationship is missing type attribute.");
        mRelationship.setType(attrValue);

        NamedNodeMap namedNodeMap = anElement.getAttributes();
        int attrCount = namedNodeMap.getLength();
        for (int attrOffset = 0; attrOffset < attrCount; attrOffset++)
        {
            nodeAttr = (Attr) namedNodeMap.item(attrOffset);
            nodeName = nodeAttr.getNodeName();
            nodeValue = nodeAttr.getNodeValue();

            if (StringUtils.isNotEmpty(nodeValue))
            {
                if ((! StringUtils.equalsIgnoreCase(nodeName, "type")))
                    mRelationship.addFeature(nodeName, nodeValue);
            }
        }
        NodeList nodeList = anElement.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            nodeItem = nodeList.item(i);

            if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
                continue;

            nodeName = nodeItem.getNodeName();
            if (nodeName.equalsIgnoreCase(IO.XML_PROPERTIES_NODE_NAME))
            {
                nodeElement = (Element) nodeItem;
                dataBagXML = new DataBagXML();
                dataBagXML.load(nodeElement);
                mRelationship.setBag(dataBagXML.getBag());
            }
            else
            {
                nodeElement = (Element) nodeItem;
                documentXML = new DocumentXML();
                documentXML.load(nodeElement);
                mRelationship.add(documentXML.getDocument());
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
