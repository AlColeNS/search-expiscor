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

import com.nridge.core.base.doc.Document;
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.io.IO;
import com.nridge.core.base.std.StrUtl;
import com.nridge.core.base.std.XMLUtl;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * The DocumentXML provides a collection of methods that can generate/load
 * an XML representation of a {@link com.nridge.core.base.doc.Document} object.
 *
 * @since 1.0
 * @author Al Cole
 */
public class DocumentXML implements DOMInterface
{
    private boolean mIsSimple;
    private Document mDocument;
    private boolean mSaveFieldsWithoutValues;

    /**
     * Default constructor.
     */
    public DocumentXML()
    {
        mDocument = new Document(IO.XML_DOCUMENT_NODE_NAME);
    }

    /**
     * Constructor accepts a document as a parameter.
     *
     * @param aDocument Document instance.
     */
    public DocumentXML(Document aDocument)
    {
        mDocument = aDocument;
    }

    /**
     * Assigns a {@link Document}.  This should be done via
     * a constructor or this method prior to a save method
     * being invoked.
     *
     * @param aDocument Document instance.
     */
    public void setDocument(Document aDocument)
    {
        mDocument = aDocument;
    }

    /**
     * Returns a reference to the {@link Document} that this
     * class is managing.
     *
     * @return Document instance.
     */
    public Document getDocument()
    {
        return mDocument;
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
     * Saves the previous assigned document (e.g. via constructor or set method)
     * to the print writer stream wrapped in a tag name specified in the parameter.
     *
     * @param aPW            PrintWriter stream instance.
     * @param aParentTag     Parent tag name.
     * @param aDocument      Document instance.
     * @param anIndentAmount Indentation count.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(PrintWriter aPW, String aParentTag,
                     Document aDocument, int anIndentAmount)
        throws IOException
    {
        RelationshipXML relationshipXML;
        String docType = StringUtils.remove(aDocument.getType(), StrUtl.CHAR_SPACE);
        String parentTag = StringUtils.remove(aParentTag, StrUtl.CHAR_SPACE);

        IOXML.indentLine(aPW, anIndentAmount);
        if (StringUtils.isNotEmpty(aParentTag))
            aPW.printf("<%s-%s", parentTag, docType);
        else
            aPW.printf("<%s", docType);
        if (! mIsSimple)
        {
            IOXML.writeAttrNameValue(aPW, "type", aDocument.getType());
            IOXML.writeAttrNameValue(aPW, "name", aDocument.getName());
            IOXML.writeAttrNameValue(aPW, "title", aDocument.getTitle());
            IOXML.writeAttrNameValue(aPW, "schemaVersion", aDocument.getSchemaVersion());
        }
        for (Map.Entry<String, String> featureEntry : aDocument.getFeatures().entrySet())
            IOXML.writeAttrNameValue(aPW, featureEntry.getKey(), featureEntry.getValue());
        aPW.printf(">%n");
        DataTableXML dataTableXML = new DataTableXML(aDocument.getTable());
        dataTableXML.setSaveFieldsWithoutValues(mSaveFieldsWithoutValues);
        dataTableXML.save(aPW, IO.XML_TABLE_NODE_NAME, anIndentAmount + 1);
        if (aDocument.relationshipCount() > 0)
        {
            ArrayList<Relationship> docRelationships = aDocument.getRelationships();
            IOXML.indentLine(aPW, anIndentAmount + 1);
            aPW.printf("<%s>%n", IO.XML_RELATED_NODE_NAME);
            for (Relationship relationship : docRelationships)
            {
                relationshipXML = new RelationshipXML(relationship);
                relationshipXML.setSaveFieldsWithoutValues(mSaveFieldsWithoutValues);
                relationshipXML.save(aPW, IO.XML_RELATIONSHIP_NODE_NAME, anIndentAmount + 2);
            }
            IOXML.indentLine(aPW, anIndentAmount + 1);
            aPW.printf("</%s>%n", IO.XML_RELATED_NODE_NAME);
        }
        HashMap<String, String> docACL = aDocument.getACL();
        if (docACL.size() > 0)
        {
            IOXML.indentLine(aPW, anIndentAmount + 1);
            aPW.printf("<%s>%n", IO.XML_ACL_NODE_NAME);
            for (Map.Entry<String,String> aclEntry : docACL.entrySet())
            {
                IOXML.indentLine(aPW, anIndentAmount + 2);
                aPW.printf("<%s", IO.XML_ACE_NODE_NAME);
                IOXML.writeAttrNameValue(aPW, "name", aclEntry.getKey());
                aPW.printf(">%s</%s>%n", StringEscapeUtils.escapeXml10(aclEntry.getValue()), IO.XML_ACE_NODE_NAME);
            }
            IOXML.indentLine(aPW, anIndentAmount + 1);
            aPW.printf("</%s>%n", IO.XML_ACL_NODE_NAME);
        }
        IOXML.indentLine(aPW, anIndentAmount);
        if (StringUtils.isNotEmpty(aParentTag))
            aPW.printf("</%s-%s>%n", parentTag, docType);
        else
            aPW.printf("</%s>%n", docType);
    }

    /**
     * Saves the previous assigned document (e.g. via constructor or set method)
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
        save(aPW, aTagName, mDocument, anIndentAmount);
    }

    /**
     * Saves the previous assigned document (e.g. via constructor or set method)
     * to the print writer stream specified as a parameter.
     *
     * @param aPW PrintWriter stream instance.
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void save(PrintWriter aPW)
        throws IOException
    {
        save(aPW, StringUtils.EMPTY, mDocument, 0);
    }

    /**
     * Saves the previous assigned document (e.g. via constructor or set method)
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

    private void loadRelated(Document aDocument, Element anElement)
        throws IOException
    {
        Node nodeItem;
        String nodeName;
        Element nodeElement;
        RelationshipXML relationshipXML;

        ArrayList<Relationship> docRelationships = aDocument.getRelationships();
        NodeList nodeList = anElement.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            nodeItem = nodeList.item(i);

            if (nodeItem.getNodeType() == Node.ELEMENT_NODE)
            {
                nodeName = nodeItem.getNodeName();
                if (StringUtils.equalsIgnoreCase(nodeName, IO.XML_RELATIONSHIP_NODE_NAME))
                {
                    nodeElement = (Element) nodeItem;
                    relationshipXML = new RelationshipXML();
                    relationshipXML.load(nodeElement);
                    docRelationships.add(relationshipXML.getRelationship());
                }
            }
        }
    }

    private void loadACL(Document aDocument, Element anElement)
        throws IOException
    {
        Node nodeItem;
        Element nodeElement;
        String nodeName, aceName, aceValue;

        HashMap<String, String> docACL = aDocument.getACL();
        NodeList nodeList = anElement.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            nodeItem = nodeList.item(i);

            if (nodeItem.getNodeType() == Node.ELEMENT_NODE)
            {
                nodeName = nodeItem.getNodeName();
                if (StringUtils.equalsIgnoreCase(nodeName, IO.XML_ACE_NODE_NAME))
                {
                    nodeElement = (Element) nodeItem;
                    aceName = nodeElement.getAttribute("name");
                    aceValue = XMLUtl.getNodeStrValue(nodeItem);
                    docACL.put(aceName, aceValue);
                }
            }
        }
    }

    private Document loadDocument(Element anElement)
        throws IOException
    {
        Node nodeItem;
        Attr nodeAttr;
        Document document;
        Element nodeElement;
        String nodeName, nodeValue;

        String docName = anElement.getAttribute("name");
        String typeName = anElement.getAttribute("type");
        String docTitle = anElement.getAttribute("title");
        String schemaVersion = anElement.getAttribute("schemaVersion");
        if ((StringUtils.isNotEmpty(typeName)) && (StringUtils.isNotEmpty(schemaVersion)))
            document = new Document(typeName);
        else
            document = new Document("Unknown");
        if (StringUtils.isNotEmpty(docName))
            document.setName(docName);
        if (StringUtils.isNotEmpty(docTitle))
            document.setName(docTitle);

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
                    (StringUtils.equalsIgnoreCase(nodeName, "title")) ||
                    (StringUtils.equalsIgnoreCase(nodeName, "schemaVersion")))
                    continue;
                else
                    document.addFeature(nodeName, nodeValue);
            }
        }

        NodeList nodeList = anElement.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            nodeItem = nodeList.item(i);

            if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
                continue;

            nodeName = nodeItem.getNodeName();
            if (StringUtils.equalsIgnoreCase(nodeName, IO.XML_TABLE_NODE_NAME))
            {
                nodeElement = (Element) nodeItem;
                DataTableXML dataTableXML = new DataTableXML();
                dataTableXML.load(nodeElement);
                document.setTable(dataTableXML.getTable());
            }
            else if (StringUtils.equalsIgnoreCase(nodeName, IO.XML_RELATED_NODE_NAME))
            {
                nodeElement = (Element) nodeItem;
                loadRelated(document, nodeElement);
            }
            else if (StringUtils.equalsIgnoreCase(nodeName, IO.XML_ACL_NODE_NAME))
            {
                nodeElement = (Element) nodeItem;
                loadACL(document, nodeElement);
            }
        }

        return document;
    }

    /**
     * Parses an XML DOM element and loads it into a document.
     *
     * @param anElement DOM element.
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void load(Element anElement)
        throws IOException
    {
        mDocument = loadDocument(anElement);
    }

    /**
     * Parses an XML DOM element and loads it into a document.
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
        org.w3c.dom.Document xmlDocument = docBuilder.parse(inputSource);
        xmlDocument.getDocumentElement().normalize();

        load(xmlDocument.getDocumentElement());
    }

    /**
     * Parses an XML file identified by the path/file name parameter
     * and loads it into a document.
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
        org.w3c.dom.Document xmlDocument = docBuilder.parse(new File(aPathFileName));
        xmlDocument.getDocumentElement().normalize();

        load(xmlDocument.getDocumentElement());
    }
}
