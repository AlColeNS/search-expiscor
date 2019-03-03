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

import com.nridge.core.base.doc.Doc;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataIntegerField;
import com.nridge.core.base.io.DocReplyInterface;
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
import java.io.*;
import java.util.ArrayList;
import java.util.Map;

/**
 * The DocumentReplyXML class provides XML helper methods.
 */
public class DocumentReplyXML implements DOMInterface, DocReplyInterface
{
    private DataField mField;
    private boolean mIsSimple;
    private ArrayList<Document> mDocumentList;

    /**
     * Default constructor.
     */
    public DocumentReplyXML()
    {
        mField = new DataIntegerField("status_code", "Status Code");
        mField.setValue(Doc.STATUS_CODE_SUCCESS);
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Default constructor that accepts a status code.
     *
     * @param aCode Status code.
     */
    public DocumentReplyXML(int aCode)
    {
        mField = new DataIntegerField("status_code", "Status Code");
        mField.setValue(aCode);
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Default constructor that accepts a status code.
     *
     * @param aCode Status code.
     * @param aSessionId Session identifier.
     */
    public DocumentReplyXML(int aCode, String aSessionId)
    {
        mField = new DataIntegerField("status_code", "Status Code");
        mField.setValue(aCode);
        mField.addFeature(Doc.FEATURE_REPLY_SESSION_ID, aSessionId);
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Constructor accepts a status code and a list of document instances
     * as a parameter.
     *
     * @param aCode Status code.
     * @param aDocumentList A list of document instances.
     */
    public DocumentReplyXML(int aCode, ArrayList<Document> aDocumentList)
    {
        mField = new DataIntegerField("status_code", "Status Code");
        mField.setValue(aCode);
        mDocumentList = aDocumentList;
    }

    /**
     * Assigns a reply field containing status and other desired features.
     *
     * @param aField Operation field.
     */
    public void setField(DataField aField)
    {
        mField = aField;
    }

    /**
     * Returns a reply field.
     *
     * @return Data field instance.
     */
    public DataField getField()
    {
        return mField;
    }

    /**
     * Assigns standard message features to the reply.
     *
     * @param aMessage Reply message.
     * @param aDetail Reply message detail.
     */
    public void setFeatures(String aMessage, String aDetail)
    {
        if (StringUtils.isNotEmpty(aMessage))
            mField.addFeature(Doc.FEATURE_REPLY_MESSAGE, aMessage);
        if (StringUtils.isNotEmpty(aDetail))
            mField.addFeature(Doc.FEATURE_REPLY_DETAIL, aDetail);
    }

    /**
     * Assigns standard account features to the reply.
     *
     * @param aSessionId Session identifier.
     */
    public void setFeatures(String aSessionId)
    {
        mField.addFeature(Doc.FEATURE_OP_SESSION, aSessionId);
    }

    /**
     * Assigns the document list parameter to the internally managed list instance.
     *
     * @param aDocumentList A list of document instances.
     */
    public void setDocumentList(ArrayList<Document> aDocumentList)
    {
        mDocumentList = aDocumentList;
    }

    /**
     * Returns a reference to the internally managed list of document instances.
     *
     * @return List of document instances.
     */
    public ArrayList<Document> getDocumentList()
    {
        return mDocumentList;
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
     * Saves the previous assigned bag list (e.g. via constructor or set method)
     * to the print writer stream wrapped in a tag name specified in the parameter.
     *
     * @param aPW PrintWriter stream instance.
     * @param aTagName Tag name.
     * @param anIndentAmount Indentation count.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(PrintWriter aPW, String aTagName, int anIndentAmount)
        throws IOException
    {
        DocumentXML documentXML;

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<%s", IO.XML_REPLY_NODE_NAME);
        IOXML.writeAttrNameValue(aPW, Doc.FEATURE_OP_STATUS_CODE, mField.getValue());
        int docCount = mDocumentList.size();
        if (docCount > 0)
            IOXML.writeAttrNameValue(aPW, "count", mDocumentList.size());
        for (Map.Entry<String, String> featureEntry : mField.getFeatures().entrySet())
            IOXML.writeAttrNameValue(aPW, featureEntry.getKey(), featureEntry.getValue());
        aPW.printf(">%n");

        for (Document document : mDocumentList)
        {
            documentXML = new DocumentXML(document);
            documentXML.setIsSimpleFlag(mIsSimple);
            documentXML.setSaveFieldsWithoutValues(true);
            documentXML.save(aPW, aTagName, anIndentAmount+1);
        }

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("</%s>%n", IO.XML_REPLY_NODE_NAME);
    }

    /**
     * Saves the previous assigned bag list (e.g. via constructor or set method)
     * to the print writer stream specified as a parameter.
     *
     * @param aPW PrintWriter stream instance.
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void save(PrintWriter aPW)
        throws IOException
    {
        save(aPW, StringUtils.EMPTY, 0);
    }

    /**
     * Saves the previous assigned document list (e.g. via constructor or set method)
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
     * Saves the previous assigned document list (e.g. via constructor or set method)
     * to a string and returns it.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public String saveAsString()
        throws IOException
    {
        StringWriter stringWriter = new StringWriter();
        try (PrintWriter printWriter = new PrintWriter(stringWriter))
        {
            save(printWriter);
        }

        return stringWriter.toString();
    }

    /**
     * Parses an XML DOM element and loads it into a document list.
     *
     * @param anElement DOM element.
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void load(Element anElement)
        throws IOException
    {
        Attr nodeAttr;
        Node nodeItem;
        Document document;
        Element nodeElement;
        DocumentXML documentXML;
        String nodeName, nodeValue;

        nodeName = anElement.getNodeName();
        if (StringUtils.equalsIgnoreCase(nodeName, IO.XML_REPLY_NODE_NAME))
        {
            mField.clearFeatures();
            String attrValue = anElement.getAttribute(Doc.FEATURE_OP_STATUS_CODE);
            if (StringUtils.isNotEmpty(attrValue))
            {
                mField.setName(Doc.FEATURE_OP_STATUS_CODE);
                mField.setValue(attrValue);
                NamedNodeMap namedNodeMap = anElement.getAttributes();
                int attrCount = namedNodeMap.getLength();
                for (int attrOffset = 0; attrOffset < attrCount; attrOffset++)
                {
                    nodeAttr = (Attr) namedNodeMap.item(attrOffset);
                    nodeName = nodeAttr.getNodeName();
                    nodeValue = nodeAttr.getNodeValue();

                    if (StringUtils.isNotEmpty(nodeValue))
                    {
                        if ((! StringUtils.equalsIgnoreCase(nodeName, Doc.FEATURE_OP_STATUS_CODE)) &&
                            (! StringUtils.equalsIgnoreCase(nodeName, Doc.FEATURE_OP_COUNT)))
                            mField.addFeature(nodeName, nodeValue);
                    }
                }
            }
            NodeList nodeList = anElement.getChildNodes();
            for (int i = 0; i < nodeList.getLength(); i++)
            {
                nodeItem = nodeList.item(i);

                if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
                    continue;

// We really do not know how the node was named, so we will just accept it.

                nodeElement = (Element) nodeItem;
                documentXML = new DocumentXML();
                documentXML.load(nodeElement);
                document = documentXML.getDocument();
                if (document != null)
                    mDocumentList.add(document);
            }
        }
    }

    /**
     * Parses an XML DOM element and loads it into a document list.
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
        org.w3c.dom.Document xmlDocument = docBuilder.parse(inputSource);
        xmlDocument.getDocumentElement().normalize();

        load(xmlDocument.getDocumentElement());
    }

    /**
     * Parses an XML file identified by the path/file name parameter
     * and loads it into a document list.
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
