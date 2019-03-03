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
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.io.DocOpInterface;
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
 * The DocumentOpXML class provides XML helper methods for document
 * operations.
 */
public class DocumentOpXML implements DOMInterface, DocOpInterface
{
    private DataField mField;
    private boolean mIsSimple;
    private DSCriteria mCriteria;
    private ArrayList<Document> mDocumentList;

    /**
     * Default constructor.
     */
    public DocumentOpXML()
    {
        mField = new DataTextField("undefined");
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Default constructor.
     *
     * @param aName Operation name.
     */
    public DocumentOpXML(String aName)
    {
        mField = new DataTextField(aName);
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Constructor accepts a list of document instances as a parameter.
     *
     * @param aName Text field name.
     * @param aDocumentList A list of document instances.
     */
    public DocumentOpXML(String aName, ArrayList<Document> aDocumentList)
    {
        mField = new DataTextField(aName);
        mDocumentList = aDocumentList;
    }

    /**
     * Constructor accepts a data source criteria as a parameter.
     *
     * @param aName Criteria name.
     * @param aCriteria Data source criteria instance.
     */
    public DocumentOpXML(String aName, DSCriteria aCriteria)
    {
        mCriteria = aCriteria;
        mField = new DataTextField(aName);
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Assigns an operation field containing a name and other desired features.
     *
     * @param aField Operation field.
     */
    public void setField(DataField aField)
    {
        mField = aField;
    }

    /**
     * Returns an operation field.
     *
     * @return Data field instance.
     */
    public DataField getField()
    {
        return mField;
    }

    /**
     * Assigns standard account features to the operation. Please
     * note that the parent application is responsible for encrypting
     * the account password prior to assigning it in this method.
     *
     * @param anAccountName Account name.
     * @param anAccountPassword Account password.
     */
    public void setAccountNamePassword(String anAccountName, String anAccountPassword)
    {
        mField.addFeature(Doc.FEATURE_OP_ACCOUNT_NAME, anAccountName);
        mField.addFeature(Doc.FEATURE_OP_ACCOUNT_PASSWORD, anAccountPassword);
    }

    /**
     * Assigns standard account features to the operation. Please
     * note that the parent application is responsible for hashing
     * the account password prior to assigning it in this method.
     *
     * @param anAccountName Account name.
     * @param anAccountPasshash Account password.
     */
    public void setAccountNamePasshash(String anAccountName, String anAccountPasshash)
    {
        mField.addFeature(Doc.FEATURE_OP_ACCOUNT_NAME, anAccountName);
        mField.addFeature(Doc.FEATURE_OP_ACCOUNT_PASSHASH, anAccountPasshash);
    }

    /**
     * Assigns standard account features to the operation.
     *
     * @param aSessionId Session identifier.
     */
    public void setSessionId(String aSessionId)
    {
        mField.addFeature(Doc.FEATURE_OP_SESSION, aSessionId);
    }

    /**
     * Adds the document to the internally managed document list.
     *
     * @param aDocument Document instance.
     */
    public void addDocument(Document aDocument)
    {
        if (aDocument != null)
            mDocumentList.add(aDocument);
    }

    /**
     * Assigns the document list parameter to the internally managed list instance.
     *
     * @param aDocumentList A list of document instances.
     */
    public void setDocumentList(ArrayList<Document> aDocumentList)
    {
        if (aDocumentList != null)
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
     * Assigns the data source criteria instance.  A criteria instance
     * is only assigned for <code>Doc.OPERATION_FETCH</code> names.
     *
     * @param aCriteria Data source criteria instance.
     */
    public void setCriteria(DSCriteria aCriteria)
    {
        mCriteria = aCriteria;
    }

    /**
     * Returns a reference to an internally managed criteria instance.
     *
     * @return Data source criteria instance if the operation was
     * <code>Doc.OPERATION_FETCH</code> or <i>null</i> otherwise.
     */
    public DSCriteria getCriteria()
    {
        return mCriteria;
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
     * Saves the previous assigned document list (e.g. via constructor or set method)
     * to the print writer stream wrapped in a tag name specified in the parameter.
     *
     * @param aPW PrintWriter stream instance.
     * @param aTagName Document tag name.
     * @param anIndentAmount Indentation count.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(PrintWriter aPW, String aTagName, int anIndentAmount)
        throws IOException
    {
        DocumentXML documentXML;

        if (mCriteria == null)
        {
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<%s", IO.XML_OPERATION_NODE_NAME);
            IOXML.writeAttrNameValue(aPW, Doc.FEATURE_OP_NAME, mField.getName());
            int docCount = mDocumentList.size();
            if (docCount > 0)
                IOXML.writeAttrNameValue(aPW, "count", mDocumentList.size());
            for (Map.Entry<String, String> featureEntry : mField.getFeatures().entrySet())
                IOXML.writeAttrNameValue(aPW, featureEntry.getKey(), featureEntry.getValue());

            if (docCount == 0)
                aPW.printf("/>%n");
            else
            {
                aPW.printf(">%n");
                for (Document document : mDocumentList)
                {
                    documentXML = new DocumentXML(document);
                    documentXML.setIsSimpleFlag(mIsSimple);
                    documentXML.setSaveFieldsWithoutValues(true);
                    documentXML.save(aPW, aTagName, anIndentAmount + 1);
                }
                IOXML.indentLine(aPW, anIndentAmount);
                aPW.printf("</%s>%n", IO.XML_OPERATION_NODE_NAME);
            }
        }
        else
        {
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<%s", IO.XML_OPERATION_NODE_NAME);
            IOXML.writeAttrNameValue(aPW, Doc.FEATURE_OP_NAME, mField.getName());
            for (Map.Entry<String, String> featureEntry : mField.getFeatures().entrySet())
                IOXML.writeAttrNameValue(aPW, featureEntry.getKey(), featureEntry.getValue());
            aPW.printf(">%n");
            DSCriteriaXML dsCriteriaXML = new DSCriteriaXML(mCriteria);
            dsCriteriaXML.save(aPW, IO.XML_CRITERIA_NODE_NAME, anIndentAmount + 1);
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("</%s>%n", IO.XML_OPERATION_NODE_NAME);
        }
    }

    /**
     * Saves the previous assigned document list (e.g. via constructor or set method)
     * to the print writer stream specified as a parameter.
     *
     * @param aPW PrintWriter stream instance.
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void save(PrintWriter aPW)
        throws IOException
    {
        save(aPW, IO.XML_DOCUMENT_NODE_NAME, 0);
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
        PrintWriter printWriter = new PrintWriter(stringWriter);
        save(printWriter);
        printWriter.close();

        return stringWriter.toString();
    }

    private void loadOperation(Element anElement)
        throws IOException
    {
        Attr nodeAttr;
        Node nodeItem;
        Document document;
        Element nodeElement;
        DocumentXML documentXML;
        DSCriteriaXML criteriaXML;
        String nodeName, nodeValue;

        mCriteria = null;
        mDocumentList.clear();
        mField.clearFeatures();

        String attrValue = anElement.getAttribute(Doc.FEATURE_OP_NAME);
        if (StringUtils.isNotEmpty(attrValue))
        {
            mField.setName(attrValue);
            NamedNodeMap namedNodeMap = anElement.getAttributes();
            int attrCount = namedNodeMap.getLength();
            for (int attrOffset = 0; attrOffset < attrCount; attrOffset++)
            {
                nodeAttr = (Attr) namedNodeMap.item(attrOffset);
                nodeName = nodeAttr.getNodeName();
                nodeValue = nodeAttr.getNodeValue();

                if (StringUtils.isNotEmpty(nodeValue))
                {
                    if ((! StringUtils.equalsIgnoreCase(nodeName, Doc.FEATURE_OP_NAME)) &&
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

            nodeName = nodeItem.getNodeName();
            if (nodeName.equalsIgnoreCase(IO.XML_CRITERIA_NODE_NAME))
            {
                nodeElement = (Element) nodeItem;
                criteriaXML = new DSCriteriaXML();
                criteriaXML.load(nodeElement);
                mCriteria = criteriaXML.getCriteria();
            }
            else if (StringUtils.startsWithIgnoreCase(nodeName, IO.XML_DOCUMENT_NODE_NAME))
            {
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
     * @param anElement DOM element.
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void load(Element anElement)
        throws IOException
    {
        String nodeName = anElement.getNodeName();
        if (StringUtils.equalsIgnoreCase(nodeName, IO.XML_OPERATION_NODE_NAME))
            loadOperation(anElement);
    }

    /**
     * Parses an XML DOM element and loads it into a document list.
     *
     * @param aDocument XML document instance.
     *
     * @throws java.io.IOException                            I/O related exception.
     * @throws javax.xml.parsers.ParserConfigurationException XML parser related exception.
     * @throws org.xml.sax.SAXException                       XML parser related exception.
     * @throws TransformerException Transformer exception.
     */
    public void load(org.w3c.dom.Document aDocument)
        throws ParserConfigurationException, IOException, SAXException, TransformerException
    {
        aDocument.getDocumentElement().normalize();
        load(aDocument.getDocumentElement());
    }

    /**
     * Parses an XML DOM element and loads it into a document list.
     *
     * @param anIS Input stream.
     *
     * @throws java.io.IOException                            I/O related exception.
     * @throws javax.xml.parsers.ParserConfigurationException XML parser related exception.
     * @throws org.xml.sax.SAXException                       XML parser related exception.
     * @throws TransformerException Transformer exception.
     */
    @Override
    public void load(InputStream anIS)
        throws ParserConfigurationException, IOException, SAXException, TransformerException
    {
        DocumentBuilderFactory docBldFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docBldFactory.newDocumentBuilder();
        InputSource inputSource = new InputSource(anIS);
        org.w3c.dom.Document xmlDocument = docBuilder.parse(inputSource);
        load(xmlDocument);
    }

    /**
     * Parses an XML file identified by the path/file name parameter
     * and loads it into a document list.
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
        org.w3c.dom.Document xmlDocument = docBuilder.parse(new File(aPathFileName));
        xmlDocument.getDocumentElement().normalize();

        load(xmlDocument.getDocumentElement());
    }
}
