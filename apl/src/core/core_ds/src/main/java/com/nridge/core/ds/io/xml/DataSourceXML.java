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

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Doc;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.ds.DS;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.io.IO;
import com.nridge.core.base.io.xml.*;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
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

/**
 * The DataSourceXML provides a collection of methods that can generate/load
 * an XML representation of a DataSource object.  In general, the
 * developer should use the data source I/O methods instead of this helper
 * implementation.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataSourceXML implements DOMInterface
{
    private Document mDocument;
    private final AppMgr mAppMgr;

    /**
     * Default constructor.
     */
    public DataSourceXML(AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        mDocument = new Document(IO.XML_DOCUMENT_NODE_NAME);
    }

    /**
     * Constructor accepts a document as a parameter.
     *
     * @param aDocument Document instance.
     */
    public DataSourceXML(AppMgr anAppMgr, Document aDocument)
    {
        mAppMgr = anAppMgr;
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
     * Saves the previous assigned document (e.g. via constructor or set method)
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
        Logger appLogger = mAppMgr.getLogger(this, "save");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<%s", aTagName);
        IOXML.writeAttrNameValue(aPW, "type", mDocument.getType());
        IOXML.writeAttrNameValue(aPW, "name", mDocument.getName());
        IOXML.writeAttrNameValue(aPW, "title", mDocument.getTitle());
        aPW.printf(">%n");

        DataBag dataBag = mDocument.getBag();
        DataField dataField = dataBag.getFieldByName("description");
        if ((dataField != null) && (StringUtils.isNotEmpty(dataField.getValue())))
        {
            DataFieldXML dataFieldXML = new DataFieldXML();
            if (StringUtils.isNotEmpty(dataBag.getValueAsString("description")))
            {
                dataFieldXML.saveNode(aPW, anIndentAmount+1);
                dataFieldXML.saveAttr(aPW, dataField);
                dataFieldXML.saveValue(aPW, dataField, anIndentAmount+1);
            }
        }

        String typeName = DS.RELATIONSHIP_CONFIGURATION;
        Relationship cfgRelationship = mDocument.getFirstRelationship(typeName);
        if (cfgRelationship != null)
        {
            DataBagXML dataBagXML = new DataBagXML(cfgRelationship.getBag());
            dataBagXML.setIsSimpleFlag(true);
            dataBagXML.save(aPW, typeName, anIndentAmount+1);
        }

        typeName = DS.RELATIONSHIP_SCHEMA;
        Relationship schemaRelationship = mDocument.getFirstRelationship(typeName);
        if (schemaRelationship != null)
        {
            DataBagXML dataBagXML = new DataBagXML(schemaRelationship.getBag());
            dataBagXML.setIsSimpleFlag(true);
            dataBagXML.save(aPW, typeName, anIndentAmount+1);
        }

        typeName = DS.RELATIONSHIP_VALUES;
        Relationship valuesRelationship = mDocument.getFirstRelationship(typeName);
        if (valuesRelationship != null)
        {
            DataBagXML dataBagXML = new DataBagXML(valuesRelationship.getBag());
            dataBagXML.setIsSimpleFlag(true);
            dataBagXML.save(aPW, typeName, anIndentAmount+1);
        }

        Document criteriaDocument = mDocument.getFirstRelatedDocument(DS.RELATIONSHIP_CRITERIA);
        if (criteriaDocument != null)
        {
            DataBagXML dataBagXML = new DataBagXML(criteriaDocument.getBag());
            dataBagXML.setIsSimpleFlag(true);
            dataBagXML.save(aPW, DS.RELATIONSHIP_CRITERIA, anIndentAmount+1);
        }

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("</%s>%n", aTagName);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
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
        save(aPW, "DataSource", 0);
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

    /**
     * Parses an XML DOM element and loads it into the document.
     *
     * @param anElement DOM element.
     * @throws java.io.IOException I/O related exception.
     */
    @Override
    public void load(Element anElement)
        throws IOException
    {
        Node nodeItem;
        String nodeName;
        DataBag dataBag;
        Element nodeElement;
        DataField dataField;
        DataTable dataTable;
        DataBagXML dataBagXML;
        DSCriteria dsCriteria;
        Document relatedDocument;
        DSCriteriaXML dsCriteriaXML;
        Relationship docRelationship;
        Logger appLogger = mAppMgr.getLogger(this, "load");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String typeName = anElement.getAttribute("type");
        if (StringUtils.isNotEmpty(typeName))
            mDocument.setType(typeName);
        String docName = anElement.getAttribute("name");
        if (StringUtils.isNotEmpty(typeName))
            mDocument.setName(docName);
        String docTitle = anElement.getAttribute("title");
        if (StringUtils.isNotEmpty(typeName))
            mDocument.setTitle(docTitle);


        DataFieldXML dataFieldXML = new DataFieldXML();
        NodeList nodeList = anElement.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            nodeItem = nodeList.item(i);

            if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
                continue;

            nodeName = nodeItem.getNodeName();
            if (StringUtils.equalsIgnoreCase(nodeName, "Field"))
            {
                nodeElement = (Element) nodeItem;
                dataField = dataFieldXML.load(nodeElement);
                if (dataField != null)
                {
                    dataBag = mDocument.getBag();
                    dataBag.add(dataField);
                }
            }
            else if (StringUtils.equalsIgnoreCase(nodeName, DS.RELATIONSHIP_CONFIGURATION))
            {
                nodeElement = (Element) nodeItem;
                dataBagXML = new DataBagXML();
                dataBagXML.load(nodeElement);
                dataBag = dataBagXML.getBag();
                if (dataBag.count() > 0)
                {
                    docRelationship = new Relationship(DS.RELATIONSHIP_CONFIGURATION, dataBag);
                    mDocument.addRelationship(docRelationship);
                }
            }
            else if (StringUtils.equalsIgnoreCase(nodeName, DS.RELATIONSHIP_SCHEMA))
            {
                nodeElement = (Element) nodeItem;
                dataBagXML = new DataBagXML();
                dataBagXML.load(nodeElement);
                dataBag = dataBagXML.getBag();
                if (dataBag.count() > 0)
                {
                    if (StringUtils.isEmpty(dataBag.getName()))
                        dataBag.setName(mDocument.getName());
                    {
                        docRelationship = new Relationship(DS.RELATIONSHIP_SCHEMA, dataBag);
                        mDocument.addRelationship(docRelationship);
                    }
                }
            }
            else if (StringUtils.equalsIgnoreCase(nodeName, DS.RELATIONSHIP_VALUES))
            {
                nodeElement = (Element) nodeItem;
                dataBagXML = new DataBagXML();
                dataBagXML.load(nodeElement);
                dataBag = dataBagXML.getBag();
                if (dataBag.count() > 0)
                {
                    docRelationship = new Relationship(DS.RELATIONSHIP_VALUES, dataBag);
                    mDocument.addRelationship(docRelationship);
                }
            }
            else if (StringUtils.equalsIgnoreCase(nodeName, DS.RELATIONSHIP_CRITERIA))
            {
                nodeElement = (Element) nodeItem;
                dsCriteriaXML = new DSCriteriaXML();
                dsCriteriaXML.load(nodeElement);
                dsCriteria = dsCriteriaXML.getCriteria();
                if (dsCriteria.count() > 0)
                {
                    dataTable = dsCriteria.toTable();
                    relatedDocument = new Document(DS.RELATIONSHIP_CRITERIA, dataTable);
                    mDocument.addRelationship(relatedDocument.getType(), relatedDocument);
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
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
