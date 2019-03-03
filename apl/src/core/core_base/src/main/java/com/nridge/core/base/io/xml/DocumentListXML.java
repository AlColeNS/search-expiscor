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
import com.nridge.core.base.io.IO;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
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
import java.util.ArrayList;

/**
 * The DocumentListXML class provides XML helper methods.
 */
public class DocumentListXML implements DOMInterface
{
    private boolean mIsSimple;
    private ArrayList<Document> mDocumentList;

    /**
     * Default constructor.
     */
    public DocumentListXML()
    {
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Constructor accepts a list of document instances as a parameter.
     *
     * @param aDocumentList A list of document instances.
     */
    public DocumentListXML(ArrayList<Document> aDocumentList)
    {
        mDocumentList = aDocumentList;
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
        aPW.printf("<%s%s count=\"%d\">%n", aTagName, IO.XML_LIST_NODE_NAME, mDocumentList.size());

        for (Document document : mDocumentList)
        {
            documentXML = new DocumentXML(document);
            documentXML.setIsSimpleFlag(mIsSimple);
            documentXML.save(aPW, aTagName, anIndentAmount+1);
        }

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("</%s%s>%n", aTagName, IO.XML_LIST_NODE_NAME);
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
        save(aPW, IO.XML_DOCUMENT_NODE_NAME, 0);
    }

    /**
     * Saves the previous assigned bag list (e.g. via constructor or set method)
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
     * Parses an XML DOM element and loads it into a bag list.
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
        Document document;
        Element nodeElement;
        DocumentXML documentXML;

        nodeName = anElement.getNodeName();
        if (StringUtils.endsWithIgnoreCase(nodeName, IO.XML_LIST_NODE_NAME))
        {
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
     * Parses an XML DOM element and loads it into a bag list.
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
        org.w3c.dom.Document xmlDocument = docBuilder.parse(new File(aPathFileName));
        xmlDocument.getDocumentElement().normalize();

        load(xmlDocument.getDocumentElement());
    }
}
