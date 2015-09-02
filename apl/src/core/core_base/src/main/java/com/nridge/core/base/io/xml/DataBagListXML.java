/*
 * NorthRidge Software, LLC - Copyright (c) 2015.
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
import com.nridge.core.base.field.data.DataTable;
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

/**
 * The DataBagListXML class provides XML helper methods.
 */
public class DataBagListXML implements DOMInterface
{
    private boolean mIsSimple;
    private ArrayList<DataBag> mDataBagList;

    /**
     * Default constructor.
     */
    public DataBagListXML()
    {
        mDataBagList = new ArrayList<DataBag>();
    }

    /**
     * Constructor accepts a list of bag instances as a parameter.
     *
     * @param aDataBagList A list of bag instances.
     */
    public DataBagListXML(ArrayList<DataBag> aDataBagList)
    {
        mDataBagList = aDataBagList;
    }

    /**
     * Constructor accepts a table instance as a parameter.
     *
     * @param aDataTable A data table instances.
     */
    public DataBagListXML(DataTable aDataTable)
    {
        mDataBagList = aDataTable.getAsBagList();
    }

    /**
     * Assigns the data bag list parameter to the internally managed list instance.
     *
     * @param aBagList A list of bag instances.
     */
    public void setBagList(ArrayList<DataBag> aBagList)
    {
        mDataBagList = aBagList;
    }

    /**
     * Returns a reference to the internally managed list of bag instances.
     *
     * @return List of bag instances.
     */
    public ArrayList<DataBag> getBagList()
    {
        return mDataBagList;
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
        DataBagXML dataBagXML;

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<%sList count=\"%d\">%n", aTagName, mDataBagList.size());

        for (DataBag dataBag : mDataBagList)
        {
            dataBagXML = new DataBagXML(dataBag);
            dataBagXML.setIsSimpleFlag(mIsSimple);
            dataBagXML.save(aPW, aTagName, anIndentAmount+1);
        }

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("</%sList>%n", aTagName);
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
        save(aPW, "DataBag", 0);
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
        DataBag dataBag;
        Element nodeElement;
        DataBagXML dataBagXML;

        nodeName = anElement.getNodeName();
        if (StringUtils.endsWithIgnoreCase(nodeName, "List"))
        {
            NodeList nodeList = anElement.getChildNodes();
            for (int i = 0; i < nodeList.getLength(); i++)
            {
                nodeItem = nodeList.item(i);

                if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
                    continue;

                nodeName = nodeItem.getNodeName();
                if (StringUtils.endsWithIgnoreCase(nodeName, "DataBag"))
                {
                    nodeElement = (Element) nodeItem;
                    dataBagXML = new DataBagXML();
                    dataBagXML.load(nodeElement);
                    dataBag = dataBagXML.getBag();
                    if (dataBag != null)
                        mDataBagList.add(dataBag);
                }
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
