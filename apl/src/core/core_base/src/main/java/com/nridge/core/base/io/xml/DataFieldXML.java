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
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.StrUtl;
import com.nridge.core.base.std.XMLUtl;
import com.nridge.core.base.io.IO;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

/**
 * The DataFieldXML class provides XML helper methods.
 */
public class DataFieldXML
{
    private RangeXML mRangeXML;

    public DataFieldXML()
    {
        mRangeXML = new RangeXML();
    }

    public void saveNode(PrintWriter aPW, int anIndentAmount)
        throws IOException
    {
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<%s", IO.XML_FIELD_NODE_NAME);
    }

    public void saveAttr(PrintWriter aPW, DataField aDataField)
        throws IOException
    {
        IOXML.writeAttrNameValue(aPW, "type", Field.typeToString(aDataField.getType()));
        IOXML.writeAttrNameValue(aPW, "name", aDataField.getName());
        IOXML.writeAttrNameValue(aPW, "title", aDataField.getTitle());
        if (aDataField.isMultiValue())
            IOXML.writeAttrNameValue(aPW, "isMultiValue", aDataField.isMultiValue());
        if (aDataField.getDisplaySize() > 0)
            IOXML.writeAttrNameValue(aPW, "displaySize", aDataField.getDisplaySize());
        IOXML.writeAttrNameValue(aPW, "defaultValue", aDataField.getDefaultValue());
        if (aDataField.getSortOrder() != Field.Order.UNDEFINED)
            IOXML.writeAttrNameValue(aPW, "sortOrder", aDataField.getSortOrder().name());
        if (aDataField.isRangeAssigned())
            IOXML.writeAttrNameValue(aPW, "rangeType", Field.typeToString(aDataField.getRange().getType()));
        for (Map.Entry<String, String> featureEntry : aDataField.getFeatures().entrySet())
            IOXML.writeAttrNameValue(aPW, featureEntry.getKey(), featureEntry.getValue());
        if (aDataField.isRangeAssigned())
            aPW.printf(">%n");
    }

    public void saveValue(PrintWriter aPW, DataField aDataField, int anIndentAmount)
            throws IOException
    {
        if (aDataField.isRangeAssigned())
        {
            String singleValue;
            mRangeXML.saveNode(aPW, anIndentAmount+1);
            mRangeXML.saveAttr(aPW, aDataField.getRange());
            mRangeXML.saveValue(aPW, aDataField.getRange());
            if (aDataField.isMultiValue())
            {
                String mvDelimiter = aDataField.getFeature(Field.FEATURE_MV_DELIMITER);
                if (StringUtils.isNotEmpty(mvDelimiter))
                    singleValue = aDataField.collapse(mvDelimiter.charAt(0));
                else
                    singleValue = aDataField.collapse();
            }
            else
                singleValue = aDataField.getValue();

            IOXML.writeNodeNameValue(aPW, anIndentAmount+1, "Value", singleValue);
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("</%s>%n", IO.XML_FIELD_NODE_NAME);
        }
        else
        {
            if (aDataField.isMultiValue())
            {
                String mvDelimiter = aDataField.getFeature(Field.FEATURE_MV_DELIMITER);
                if (StringUtils.isNotEmpty(mvDelimiter))
                    aPW.printf(">%s</%s>%n", StringEscapeUtils.escapeXml10(aDataField.collapse(mvDelimiter.charAt(0))),
                               IO.XML_FIELD_NODE_NAME);
                else
                    aPW.printf(">%s</%s>%n", StringEscapeUtils.escapeXml10(aDataField.collapse()),
                               IO.XML_FIELD_NODE_NAME);
            }
            else
            {
                if (aDataField.isFeatureTrue(Field.FEATURE_IS_CONTENT))
                    aPW.printf(">%s</%s>%n", XMLUtl.escapeElemStrValue(aDataField.getValue()),
                               IO.XML_FIELD_NODE_NAME);
                else
                    aPW.printf(">%s</%s>%n", StringEscapeUtils.escapeXml10(aDataField.getValue()),
                               IO.XML_FIELD_NODE_NAME);
            }
        }
    }

    public DataField load(Element anElement)
        throws IOException
    {
        Attr nodeAttr;
        Node nodeItem;
        DataField dataField;
        Element nodeElement;
        Field.Type fieldType;
        String nodeName, nodeValue;

        String attrValue = anElement.getAttribute("name");
        if (StringUtils.isNotEmpty(attrValue))
        {
            String fieldName = attrValue;
            attrValue = anElement.getAttribute("type");
            if (StringUtils.isNotEmpty(attrValue))
                fieldType = Field.stringToType(attrValue);
            else
                fieldType = Field.Type.Text;
            dataField = new DataField(fieldType, fieldName);

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
                        (StringUtils.equalsIgnoreCase(nodeName, "rangeType")))
                        continue;
                    else if (StringUtils.equalsIgnoreCase(nodeName, "title"))
                        dataField.setTitle(nodeValue);
                    else if (StringUtils.equalsIgnoreCase(nodeName, "isMultiValue"))
                        dataField.setMultiValueFlag(StrUtl.stringToBoolean(nodeValue));
                    else if (StringUtils.equalsIgnoreCase(nodeName, "displaySize"))
                        dataField.setDisplaySize(Field.createInt(nodeValue));
                    else if (StringUtils.equalsIgnoreCase(nodeName, "sortOrder"))
                        dataField.setSortOrder(Field.Order.valueOf(nodeValue));
                    else if (StringUtils.equalsIgnoreCase(nodeName, "defaultValue"))
                        dataField.setDefaultValue(nodeValue);
                    else
                        dataField.addFeature(nodeName, nodeValue);
                }
            }
            String rangeType = anElement.getAttribute("rangeType");
            if (StringUtils.isNotEmpty(rangeType))
            {
                NodeList nodeList = anElement.getChildNodes();
                for (int i = 0; i < nodeList.getLength(); i++)
                {
                    nodeItem = nodeList.item(i);

                    if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
                        continue;

                    nodeName = nodeItem.getNodeName();
                    if (StringUtils.equalsIgnoreCase(nodeName, "Range"))
                    {
                        nodeElement = (Element) nodeItem;
                        dataField.setRange(mRangeXML.load(nodeElement));
                    }
                    else if (StringUtils.equalsIgnoreCase(nodeName, "Value"))
                    {
                        nodeValue = XMLUtl.getNodeStrValue(nodeItem);
                        String mvDelimiter = dataField.getFeature(Field.FEATURE_MV_DELIMITER);
                        if (StringUtils.isNotEmpty(mvDelimiter))
                            dataField.expand(nodeValue, mvDelimiter.charAt(0));
                        else
                            dataField.expand(nodeValue);
                    }
                }
            }
            else
            {
                nodeItem = (Node) anElement;
                if (dataField.isFeatureTrue(Field.FEATURE_IS_CONTENT))
                    nodeValue = XMLUtl.getNodeCDATAValue(nodeItem);
                else
                    nodeValue = XMLUtl.getNodeStrValue(nodeItem);
                if (dataField.isMultiValue())
                {
                    String mvDelimiter = dataField.getFeature(Field.FEATURE_MV_DELIMITER);
                    if (StringUtils.isNotEmpty(mvDelimiter))
                        dataField.expand(nodeValue, mvDelimiter.charAt(0));
                    else
                        dataField.expand(nodeValue);
                }
                else
                    dataField.setValue(nodeValue);
            }
        }
        else
            dataField = null;

        return dataField;
    }
}
