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
import com.nridge.core.base.field.FieldRange;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.StrUtl;
import com.nridge.core.base.std.XMLUtl;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

/**
 * The RangeXML class provides XML helper methods.
 */
public class RangeXML
{
    private final String RANGE_NODE_NAME = "Range";

    public RangeXML()
    {
    }

    public void saveNode(PrintWriter aPW, int anIndentAmount)
        throws IOException
    {
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<%s", RANGE_NODE_NAME);
    }

    public void saveAttr(PrintWriter aPW, FieldRange aFieldRange)
        throws IOException
    {
        IOXML.writeAttrNameValue(aPW, "type", Field.typeToString(aFieldRange.getType()));
        IOXML.writeAttrNameValue(aPW, "delimiterChar", aFieldRange.getDelimiterChar());
    }

    public void saveValue(PrintWriter aPW, FieldRange aFieldRange)
        throws IOException
    {
        if (aFieldRange.getType() == Field.Type.Text)
        {
            String singleString = StrUtl.collapseToSingle(aFieldRange.getItems(), aFieldRange.getDelimiterChar());
            aPW.printf(">%s</%s>%n", StringEscapeUtils.escapeXml10(singleString),
                       RANGE_NODE_NAME);
        }
        else
        {
            IOXML.writeAttrNameValue(aPW, "min", aFieldRange.getMinString());
            IOXML.writeAttrNameValue(aPW, "max", aFieldRange.getMaxString());
            aPW.printf("/>%n");
        }
    }

    public FieldRange load(Element anElement)
        throws IOException
    {
        FieldRange fieldRange;

        String attrValue = anElement.getAttribute("type");
        if (StringUtils.isNotEmpty(attrValue))
        {
            Field.Type rangeType = Field.stringToType(attrValue);
            if (rangeType == Field.Type.Text)
            {
                fieldRange = new FieldRange();
                String rangeValue = XMLUtl.getElementStrValue(anElement);
                String delimiterString = anElement.getAttribute("delimiterChar");
                if (StringUtils.isNotEmpty(delimiterString))
                    fieldRange.setDelimiterChar(delimiterString);
                fieldRange.setItems(StrUtl.expandToList(rangeValue, fieldRange.getDelimiterChar()));
            }
            else
            {
                String minValue = anElement.getAttribute("min");
                String maxValue = anElement.getAttribute("max");
                switch (rangeType)
                {
                    case Long:
                        fieldRange = new FieldRange(Field.createLong(minValue), Field.createLong(maxValue));
                        break;
                    case Integer:
                        fieldRange = new FieldRange(Field.createInt(minValue), Field.createInt(maxValue));
                        break;
                    case Double:
                        fieldRange = new FieldRange(Field.createDouble(minValue), Field.createDouble(maxValue));
                        break;
                    case DateTime:
                        fieldRange = new FieldRange(Field.createDate(minValue), Field.createDate(maxValue));
                        break;
                    default:
                        fieldRange = null;
                        break;
                }
            }
        }
        else
            fieldRange = null;

        return fieldRange;
    }
}
