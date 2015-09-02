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

package com.nridge.core.base.io.console;

import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * The DataTableConsole class provides a convenient way for an application
 * to generate a presentation for the console.  The presentation of the
 * columns are aligned based on display widths.
 * <p>
 * <b>Note:</b> The width of the column will be derived from the field
 * display width or the field name length or the field value length.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataTableConsole
{
    private DataTable mTable;

    /**
     * Creates an attribute table console object.
     *
     * @param aTable A data table.
     */
    public DataTableConsole(DataTable aTable)
    {
        mTable = aTable;
    }

    private int deriveFieldWidth(DataField aField, int aMaxWidth)
    {
        int nameLength, titleLength, valueLength;

        int displayWidth = aField.getDisplaySize();
        if (displayWidth == 0)
        {
            nameLength = aField.getName().length();
            titleLength = aField.getTitle().length();
            valueLength = aField.collapse(StrUtl.CHAR_COMMA).length();
            displayWidth = Math.max(nameLength, displayWidth);
            displayWidth = Math.max(titleLength, displayWidth);
            displayWidth = Math.max(valueLength, displayWidth);
        }

        if (aMaxWidth == 0)
            return displayWidth;
        else
            return Math.min(displayWidth, aMaxWidth);
    }

    private int deriveRowColumnWidth(int aCol, int aMaxWidth)
    {
        int rowCount = mTable.rowCount();
        DataField dataField = mTable.getFieldByColumn(aCol);
        int maxColDisplayWidth = deriveFieldWidth(dataField, aMaxWidth);

        for (int row = 0; row < rowCount; row++)
        {
            dataField = mTable.getFieldByRowCol(row, aCol);
            maxColDisplayWidth = Math.max(maxColDisplayWidth, deriveFieldWidth(dataField, aMaxWidth));
        }

        return maxColDisplayWidth;
    }

    /**
     * Writes the contents of the data table to the console.
     *
     * @param aPW Printer writer instance.
     * @param aTitle An optional presentation title.
     * @param aMaxWidth Maximum width of any column (zero means unlimited).
     * @param aColSpace Number of spaces between columns.
     */
    public void write(PrintWriter aPW, String aTitle,
                      int aMaxWidth, int aColSpace)
    {
        String rowString;
        DataField dataField;
        StringBuilder rowStrBuilder;
        int i, j, k, colCount, rowCount;
        String labelString, valueString;
        int strLength, colWidth, displayWidth, totalWidth;

// Calculate our total output width.

        totalWidth = 0;
        colCount = mTable.columnCount();
        for (int col = 0; col < colCount; col++)
        {
            dataField = mTable.getFieldByColumn(col);

            if (dataField.isFeatureTrue(Field.FEATURE_IS_HIDDEN))
                continue;

            totalWidth += deriveRowColumnWidth(col, aMaxWidth);
        }
        totalWidth += (aColSpace * (colCount - 1));

// Account for our title string.

        if (StringUtils.isNotEmpty(aTitle))
        {
            totalWidth = Math.max(aTitle.length(), totalWidth);
            rowString = StrUtl.centerSpaces(aTitle, totalWidth);
            aPW.printf("%n%s%n%n", rowString);
        }

// Display our column header information.

        rowStrBuilder = new StringBuilder();

        for (int col = 0; col < colCount; col++)
        {
            dataField = mTable.getFieldByColumn(col);

            if (dataField.isFeatureTrue(Field.FEATURE_IS_HIDDEN))
                continue;

            displayWidth = deriveRowColumnWidth(col, aMaxWidth);
            labelString = dataField.getTitle();
            strLength = labelString.length();
            colWidth = displayWidth + aColSpace;
            strLength = Math.min(displayWidth, strLength);
            rowStrBuilder.append(labelString.substring(0, strLength));
            for (k = strLength; k < colWidth; k++)
                rowStrBuilder.append(StrUtl.CHAR_SPACE);
        }
        aPW.printf("%s%n", rowStrBuilder.toString());

// Underline our column headers.

        rowStrBuilder.setLength(0);

        for (int col = 0; col < colCount; col++)
        {
            dataField = mTable.getFieldByColumn(col);

            if (dataField.isFeatureTrue(Field.FEATURE_IS_HIDDEN))
                continue;

            displayWidth = deriveRowColumnWidth(col, aMaxWidth);
            labelString = dataField.getTitle();
            strLength = labelString.length();
            colWidth = displayWidth + aColSpace;
            strLength = Math.min(displayWidth, strLength);
            for (j = 0; j < strLength; j++)
                rowStrBuilder.append(StrUtl.CHAR_HYPHEN);
            for (k = strLength; k < colWidth; k++)
                rowStrBuilder.append(StrUtl.CHAR_SPACE);
        }
        aPW.printf("%s%n", rowStrBuilder.toString());

// Display each row of cells.

        rowCount = mTable.rowCount();

        for (int row = 0; row < rowCount; row++)
        {
            rowStrBuilder.setLength(0);

            for (int col = 0; col < colCount; col++)
            {
                dataField = mTable.getFieldByRowCol(row, col);

                if (dataField.isFeatureTrue(Field.FEATURE_IS_HIDDEN))
                    continue;

                displayWidth = deriveRowColumnWidth(col, aMaxWidth);
                if (dataField.isAssigned())
                {
                    valueString = dataField.collapse(StrUtl.CHAR_COMMA);
                    if (StringUtils.isEmpty(valueString))
                        valueString = StringUtils.EMPTY;
                }
                else
                    valueString = StringUtils.EMPTY;

                strLength = valueString.length();
                colWidth = displayWidth + aColSpace;
                strLength = Math.min(displayWidth, strLength);
                rowStrBuilder.append(valueString.substring(0, strLength));
                for (k = strLength; k < colWidth; k++)
                    rowStrBuilder.append(StrUtl.CHAR_SPACE);
            }
            aPW.printf("%s%n", rowStrBuilder.toString());
        }
        aPW.printf("%n");
    }

    /**
     * Writes the contents of the attribute table to the console.
     *
     * @param aPW Print writer instance.
     * @param aTitle An optional presentation title.
     */
    public void write(PrintWriter aPW, String aTitle)
    {
        write(aPW, aTitle, 0, 2);
    }
}
