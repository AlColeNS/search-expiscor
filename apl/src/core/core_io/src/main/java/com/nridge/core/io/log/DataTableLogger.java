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

package com.nridge.core.io.log;

import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.std.StrUtl;
import org.slf4j.Logger;

import java.util.Map;

/**
 * The DataTableLogger class provides logger helper methods.
 */
public class DataTableLogger
{
    private Logger mLogger;
    private DataFieldLogger mDataFieldLogger;

    public DataTableLogger(Logger aLogger)
    {
        mLogger = aLogger;
        mDataFieldLogger = new DataFieldLogger(aLogger);
    }

    public void writeCommon(DataTable aTable)
    {
        if (aTable != null)
        {
            DataField dataField;
            int fieldWidth, colWidth;

// Calculate our maximum column width based on value size

            int maxColWidth = 0;
            int rowCount = aTable.rowCount();
            int colCount = aTable.columnCount();
            if ((rowCount > 0) && (colCount > 0))
            {
                for (int row = 0; row < rowCount; row++)
                {
                    for (int col = 0; col < colCount; col++)
                    {
                        dataField = aTable.getFieldByRowCol(row, col);
                        maxColWidth = Math.max(maxColWidth, dataField.getName().length());
                        maxColWidth = Math.max(maxColWidth, dataField.collapse().length());
                    }
                }
            }

// Field Name

            StringBuilder rowStrBuilder = new StringBuilder();
            for (int col = 0; col < colCount; col++)
            {
                dataField = aTable.getFieldByColumn(col);
                fieldWidth = dataField.getName().length();
                colWidth = Math.min(maxColWidth, fieldWidth);
                rowStrBuilder.append(dataField.getName().substring(0, colWidth));
                for (int k = fieldWidth; k < maxColWidth; k++)
                    rowStrBuilder.append(StrUtl.CHAR_SPACE);
                rowStrBuilder.append(StrUtl.CHAR_SPACE);
            }
            mLogger.debug(rowStrBuilder.toString());

// Underline it

            rowStrBuilder.setLength(0);
            for (int col = 0; col < colCount; col++)
            {
                dataField = aTable.getFieldByColumn(col);
                fieldWidth = dataField.getName().length();
                colWidth = Math.min(maxColWidth, fieldWidth);
                for (int j = 0; j < colWidth; j++)
                    rowStrBuilder.append(StrUtl.CHAR_HYPHEN);
                for (int k = fieldWidth; k < maxColWidth; k++)
                    rowStrBuilder.append(StrUtl.CHAR_SPACE);
                rowStrBuilder.append(StrUtl.CHAR_SPACE);
            }
            mLogger.debug(rowStrBuilder.toString());

// Row values

            if ((rowCount > 0) && (colCount > 0))
            {
                for (int row = 0; row < rowCount; row++)
                {
                    rowStrBuilder.setLength(0);
                    for (int col = 0; col < colCount; col++)
                    {
                        dataField = aTable.getFieldByRowCol(row, col);
                        fieldWidth = dataField.getValue().length();
                        colWidth = Math.min(maxColWidth, fieldWidth);
                        rowStrBuilder.append(dataField.getValue().substring(0, colWidth));
                        for (int k = fieldWidth; k < maxColWidth; k++)
                            rowStrBuilder.append(StrUtl.CHAR_SPACE);
                        rowStrBuilder.append(StrUtl.CHAR_SPACE);
                    }
                    mLogger.debug(rowStrBuilder.toString());
                }
            }
        }
    }

    public void writeFull(DataTable aTable)
    {
        if (aTable != null)
        {
            mDataFieldLogger.writeNV("Name", aTable.getName());

            String nameString;
            int featureOffset = 0;
            for (Map.Entry<String, String> featureEntry : aTable.getFeatures().entrySet())
            {
                nameString = String.format(" F[%02d] %s", featureOffset++, featureEntry.getKey());
                mDataFieldLogger.writeNV(nameString, featureEntry.getValue());
            }
            writeCommon(aTable);
            PropertyLogger propertyLogger = new PropertyLogger(mLogger);
            propertyLogger.writeFull(aTable.getProperties());
        }
    }

    public void writeSimple(DataTable aTable)
    {
        writeCommon(aTable);
    }

    public void write(DataTable aTable)
    {
        writeFull(aTable);
    }
}
