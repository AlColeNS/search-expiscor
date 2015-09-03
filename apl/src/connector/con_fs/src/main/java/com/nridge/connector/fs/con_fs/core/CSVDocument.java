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

package com.nridge.connector.fs.con_fs.core;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.StrUtl;
import com.nridge.ds.solr.Solr;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * The CSVDocument reads a CSV file and generates a data
 * bag from each row in the table.
 */
public class CSVDocument
{
    private DataBag mBag;
    private AppMgr mAppMgr;
    private String mPathFileName;
    private String mCellDateFormat;
    private String[] mColumnHeaders;
    private CsvListReader mCSVListReader;

    public CSVDocument(AppMgr anAppMgr, DataBag aBag)
    {
        mBag = aBag;
        mAppMgr = anAppMgr;
        mCellDateFormat = Solr.DOC_DATETIME_FORMAT;
    }

    /**
     * Opens the CSV file and prepares it for multi-document processing.
     *
     * @param aPathFileName Path/File name.
     *
     * @throws IOException Identifies an I/O error.
     */
    public void open(String aPathFileName)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "open");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        File csvFile = new File(aPathFileName);
        if (! csvFile.exists())
            throw new IOException(aPathFileName + ": Does not exist.");

        mCellDateFormat = mAppMgr.getString(Constants.CFG_PROPERTY_PREFIX + ".extract.csv_cell_date_format",
                                            Solr.DOC_DATETIME_FORMAT);

        mPathFileName = aPathFileName;
        FileReader fileReader = new FileReader(csvFile);
        mCSVListReader = new CsvListReader(fileReader, CsvPreference.EXCEL_PREFERENCE);
        mColumnHeaders = mCSVListReader.getHeader(true);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Extracts the next row of fields and populates them into a newly
     * created data bag.
     *
     * ToDo: Support for multi-value.
     *
     * @return Data bag instance.
     *
     * @throws IOException Identifies an I/O error.
     */
    public DataBag extractNext()
        throws IOException
    {
        Date cellDate;
        DataBag dataBag;
        String mvDelimiter;
        DataField dataField;
        String cellName, cellValue;
        Logger appLogger = mAppMgr.getLogger(this, "extractNext");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mCSVListReader == null)
        {
            dataBag = null;
            appLogger.error("The CSV Document has not been successfully opened for processing.");
        }
        else
        {
            List<String> rowCells = mCSVListReader.read();
            if (rowCells == null)
                dataBag = null;
            else
            {
                dataBag = new DataBag(mBag);
                dataBag.resetValuesWithDefaults();

                int colCount = rowCells.size();
                for (int col = 0; col < colCount; col++)
                {
                    cellValue = rowCells.get(col);
                    cellName = StringUtils.trim(mColumnHeaders[col]);
                    if ((StringUtils.isNotEmpty(cellName)) && (StringUtils.isNotEmpty(cellValue)) &&
                        (! Solr.isSolrReservedFieldName(cellName)))
                    {
                        dataField = dataBag.getFieldByName(cellName);
                        if (dataField == null)
                        {
                            String msgStr = String.format("%s: Does not match any known fields.", cellName);
                            throw new IOException(msgStr);
                        }
                        else
                        {
                            if (dataField.isMultiValue())
                            {
                                mvDelimiter = dataField.getFeature(Field.FEATURE_MV_DELIMITER);
                                if (StringUtils.isNotEmpty(mvDelimiter))
                                    dataField.setValues(StrUtl.expandToList(cellValue, mvDelimiter.charAt(0)));
                                else
                                    dataField.setValues(StrUtl.expandToList(cellValue, StrUtl.CHAR_PIPE));
                            }
                            else
                            {
                                if (dataField.isTypeDateOrTime())
                                {
                                    cellDate = Field.createDate(cellValue, mCellDateFormat);
                                    dataField.setValue(cellDate);
                                }
                                else
                                    dataField.setValue(cellValue);
                            }
                        }
                    }
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dataBag;
    }

    /**
     * Closes the reader stream and resets the state.
     */
    public void close()
    {
        Logger appLogger = mAppMgr.getLogger(this, "close");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mCSVListReader != null)
        {
            try
            {
                mCSVListReader.close();
            }
            catch (IOException e)
            {
                String msgStr = String.format("%s: %s", mPathFileName, e.getMessage());
                appLogger.error(msgStr, e);
            }
            mCSVListReader = null;
            mColumnHeaders = null;
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
