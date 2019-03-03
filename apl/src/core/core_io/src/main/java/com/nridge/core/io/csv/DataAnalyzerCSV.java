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

package com.nridge.core.io.csv;

import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.*;
import com.nridge.core.base.io.console.DataTableConsole;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * This class provides a convienence methods for analyzing small to medium
 * CSV files and generating a detail summary report.  It focuses on streaming
 * the CSV file to minimize memory pressure.
 */
public class DataAnalyzerCSV
{
	public DataAnalyzerCSV()
	{
	}

	/**
	 * This method will stream, analyze the CSV row data and return a
	 * summary detail report table.  This method assumes that
	 * the CSV file has a header identifying the name of columns.
	 *
	 * @param aPathFileName CSV file name to stream in (with a header).
	 *
	 * @throws IOException Thrown if an I/O issue is detected.
	 */
	public DataTable streamAnalyzeData(String aPathFileName)
		throws IOException
	{
		int adjCount;
		DataField dataField;
		String cellValue, mvDelimiter;

		try (FileReader fileReader = new FileReader(aPathFileName))
		{
			CsvListReader csvListReader = new CsvListReader(fileReader, CsvPreference.EXCEL_PREFERENCE);
			String[] columnHeaders = csvListReader.getHeader(true);
			if (columnHeaders == null)
				throw new IOException(aPathFileName + ": Does not have a header row.");

			DataBag columnBag = new DataBag("Data Analysis Table");
			for (String columnName : columnHeaders)
			{
				if (StringUtils.isAllLowerCase(columnName))
					dataField = new DataTextField(columnName, Field.nameToTitle(columnName));
				else
					dataField = new DataTextField(Field.titleToName(columnName), columnName);
				columnBag.add(dataField);
			}
			int columnCount = columnBag.count();
			DataAnalyzer dataAnalyzer = new DataAnalyzer(columnBag);

			List<String> rowCells = csvListReader.read();
			while (rowCells != null)
			{
				DataBag dataBag = new DataBag(columnBag);
				adjCount = Math.min(rowCells.size(), columnCount);
				for (int col = 0; col < adjCount; col++)
				{
					cellValue = rowCells.get(col);
					if (StringUtils.isNotEmpty(cellValue))
					{
						dataField = dataBag.getByOffset(col);
						if (dataField.isMultiValue())
						{
							mvDelimiter = dataField.getFeature(Field.FEATURE_MV_DELIMITER);
							if (StringUtils.isNotEmpty(mvDelimiter))
								dataField.setValues(StrUtl.expandToList(cellValue, mvDelimiter.charAt(0)));
							else
								dataField.setValues(StrUtl.expandToList(cellValue, StrUtl.CHAR_PIPE));
						}
						else
							dataBag.setValueByName(dataField.getName(), cellValue);
					}
				}
				dataAnalyzer.scan(dataBag);
				rowCells = csvListReader.read();
			}

			return dataAnalyzer.getDetails();
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}
	}

	/**
	 * This method will stream, analyze the CSV row data and generate a
	 * summary report to the output console.  This method assumes that
	 * the CSV file has a header identifying the name of columns.
	 *
	 * @param aPathFileName CSV file name to stream in (with a header).
	 * @param aTitle Title for the output writing of the detail summmary.
	 *
	 * @throws IOException Thrown if an I/O issue is detected.
	 */
	public void streamAnalyzeDataConsole(String aPathFileName, String aTitle)
		throws IOException
	{
		DataTable analyzerDetailTable = streamAnalyzeData(aPathFileName);

		PrintWriter printWriter = new PrintWriter(System.out, true);
		DataTableConsole dataTableConsole = new DataTableConsole(analyzerDetailTable);
		dataTableConsole.write(printWriter, aTitle);
	}
}
