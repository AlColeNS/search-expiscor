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

package com.nridge.core.base.io.console;

import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.io.*;

/**
 * The DataBagConsole class provides a convenient way for an application
 * to capture field input from the console.  The presentation of the fields
 * are aligned based on title name widths.  Special handling logic exists to
 * handle password presentations.
 * <p>
 * <b>Note:</b> Only values that are visible will be processed by this class.
 * </p>
 *
 * @since 1.0
 * @author Al Cole
 */
public class DataBagConsole
{
    private DataBag mBag;
    private boolean mIsBasedOnTitle = true;

    /**
     * Constructor that identifies a bag prior to an edit operation.
     *
     * @param aBag Data bag of fields.
     */
    public DataBagConsole(DataBag aBag)
    {
        mBag = aBag;
    }

    /**
     * By default, the logic will use the title as a prompt string.
     * You can change this default behaviour to use the field name.
     *
     * @param aTitleFlag If false, then the field name will be
     * used as a prompt string.
     */
    public void setUseTitleFlag(boolean aTitleFlag)
    {
        mIsBasedOnTitle = aTitleFlag;
    }

    private void editBag(String aTitle)
    {
        StringBuilder stringBuilder;
        String fieldTitle, fieldValue, promptString, inputString;

        int maxPromptLength = 0;
        for (DataField dataField : mBag.getFields())
        {
            if (dataField.isDisplayable())
            {
                if (mIsBasedOnTitle)
                    fieldTitle = dataField.getTitle();
                else
                    fieldTitle = dataField.getName();
                fieldValue = dataField.getValue();
                if (StringUtils.isEmpty(fieldValue))
                    promptString = fieldTitle;
                else
                    promptString = String.format("%s [%s]", fieldTitle, fieldValue);
                maxPromptLength = Math.max(maxPromptLength, promptString.length());
            }
        }

        if (StringUtils.isNotEmpty(aTitle))
        {
            stringBuilder = new StringBuilder();
            for (int j = aTitle.length(); j < maxPromptLength; j++)
                stringBuilder.append(StrUtl.CHAR_SPACE);

            System.out.printf("%n%s%s%n%n", stringBuilder.toString(), aTitle);
        }

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

        for (DataField dataField : mBag.getFields())
        {
            if (dataField.isDisplayable())
            {
                if (mIsBasedOnTitle)
                    fieldTitle = dataField.getTitle();
                else
                    fieldTitle = dataField.getName();
                fieldValue = dataField.getValue();
                if (StringUtils.isEmpty(fieldValue))
                    promptString = fieldTitle;
                else
                    promptString = String.format("%s [%s]", fieldTitle, fieldValue);

                stringBuilder = new StringBuilder();
                for (int j = promptString.length(); j < maxPromptLength; j++)
                    stringBuilder.append(StrUtl.CHAR_SPACE);

                System.out.printf("%s%s: ", stringBuilder.toString(), promptString);

                try
                {
                    inputString = stdin.readLine();
                }
                catch (IOException e)
                {
                    inputString = StringUtils.EMPTY;
                }

                if (StringUtils.isNotEmpty(inputString))
                    dataField.setValue(inputString);
            }
        }
    }

    /**
     * Prompts the user from the console for the bag values.
     * <p>
     * <b>Note:</b> This method can only support single value
     * fields.
     * </p>
     *
     * @param aTitle A title string for the presentation.
     */
    public void edit(String aTitle)
    {
        editBag(aTitle);
    }

    /**
     * Prompts the user from the console for the bag values.
     * <p>
     * <b>Note:</b> This method can only support single value
     * fields.
     * </p>
     */
    public void edit()
    {
        edit(StringUtils.EMPTY);
    }

    /**
     * Write bag name and values to the console.
     * <p>
     * <b>Note:</b> This method can only support single value
     * fields.
     * </p>
     *
     * @param aPW PrintWriter instance.
     *
     * @param aTitle A title string for the presentation.
     */
    public void writeBag(PrintWriter aPW, String aTitle)
    {
        StringBuilder stringBuilder;
        String fieldTitle, fieldValue, titleString;

        int maxTitleLength = 0;
        for (DataField dataField : mBag.getFields())
        {
            if (dataField.isDisplayable())
            {
                if (mIsBasedOnTitle)
                    fieldTitle = dataField.getTitle();
                else
                    fieldTitle = dataField.getName();
                maxTitleLength = Math.max(maxTitleLength, fieldTitle.length());
            }
        }

        if (StringUtils.isNotEmpty(aTitle))
        {
            stringBuilder = new StringBuilder();
            for (int j = aTitle.length(); j < maxTitleLength; j++)
                stringBuilder.append(StrUtl.CHAR_SPACE);

            aPW.printf("%n%s%s%n%n", stringBuilder.toString(), aTitle);
        }

        for (DataField dataField : mBag.getFields())
        {
            if (dataField.isDisplayable())
            {
                if (mIsBasedOnTitle)
                    fieldTitle = dataField.getTitle();
                else
                    fieldTitle = dataField.getName();
                if (dataField.isMultiValue())
                    fieldValue = StrUtl.collapseToSingle(dataField.getValues(), StrUtl.CHAR_COMMA);
                else
                    fieldValue = dataField.getValue();
                stringBuilder = new StringBuilder();
                for (int j = fieldTitle.length(); j < maxTitleLength; j++)
                    stringBuilder.append(StrUtl.CHAR_SPACE);

                aPW.printf("%s%s: %s%n", stringBuilder.toString(), fieldTitle, fieldValue);

            }
        }
    }

    /**
     * Write bag name and values to the console.
     * <p>
     * <b>Note:</b> This method can only support single value
     * fields.
     * </p>
     *
     * @param aTitle A title string for the presentation.
     */
    public void write(String aTitle)
    {
        PrintWriter printWriter = new PrintWriter(System.out);
        writeBag(printWriter, aTitle);
    }

    /**
     * Write bag name and values to the console.
     * <p>
     * <b>Note:</b> This method can only support single value
     * fields.
     * </p>
     */
    public void write()
    {
        write(StringUtils.EMPTY);
    }
}
